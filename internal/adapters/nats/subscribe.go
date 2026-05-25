package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

func (t *Transport) Subscribe(ctx context.Context, subject, durable string, handler ports.MsgHandler, opts ...ports.SubscribeOption) (ports.Unsubscribe, error) {
	if subject == "" {
		return nil, errors.New("nats: subject required")
	}
	if durable == "" {
		return nil, errors.New("nats: durable name required")
	}
	if handler == nil {
		return nil, errors.New("nats: handler required")
	}

	cfg := ports.SubscribeOpts{}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.MaxConcurrent < 1 {
		cfg.MaxConcurrent = 1
	}

	streamName, err := t.js.StreamNameBySubject(ctx, subject)
	if err != nil {
		return nil, fmt.Errorf("nats: stream lookup for %s: %w", subject, err)
	}
	stream, err := t.js.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("nats: open stream %s: %w", streamName, err)
	}

	consumerCfg := jetstream.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		MaxDeliver:    -1,
		AckWait:       30 * time.Second,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}
	if cfg.MaxConcurrent > 1 {
		consumerCfg.MaxAckPending = cfg.MaxConcurrent * 4
	}
	consumer, err := stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return nil, fmt.Errorf("nats: create consumer %s: %w", durable, err)
	}

	if cfg.MaxConcurrent == 1 {
		return t.consumeSerial(consumer, durable, handler)
	}
	return t.consumeSharded(consumer, durable, handler, cfg)
}

func (t *Transport) consumeSerial(consumer jetstream.Consumer, durable string, handler ports.MsgHandler) (ports.Unsubscribe, error) {
	cc, err := consumer.Consume(func(m jetstream.Msg) {
		t.invokeHandler(m, durable, handler)
	})
	if err != nil {
		return nil, fmt.Errorf("nats: start consume %s: %w", durable, err)
	}
	t.trackConsume(cc)
	return func() error {
		cc.Stop()
		return nil
	}, nil
}

func (t *Transport) consumeSharded(consumer jetstream.Consumer, durable string, handler ports.MsgHandler, cfg ports.SubscribeOpts) (ports.Unsubscribe, error) {
	shards := make([]chan jetstream.Msg, cfg.MaxConcurrent)
	var wg sync.WaitGroup
	for i := range shards {
		shards[i] = make(chan jetstream.Msg, 16)
		wg.Add(1)
		go func(in <-chan jetstream.Msg) {
			defer wg.Done()
			for m := range in {
				t.invokeHandler(m, durable, handler)
			}
		}(shards[i])
	}

	cc, err := consumer.Consume(func(m jetstream.Msg) {
		idx := shardIndex(m, cfg.ShardKey, len(shards))
		shards[idx] <- m
	})
	if err != nil {
		for _, ch := range shards {
			close(ch)
		}
		wg.Wait()
		return nil, fmt.Errorf("nats: start consume %s: %w", durable, err)
	}
	t.trackConsume(cc)
	return func() error {
		cc.Stop()
		for _, ch := range shards {
			close(ch)
		}
		wg.Wait()
		return nil
	}, nil
}

func (t *Transport) invokeHandler(m jetstream.Msg, durable string, handler ports.MsgHandler) {
	msg := toPortMsg(m)
	hctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := handler(hctx, msg); err != nil {
		t.log.Warn("subscribe handler error",
			"subject", m.Subject(),
			"durable", durable,
			"err", err,
		)
		if nerr := m.Nak(); nerr != nil {
			t.log.Error("nak failed", "err", nerr)
		}
		return
	}
	if aerr := m.Ack(); aerr != nil {
		t.log.Error("ack failed", "err", aerr)
	}
}

func shardIndex(m jetstream.Msg, keyFn func(ports.Msg) uint64, n int) int {
	if n <= 1 {
		return 0
	}
	pm := toPortMsg(m)
	var key uint64
	if keyFn != nil {
		key = keyFn(pm)
	} else {
		key = pm.Seq
	}
	return int(key % uint64(n)) //nolint:gosec // n is the worker-pool size from config
}

func toPortMsg(m jetstream.Msg) ports.Msg {
	headers := map[string]string{}
	for k, vv := range m.Headers() {
		if len(vv) > 0 {
			headers[k] = vv[0]
		}
	}
	var seq uint64
	if meta, err := m.Metadata(); err == nil {
		seq = meta.Sequence.Stream
	}
	return ports.Msg{
		Subject: m.Subject(),
		Headers: headers,
		Data:    m.Data(),
		Seq:     seq,
		Ack:     m.Ack,
		Nak:     m.Nak,
	}
}

package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

func (t *Transport) Subscribe(ctx context.Context, subject, durable string, handler ports.MsgHandler) (ports.Unsubscribe, error) {
	if subject == "" {
		return nil, errors.New("nats: subject required")
	}
	if durable == "" {
		return nil, errors.New("nats: durable name required")
	}
	if handler == nil {
		return nil, errors.New("nats: handler required")
	}

	streamName, err := t.js.StreamNameBySubject(ctx, subject)
	if err != nil {
		return nil, fmt.Errorf("nats: stream lookup for %s: %w", subject, err)
	}
	stream, err := t.js.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("nats: open stream %s: %w", streamName, err)
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		MaxDeliver:    -1,
		AckWait:       30 * time.Second,
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("nats: create consumer %s: %w", durable, err)
	}

	cc, err := consumer.Consume(func(m jetstream.Msg) {
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

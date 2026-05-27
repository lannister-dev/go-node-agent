package nats

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

func (t *Transport) KVPut(ctx context.Context, bucket, key string, value []byte) error {
	kv, err := t.kvBucket(ctx, bucket)
	if err != nil {
		return err
	}
	if _, err := kv.Put(ctx, key, value); err != nil {
		return fmt.Errorf("nats kv put %s/%s: %w", bucket, key, err)
	}
	return nil
}

func (t *Transport) KVGet(ctx context.Context, bucket, key string) ([]byte, error) {
	kv, err := t.kvBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	entry, err := kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("nats kv get %s/%s: %w", bucket, key, err)
	}
	return entry.Value(), nil
}

type KVUpdate struct {
	Key   string
	Value []byte
}

func (t *Transport) KVWatchKey(ctx context.Context, bucket, key string, onUpdate func(KVUpdate) error) error {
	kv, err := t.kvBucket(ctx, bucket)
	if err != nil {
		return err
	}
	watcher, err := kv.Watch(ctx, key)
	if err != nil {
		return fmt.Errorf("nats kv watch %s/%s: %w", bucket, key, err)
	}
	defer watcher.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e, ok := <-watcher.Updates():
			if !ok {
				return nil
			}
			if e == nil {
				continue
			}
			if e.Operation() != jetstream.KeyValuePut {
				continue
			}
			if err := onUpdate(KVUpdate{Key: e.Key(), Value: e.Value()}); err != nil {
				t.log.Warn("kv watch handler error", "bucket", bucket, "key", e.Key(), "err", err)
			}
		}
	}
}

func (t *Transport) kvBucket(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	kv, err := t.js.KeyValue(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("nats kv bucket %s: %w", bucket, err)
	}
	return kv, nil
}

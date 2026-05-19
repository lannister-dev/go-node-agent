package ports

import "context"

type Msg struct {
	Subject string
	Headers map[string]string
	Data    []byte
	Seq     uint64
	Ack     func() error
	Nak     func() error
}

type MsgHandler func(ctx context.Context, msg Msg) error

type Publisher interface {
	Publish(ctx context.Context, subject string, headers map[string]string, data []byte) error
}

type Subscriber interface {
	Subscribe(ctx context.Context, subject, durable string, handler MsgHandler) (Unsubscribe, error)
}

type Unsubscribe func() error

type Transport interface {
	Publisher
	Subscriber
	Close() error
}

type KV interface {
	Get(ctx context.Context, bucket, key string) ([]byte, uint64, error)
	Watch(ctx context.Context, bucket, keyPattern string, handler func(key string, value []byte, revision uint64)) (Unsubscribe, error)
}

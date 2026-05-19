package nats

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
)

func (t *Transport) Publish(ctx context.Context, subject string, headers map[string]string, data []byte) error {
	if subject == "" {
		return errors.New("nats: subject required")
	}
	msg := &nats.Msg{Subject: subject, Data: data}
	if len(headers) > 0 {
		msg.Header = nats.Header{}
		for k, v := range headers {
			msg.Header.Set(k, v)
		}
	}

	pubCtx := ctx
	if t.publishTimeout > 0 {
		var cancel context.CancelFunc
		pubCtx, cancel = context.WithTimeout(ctx, t.publishTimeout)
		defer cancel()
	}

	if _, err := t.js.PublishMsg(pubCtx, msg); err != nil {
		return fmt.Errorf("nats: publish %s: %w", subject, err)
	}
	return nil
}

package nats

import (
	"context"
	"errors"
)

func (t *Transport) Name() string { return "nats" }

func (t *Transport) Check(_ context.Context) error {
	if t.nc == nil {
		return errors.New("not initialized")
	}
	if !t.nc.IsConnected() {
		return errors.New("not connected")
	}
	return nil
}

package domain

import "errors"

var (
	ErrStaleOp          = errors.New("op_version is stale")
	ErrUnknownPlacement = errors.New("placement unknown")
	ErrInvalidTransport = errors.New("invalid transport kind")
	ErrDrainTimeout     = errors.New("drain timeout exceeded")
	ErrBackendUnhealthy = errors.New("backend not healthy")
)

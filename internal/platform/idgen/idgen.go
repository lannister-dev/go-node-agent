package idgen

import "github.com/google/uuid"

type Generator interface {
	NewID() string
}

type UUID struct{}

func (UUID) NewID() string { return uuid.NewString() }

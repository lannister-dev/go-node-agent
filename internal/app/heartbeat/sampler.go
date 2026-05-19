package heartbeat

import "context"

type NoopSampler struct{}

func (NoopSampler) Sample(context.Context) (Stats, error) { return Stats{}, nil }

type NoopCounters struct{}

func (NoopCounters) Snapshot() (uint32, uint32, uint32) { return 0, 0, 0 }

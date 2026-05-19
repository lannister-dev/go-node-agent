package heartbeat

import (
	"context"

	"github.com/lannister-dev/go-node-agent/internal/platform/idgen"
)

type Publisher interface {
	Publish(ctx context.Context, subject string, headers map[string]string, data []byte) error
}

type Stats struct {
	CPUPct       *float64
	MemPct       *float64
	BandwidthPct *float64
}

type Sampler interface {
	Sample(ctx context.Context) (Stats, error)
}

type Counters interface {
	Snapshot() (pollCount, applied, failed uint32)
}

type IDGenerator = idgen.Generator

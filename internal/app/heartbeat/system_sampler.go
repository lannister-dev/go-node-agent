package heartbeat

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
	psnet "github.com/shirou/gopsutil/v4/net"
)

type SystemSamplerOptions struct {
	NIC                 string
	CapacityBytesPerSec uint64
}

type SystemSampler struct {
	opts SystemSamplerOptions

	mu        sync.Mutex
	prevBytes uint64
	prevTaken time.Time
}

func NewSystemSampler() *SystemSampler { return NewSystemSamplerWith(SystemSamplerOptions{}) }

func NewSystemSamplerWith(opts SystemSamplerOptions) *SystemSampler {
	_, _ = cpu.Percent(0, false)
	return &SystemSampler{opts: opts}
}

func (s *SystemSampler) Sample(ctx context.Context) (Stats, error) {
	var st Stats
	if pcts, err := cpu.PercentWithContext(ctx, 0, false); err == nil && len(pcts) > 0 {
		v := pcts[0]
		st.CPUPct = &v
	}
	if mv, err := mem.VirtualMemoryWithContext(ctx); err == nil {
		v := mv.UsedPercent
		st.MemPct = &v
	}
	if s.opts.CapacityBytesPerSec > 0 {
		if pct := s.sampleBandwidth(ctx); pct != nil {
			st.BandwidthPct = pct
		}
	}
	return st, nil
}

func (s *SystemSampler) sampleBandwidth(ctx context.Context) *float64 {
	counters, err := psnet.IOCountersWithContext(ctx, true)
	if err != nil || len(counters) == 0 {
		return nil
	}
	var total uint64
	for _, c := range counters {
		if s.opts.NIC != "" {
			if !strings.EqualFold(c.Name, s.opts.NIC) {
				continue
			}
		} else if c.Name == "lo" || strings.HasPrefix(c.Name, "lo") {
			continue
		}
		total += c.BytesSent + c.BytesRecv
	}
	if total == 0 {
		return nil
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() {
		s.prevBytes = total
		s.prevTaken = now
	}()
	if s.prevTaken.IsZero() {
		return nil
	}
	elapsed := now.Sub(s.prevTaken).Seconds()
	if elapsed <= 0 || total < s.prevBytes {
		return nil
	}
	rate := float64(total-s.prevBytes) / elapsed
	pct := rate / float64(s.opts.CapacityBytesPerSec) * 100.0
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	return &pct
}

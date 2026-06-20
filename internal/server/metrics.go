package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type StatsSource interface {
	Snapshot() (received, applied, failed uint32)
}

type TrafficSource interface {
	UpBytes() uint64
	DownBytes() uint64
}

type EntrySource interface {
	Users() int
}

func registerApplierMetrics(reg *prometheus.Registry, src StatsSource) {
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	if src == nil {
		return
	}
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "agent_placement_received_total",
		Help: "Total placement commands received from NATS.",
	}, func() float64 {
		r, _, _ := src.Snapshot()
		return float64(r)
	}))
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "agent_placement_applied_total",
		Help: "Total placement commands applied successfully.",
	}, func() float64 {
		_, a, _ := src.Snapshot()
		return float64(a)
	}))
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "agent_placement_failed_total",
		Help: "Total placement commands that failed apply (non-retryable).",
	}, func() float64 {
		_, _, f := src.Snapshot()
		return float64(f)
	}))
}

func registerEntryMetrics(reg *prometheus.Registry, src EntrySource) {
	if src == nil {
		return
	}
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "agent_entry_users",
		Help: "Users currently loaded into the embedded entry proxy from the store.",
	}, func() float64 { return float64(src.Users()) }))
}

func registerTrafficMetrics(reg *prometheus.Registry, src TrafficSource) {
	if src == nil {
		return
	}
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "agent_singbox_traffic_up_bytes_total",
		Help: "Cumulative upstream bytes reported by sing-box /traffic stream.",
	}, func() float64 { return float64(src.UpBytes()) }))
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "agent_singbox_traffic_down_bytes_total",
		Help: "Cumulative downstream bytes reported by sing-box /traffic stream.",
	}, func() float64 { return float64(src.DownBytes()) }))
}

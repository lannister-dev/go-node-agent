//go:build with_utls

package entryproxy_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	singtls "github.com/sagernet/sing-box/common/tls"
	singlog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-vmess/vless"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

// TestLoadBurstHandshakes simulates the thundering-herd reconnect: N phones wake
// at once and each opens a fresh REALITY connection (full handshake + VLESS dial
// + a roundtrip). It reports handshake latency under concurrency, CPU spent per
// connection, peak goroutines, and confirms goroutines drain back to baseline
// (no leak under churn). Opt-in: LOAD_TEST=1.
//
// Caveat: client, server and backend all run in this one process, so the CPU/op
// figure is the *combined* crypto cost; the entry-proxy server alone is a
// fraction of it (and in prod additionally pays one dial to the handshake
// server). Latency, throughput and the leak check are directly representative.
func TestLoadBurstHandshakes(t *testing.T) {
	if os.Getenv("LOAD_TEST") == "" {
		t.Skip("set LOAD_TEST=1 to run the load test")
	}

	bursts := []int{50, 100, 250, 500}
	if v := os.Getenv("LOAD_BURSTS"); v != "" {
		bursts = bursts[:0]
		for _, s := range strings.Split(v, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(s))
			if err != nil {
				t.Fatalf("bad LOAD_BURSTS %q: %v", v, err)
			}
			bursts = append(bursts, n)
		}
	}
	maxN := 0
	for _, n := range bursts {
		if n > maxN {
			maxN = n
		}
	}

	// Distinct user per connection in the largest burst — matches prod where each
	// phone is its own VLESS identity.
	users := make([]string, maxN)
	for i := range users {
		users[i] = fmt.Sprintf("%08x-0000-0000-0000-%012x", i, i)
	}
	visionFlow := "xtls-rprx-vision"

	backend := taggedBackend(t, "LOAD", users...)
	proxy, proxyAddr, pub := startProxy(t)
	if err := proxy.SetBackends(context.Background(), []ports.EntryBackend{entryBackend(t, "load-be", backend)}); err != nil {
		t.Fatal(err)
	}
	for _, u := range users {
		if err := proxy.AddUser(context.Background(), u, visionFlow); err != nil {
			t.Fatal(err)
		}
		if err := proxy.SelectBackend(context.Background(), u, "load-be"); err != nil {
			t.Fatal(err)
		}
	}

	// One reusable REALITY client config (concurrency-safe across handshakes).
	cfg, err := singtls.NewRealityClient(context.Background(), singlog.NewNOPFactory().Logger(), smokeServerName, option.OutboundTLSOptions{
		Enabled:    true,
		ServerName: smokeServerName,
		UTLS:       &option.OutboundUTLSOptions{Enabled: true, Fingerprint: "chrome"},
		Reality:    &option.OutboundRealityOptions{Enabled: true, PublicKey: pub, ShortID: smokeShortID},
	})
	if err != nil {
		t.Fatalf("reality client cfg: %v", err)
	}

	op := func(clientID string) (time.Duration, error) {
		start := time.Now()
		tcp, derr := net.Dial("tcp", proxyAddr)
		if derr != nil {
			return 0, fmt.Errorf("dial: %w", derr)
		}
		defer func() { _ = tcp.Close() }()
		_ = tcp.SetDeadline(time.Now().Add(15 * time.Second))
		tlsConn, herr := singtls.ClientHandshake(context.Background(), tcp, cfg)
		if herr != nil {
			return 0, fmt.Errorf("handshake: %w", herr)
		}
		vc, verr := vless.NewClient(clientID, visionFlow, logger.NOP())
		if verr != nil {
			return 0, fmt.Errorf("vless client: %w", verr)
		}
		conn, cerr := vc.DialEarlyConn(tlsConn, M.ParseSocksaddr("1.1.1.1:80"))
		if cerr != nil {
			return 0, fmt.Errorf("vless dial: %w", cerr)
		}
		if _, werr := conn.Write([]byte("ping")); werr != nil {
			return 0, fmt.Errorf("write: %w", werr)
		}
		buf := make([]byte, 4)
		n, _ := io.ReadFull(conn, buf[:4])
		if n < 4 || string(buf[:n]) != "LOAD" {
			return 0, fmt.Errorf("bad tag %q (n=%d)", buf[:n], n)
		}
		return time.Since(start), nil
	}

	baseG := runtime.NumGoroutine()
	t.Logf("baseline goroutines=%d  cpus=%d", baseG, runtime.NumCPU())

	for _, n := range bursts {
		runLoadBurst(t, n, users, op, baseG)
	}
}

func runLoadBurst(t *testing.T, n int, users []string, op func(string) (time.Duration, error), baseG int) {
	t.Helper()
	lat := make([]time.Duration, n)
	var ok, fail atomic.Int64
	var firstErr atomic.Value
	var peakG atomic.Int64

	stopWatch := make(chan struct{})
	var watchDone sync.WaitGroup
	watchDone.Add(1)
	go func() {
		defer watchDone.Done()
		tk := time.NewTicker(time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopWatch:
				return
			case <-tk.C:
				if g := int64(runtime.NumGoroutine()); g > peakG.Load() {
					peakG.Store(g)
				}
			}
		}
	}()

	var ru0, ru1 syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru0)
	wall0 := time.Now()

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			d, err := op(users[i%len(users)])
			if err != nil {
				fail.Add(1)
				firstErr.CompareAndSwap(nil, err.Error())
				return
			}
			ok.Add(1)
			lat[i] = d
		}(i)
	}
	wg.Wait()
	wall := time.Since(wall0)
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru1)
	close(stopWatch)
	watchDone.Wait()

	cpu := cpuSeconds(ru1) - cpuSeconds(ru0)

	done := make([]time.Duration, 0, n)
	for _, d := range lat {
		if d > 0 {
			done = append(done, d)
		}
	}
	sort.Slice(done, func(a, b int) bool { return done[a] < done[b] })

	// Let churn settle, then confirm goroutines drained (no leak under load).
	time.Sleep(300 * time.Millisecond)
	runtime.GC()
	settledG := runtime.NumGoroutine()

	pct := func(p float64) time.Duration {
		if len(done) == 0 {
			return 0
		}
		idx := int(p * float64(len(done)-1))
		return done[idx]
	}

	cpuPerOp := 0.0
	if ok.Load() > 0 {
		cpuPerOp = cpu / float64(ok.Load())
	}
	throughput := float64(ok.Load()) / wall.Seconds()
	errMsg := ""
	if v := firstErr.Load(); v != nil {
		errMsg = "  firstErr=" + v.(string)
	}

	t.Logf("burst=%-4d ok=%-4d fail=%-3d wall=%-8s thr=%-7.0f/s  p50=%-8s p95=%-8s p99=%-8s max=%-8s  cpu=%.2fs (%.2fms/op)  peakG=%d settledG=%d (base %d)%s",
		n, ok.Load(), fail.Load(), wall.Round(time.Millisecond), throughput,
		pct(0.50).Round(time.Microsecond*100), pct(0.95).Round(time.Microsecond*100),
		pct(0.99).Round(time.Microsecond*100), pct(1.0).Round(time.Microsecond*100),
		cpu, cpuPerOp*1000, peakG.Load(), settledG, baseG, errMsg)

	if fail.Load() > 0 {
		t.Errorf("burst=%d had %d failures", n, fail.Load())
	}
	// Goroutines must drain back near baseline — proves no leak under churn.
	if settledG > baseG+n/10+20 {
		t.Errorf("burst=%d leaked goroutines: settled=%d base=%d", n, settledG, baseG)
	}
}

func cpuSeconds(ru syscall.Rusage) float64 {
	return float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6 +
		float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
}

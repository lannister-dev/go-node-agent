package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Options struct {
	Addr            string
	Stats           StatsSource
	Traffic         TrafficSource
	Entry           EntrySource
	Checks          []HealthCheck
	Logger          *slog.Logger
	ShutdownTimeout time.Duration
	EnablePprof     bool
}

type Server struct {
	listener        net.Listener
	checks          []HealthCheck
	log             *slog.Logger
	reg             *prometheus.Registry
	httpSrv         *http.Server
	shutdownTimeout time.Duration
}

func New(opts Options) (*Server, error) {
	if opts.Addr == "" {
		return nil, errors.New("server: Addr required")
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	log = log.With("component", "admin-http")

	shutdownTimeout := opts.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = 5 * time.Second
	}

	reg := prometheus.NewRegistry()
	registerApplierMetrics(reg, opts.Stats)
	registerTrafficMetrics(reg, opts.Traffic)
	registerEntryMetrics(reg, opts.Entry)

	ln, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("admin-http: listen %s: %w", opts.Addr, err)
	}

	s := &Server{
		listener:        ln,
		checks:          opts.Checks,
		log:             log,
		reg:             reg,
		shutdownTimeout: shutdownTimeout,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /livez", s.handleLivez)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	if opts.EnablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	s.httpSrv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return s, nil
}

func (s *Server) Run(ctx context.Context) error {
	s.log.Info("admin server listening", "addr", s.listener.Addr().String())

	errCh := make(chan error, 1)
	go func() {
		if err := s.httpSrv.Serve(s.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
		defer cancel()
		if err := s.httpSrv.Shutdown(shutCtx); err != nil {
			s.log.Warn("admin-http shutdown error", "err", err)
		}
		<-errCh
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

func (s *Server) Close() error {
	if s.listener != nil {
		_ = s.listener.Close()
	}
	return nil
}

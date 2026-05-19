package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type HealthCheck interface {
	Name() string
	Check(ctx context.Context) error
}

type healthResponse struct {
	Status string           `json:"status"`
	Checks []healthCheckOut `json:"checks,omitempty"`
}

type healthCheckOut struct {
	Name  string `json:"name"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

func (s *Server) handleLivez(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "alive"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	resp := healthResponse{Status: "ok", Checks: make([]healthCheckOut, 0, len(s.checks))}
	allOK := true
	for _, c := range s.checks {
		err := c.Check(ctx)
		ok := err == nil
		if !ok {
			allOK = false
		}
		entry := healthCheckOut{Name: c.Name(), OK: ok}
		if !ok {
			entry.Error = err.Error()
		}
		resp.Checks = append(resp.Checks, entry)
	}
	status := http.StatusOK
	if !allOK {
		status = http.StatusServiceUnavailable
		resp.Status = "not_ready"
	}
	writeJSON(w, status, resp)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

package entryproxy

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"

	"github.com/lannister-dev/go-node-agent/internal/entryproxy/api"
	"github.com/lannister-dev/go-node-agent/internal/ports"
)

// ControlServer exposes a ports.EntryProxy over a local unix-socket HTTP API so
// the agent can drive user/route/backend changes at runtime.
// Controller is the proxy surface the control API exposes: the runtime control
// port plus the restart epoch.
type Controller interface {
	ports.EntryProxy
	Epoch() int64
}

type ControlServer struct {
	proxy Controller
	log   *slog.Logger
}

func NewControlServer(proxy Controller, log *slog.Logger) *ControlServer {
	if log == nil {
		log = slog.Default()
	}
	return &ControlServer{proxy: proxy, log: log.With("component", "entryproxy-control")}
}

func (s *ControlServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+api.PathAddUser, func(w http.ResponseWriter, r *http.Request) {
		var req api.AddUserRequest
		if !decode(w, r, &req) {
			return
		}
		respond(w, s.proxy.AddUser(r.Context(), req.ClientID, req.Flow), nil)
	})
	mux.HandleFunc("POST "+api.PathRemoveUser, func(w http.ResponseWriter, r *http.Request) {
		var req api.RemoveUserRequest
		if !decode(w, r, &req) {
			return
		}
		respond(w, s.proxy.RemoveUser(r.Context(), req.ClientID), nil)
	})
	mux.HandleFunc("POST "+api.PathSelectBackend, func(w http.ResponseWriter, r *http.Request) {
		var req api.SelectBackendRequest
		if !decode(w, r, &req) {
			return
		}
		respond(w, s.proxy.SelectBackend(r.Context(), req.ClientID, req.BackendID), nil)
	})
	mux.HandleFunc("POST "+api.PathSetBackends, func(w http.ResponseWriter, r *http.Request) {
		var req api.SetBackendsRequest
		if !decode(w, r, &req) {
			return
		}
		respond(w, s.proxy.SetBackends(r.Context(), req.Backends), nil)
	})
	mux.HandleFunc("POST "+api.PathBackendConnections, func(w http.ResponseWriter, r *http.Request) {
		var req api.BackendConnectionsRequest
		if !decode(w, r, &req) {
			return
		}
		count, err := s.proxy.BackendConnections(r.Context(), req.BackendID)
		respond(w, err, api.BackendConnectionsResponse{Count: count})
	})
	mux.HandleFunc("POST "+api.PathActiveConnections, func(w http.ResponseWriter, r *http.Request) {
		conns, err := s.proxy.ActiveConnections(r.Context())
		respond(w, err, api.ActiveConnectionsResponse{Conns: conns})
	})
	mux.HandleFunc("POST "+api.PathStatus, func(w http.ResponseWriter, _ *http.Request) {
		respond(w, nil, api.StatusResponse{Epoch: s.proxy.Epoch()})
	})
	return mux
}

// Serve listens on the unix socket until ctx is cancelled.
func (s *ControlServer) Serve(ctx context.Context, socketPath string) error {
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: s.handler()}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	s.log.Info("control api listening", "socket", socketPath)
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return false
	}
	return true
}

func respond(w http.ResponseWriter, err error, body any) {
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	if body == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(api.ErrorResponse{Error: err.Error()})
}

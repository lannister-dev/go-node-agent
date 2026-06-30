package wgmesh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lannister-dev/go-node-agent/internal/adapters/nats"
	"github.com/lannister-dev/go-node-agent/internal/adapters/wg"
	"github.com/lannister-dev/go-node-agent/internal/domain"
)

const (
	BucketPubkeys = "agent-wg-pubkeys"
	BucketPeers   = "wg-mesh-peers"
	KeyPrefix     = "node."
)

type Config struct {
	NodeID     domain.NodeID
	ListenPort int
}

type pubkeyPayload struct {
	NodeID     string `json:"node_id"`
	PublicKey  string `json:"public_key"`
	ListenPort int    `json:"listen_port"`
}

type peerPayload struct {
	NodeID     string         `json:"node_id"`
	Address    string         `json:"address"`
	ListenPort int            `json:"listen_port"`
	Peers      []peerEntryDTO `json:"peers"`
}

type peerEntryDTO struct {
	NodeID     string `json:"node_id"`
	Name       string `json:"name"`
	PublicKey  string `json:"public_key"`
	Endpoint   string `json:"endpoint"`
	ListenPort int    `json:"listen_port"`
	Address    string `json:"address"`
}

type kvClient interface {
	KVPut(ctx context.Context, bucket, key string, value []byte) error
	KVWatchKey(ctx context.Context, bucket, key string, onUpdate func(nats.KVUpdate) error) error
}

type Service struct {
	cfg  Config
	mgr  *wg.Manager
	nats kvClient
	log  *slog.Logger

	mu        sync.Mutex
	lastState *wg.ApplyState
}

func New(cfg Config, mgr *wg.Manager, kv kvClient, log *slog.Logger) (*Service, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("wgmesh: NodeID required")
	}
	if mgr == nil {
		return nil, errors.New("wgmesh: manager required")
	}
	if kv == nil {
		return nil, errors.New("wgmesh: kv client required")
	}
	if cfg.ListenPort == 0 {
		cfg.ListenPort = wg.DefaultListenPort
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{cfg: cfg, mgr: mgr, nats: kv, log: log.With("component", "wgmesh")}, nil
}

func (s *Service) Run(ctx context.Context) error {
	if err := s.publishPubkey(ctx); err != nil {
		s.log.Warn("publish pubkey failed", "err", err)
	}
	key := KeyPrefix + string(s.cfg.NodeID)
	go s.republishLoop(ctx)
	go s.selfHealLoop(ctx)
	for {
		err := s.nats.KVWatchKey(ctx, BucketPeers, key, s.handlePeerUpdate)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.log.Warn("kv watch ended; restarting", "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (s *Service) publishPubkey(ctx context.Context) error {
	payload, err := json.Marshal(pubkeyPayload{
		NodeID:     string(s.cfg.NodeID),
		PublicKey:  s.mgr.PublicKey(),
		ListenPort: s.cfg.ListenPort,
	})
	if err != nil {
		return fmt.Errorf("marshal pubkey: %w", err)
	}
	return s.nats.KVPut(ctx, BucketPubkeys, KeyPrefix+string(s.cfg.NodeID), payload)
}

func (s *Service) republishLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.publishPubkey(ctx); err != nil {
				s.log.Warn("republish pubkey failed", "err", err)
			}
		}
	}
}

func (s *Service) handlePeerUpdate(u nats.KVUpdate) error {
	var payload peerPayload
	if err := json.Unmarshal(u.Value, &payload); err != nil {
		return fmt.Errorf("decode peers payload: %w", err)
	}
	peers := make([]wg.Peer, 0, len(payload.Peers))
	for _, p := range payload.Peers {
		peers = append(peers, wg.Peer{
			PublicKey:  p.PublicKey,
			Endpoint:   p.Endpoint,
			ListenPort: p.ListenPort,
			Address:    p.Address,
		})
	}
	state := wg.ApplyState{
		Address:    payload.Address,
		ListenPort: payload.ListenPort,
		Peers:      peers,
	}
	if err := s.mgr.Apply(state); err != nil {
		return fmt.Errorf("apply: %w", err)
	}
	s.mu.Lock()
	cached := state
	s.lastState = &cached
	s.mu.Unlock()
	s.log.Info("wg config applied", "address", payload.Address, "peers", len(peers))
	return nil
}

func (s *Service) selfHealLoop(ctx context.Context) {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.maybeSelfHeal()
		}
	}
}

func (s *Service) maybeSelfHeal() {
	s.mu.Lock()
	st := s.lastState
	s.mu.Unlock()
	if st == nil || len(st.Peers) == 0 {
		return
	}
	stats, err := s.mgr.PeerStats(time.Now())
	if err != nil {
		s.log.Warn("self-heal peerstats failed", "err", err)
		return
	}
	stale := 0
	for _, ps := range stats {
		if !ps.HandshakeOK {
			stale++
		}
	}
	if stale == 0 {
		return
	}
	if err := s.mgr.Apply(*st); err != nil {
		s.log.Warn("self-heal reapply failed", "stale_peers", stale, "err", err)
		return
	}
	s.log.Warn("wg mesh self-heal reapplied config", "stale_peers", stale, "peers", len(st.Peers))
}

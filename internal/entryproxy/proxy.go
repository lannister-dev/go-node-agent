// Package entryproxy is the embedded VLESS+REALITY entry proxy. It terminates
// REALITY/VLESS via sing-box libraries and routes each authenticated user to a
// backend over the wg-mesh from an in-memory map — users and routing are
// mutated live, without a config reload (the reason it replaces the external
// sing-box on entry nodes). See docs/adr/0005.
package entryproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	singtls "github.com/sagernet/sing-box/common/tls"
	singlog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-vmess/vless"
	"github.com/sagernet/sing/common/auth"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/lannister-dev/go-node-agent/internal/ports"
)

type Config struct {
	ListenAddr       string // ":443"
	RealityKey       string // REALITY private key (base64)
	ShortID          string
	ServerName       string // e.g. www.cloudflare.com
	HandshakeServer  string
	HandshakePort    uint16
	HandshakeTimeout time.Duration // bound on the inbound REALITY handshake (default 10s)
	DialTimeout      time.Duration // bound on dialing the backend (default 5s)
}

type Proxy struct {
	cfg              Config
	log              *slog.Logger
	tls              singtls.ServerConfig
	service          *vless.Service[string]
	handshakeTimeout time.Duration
	dialTimeout      time.Duration

	epoch int64

	mu       sync.RWMutex
	users    map[string]string // clientID -> flow
	backends map[string]ports.EntryBackend
	route    map[string]string // clientID -> backendID
	active   map[uint64]*connStat
	nextID   uint64

	ln net.Listener
}

type connStat struct {
	id        uint64
	clientID  string
	backendID string
	up        atomic.Uint64
	down      atomic.Uint64
}

func New(cfg Config, log *slog.Logger) (*Proxy, error) {
	if cfg.ListenAddr == "" {
		return nil, errors.New("entryproxy: ListenAddr required")
	}
	if cfg.RealityKey == "" || cfg.ShortID == "" || cfg.ServerName == "" {
		return nil, errors.New("entryproxy: REALITY key, short_id and server_name required")
	}
	if log == nil {
		log = slog.Default()
	}
	handshakeServer := cfg.HandshakeServer
	if handshakeServer == "" {
		handshakeServer = cfg.ServerName
	}
	handshakePort := cfg.HandshakePort
	if handshakePort == 0 {
		handshakePort = 443
	}

	tlsCfg, err := singtls.NewServer(context.Background(), singlog.NewNOPFactory().Logger(), option.InboundTLSOptions{
		Enabled:    true,
		ServerName: cfg.ServerName,
		Reality: &option.InboundRealityOptions{
			Enabled:    true,
			PrivateKey: cfg.RealityKey,
			ShortID:    []string{cfg.ShortID},
			Handshake: option.InboundRealityHandshakeOptions{
				ServerOptions: option.ServerOptions{Server: handshakeServer, ServerPort: handshakePort},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("entryproxy: build REALITY server: %w", err)
	}

	handshakeTimeout := cfg.HandshakeTimeout
	if handshakeTimeout <= 0 {
		handshakeTimeout = 10 * time.Second
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 5 * time.Second
	}

	p := &Proxy{
		cfg:              cfg,
		log:              log.With("component", "entryproxy"),
		tls:              tlsCfg,
		epoch:            time.Now().UnixNano(),
		handshakeTimeout: handshakeTimeout,
		dialTimeout:      dialTimeout,
		users:            map[string]string{},
		backends:         map[string]ports.EntryBackend{},
		route:            map[string]string{},
		active:           map[uint64]*connStat{},
	}
	p.service = vless.NewService[string](logger.NOP(), adapter.NewUpstreamContextHandlerEx(p.handleConn, p.handlePacket))
	p.service.UpdateUsers(nil, nil, nil)
	return p, nil
}

func (p *Proxy) Start(ctx context.Context) error {
	if err := p.tls.Start(); err != nil {
		return fmt.Errorf("entryproxy: start tls: %w", err)
	}
	ln, err := net.Listen("tcp", p.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("entryproxy: listen %s: %w", p.cfg.ListenAddr, err)
	}
	p.ln = ln
	p.log.Info("entry proxy listening", "addr", p.cfg.ListenAddr)
	go p.acceptLoop(ctx)
	return nil
}

func (p *Proxy) Close() error {
	if p.ln != nil {
		return p.ln.Close()
	}
	return nil
}

// Addr is the bound listen address (useful when ListenAddr uses port 0).
func (p *Proxy) Addr() string {
	if p.ln == nil {
		return ""
	}
	return p.ln.Addr().String()
}

func (p *Proxy) acceptLoop(ctx context.Context) {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			p.log.Warn("accept failed", "err", err)
			return
		}
		go p.serve(ctx, conn)
	}
}

func (p *Proxy) serve(ctx context.Context, conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			p.log.Error("serve panic recovered", "err", r)
			_ = conn.Close()
		}
	}()

	_ = conn.SetDeadline(time.Now().Add(p.handshakeTimeout))
	tlsConn, err := singtls.ServerHandshake(ctx, conn, p.tls)
	if err != nil {
		_ = conn.Close()
		return
	}
	_ = conn.SetDeadline(time.Time{}) // clear — the relay runs unbounded
	source := M.SocksaddrFromNet(conn.RemoteAddr())
	md := adapter.InboundContext{Source: source}
	onClose := func(error) {}
	if err := p.service.NewConnection(adapter.WithContext(ctx, &md), tlsConn, source, onClose); err != nil {
		_ = tlsConn.Close()
	}
}

// handleConn is the dispatcher: the authenticated user is the routing key.
func (p *Proxy) handleConn(ctx context.Context, conn net.Conn, md adapter.InboundContext, onClose N.CloseHandlerFunc) {
	clientID, ok := auth.UserFromContext[string](ctx)
	if !ok {
		_ = conn.Close()
		return
	}

	p.mu.RLock()
	be, ok := p.backends[p.route[clientID]]
	p.mu.RUnlock()
	if !ok {
		p.log.Warn("no backend for user", "client_id", clientID)
		_ = conn.Close()
		return
	}

	dctx, cancel := context.WithTimeout(ctx, p.dialTimeout)
	raw, err := (&net.Dialer{}).DialContext(dctx, "tcp", net.JoinHostPort(be.Address, strconv.Itoa(int(be.Port))))
	cancel()
	if err != nil {
		p.log.Warn("dial backend failed", "backend", be.ID, "err", err)
		_ = conn.Close()
		return
	}
	// The entry authenticates to the backend as the user itself (flow is empty
	// on the mesh leg — no TLS there). The client is keyed only by clientID.
	vc, err := vless.NewClient(clientID, "", logger.NOP())
	if err != nil {
		_ = raw.Close()
		_ = conn.Close()
		return
	}
	up, err := vc.DialEarlyConn(raw, md.Destination)
	if err != nil {
		_ = raw.Close()
		_ = conn.Close()
		return
	}

	p.mu.Lock()
	p.nextID++
	stat := &connStat{id: p.nextID, clientID: clientID, backendID: be.ID}
	p.active[stat.id] = stat
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.active, stat.id)
		p.mu.Unlock()
	}()
	relay(conn, up, stat)
}

func (p *Proxy) handlePacket(context.Context, N.PacketConn, adapter.InboundContext, N.CloseHandlerFunc) {
	// UDP not used by the entry profile yet.
}

func (p *Proxy) AddUser(_ context.Context, clientID, flow string) error {
	if clientID == "" {
		return errors.New("entryproxy: empty clientID")
	}
	p.mu.Lock()
	p.users[clientID] = flow
	p.syncUsersLocked()
	p.mu.Unlock()
	return nil
}

func (p *Proxy) RemoveUser(_ context.Context, clientID string) error {
	p.mu.Lock()
	delete(p.users, clientID)
	delete(p.route, clientID)
	p.syncUsersLocked()
	p.mu.Unlock()
	return nil
}

func (p *Proxy) SelectBackend(_ context.Context, clientID, backendID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.backends[backendID]; !ok {
		return fmt.Errorf("entryproxy: unknown backend %s", backendID)
	}
	p.route[clientID] = backendID
	return nil
}

func (p *Proxy) SetBackends(_ context.Context, specs []ports.EntryBackend) error {
	next := make(map[string]ports.EntryBackend, len(specs))
	for _, s := range specs {
		next[s.ID] = s
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.backends = next
	return nil
}

func (p *Proxy) BackendConnections(_ context.Context, backendID string) (uint64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var n uint64
	for _, s := range p.active {
		if s.backendID == backendID {
			n++
		}
	}
	return n, nil
}

func (p *Proxy) ActiveConnections(_ context.Context) ([]ports.EntryConnection, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]ports.EntryConnection, 0, len(p.active))
	for _, s := range p.active {
		out = append(out, ports.EntryConnection{
			ID:        strconv.FormatUint(s.id, 10),
			ClientID:  s.clientID,
			BackendID: s.backendID,
			Upload:    s.up.Load(),
			Download:  s.down.Load(),
		})
	}
	return out, nil
}

func (p *Proxy) Epoch() int64 { return p.epoch }

// syncUsersLocked rebuilds the VLESS user set from p.users. UpdateUsers replaces
// the whole set in-memory — this is the live add/remove with no reload.
func (p *Proxy) syncUsersLocked() {
	ids := make([]string, 0, len(p.users))
	flows := make([]string, 0, len(p.users))
	for id, flow := range p.users {
		ids = append(ids, id)
		flows = append(flows, flow)
	}
	// userList key == uuid == clientID, so the dispatcher reads it back via auth context.
	p.service.UpdateUsers(ids, ids, flows)
}

// relay splices the client conn and the backend conn, counting upload
// (client→backend) and download (backend→client) bytes into stat.
func relay(client, backend net.Conn, stat *connStat) {
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn, counter *atomic.Uint64) {
		_, _ = io.Copy(countingWriter{dst, counter}, src)
		_ = dst.Close()
		_ = src.Close()
		done <- struct{}{}
	}
	go cp(backend, client, &stat.up)
	go cp(client, backend, &stat.down)
	<-done
	<-done
}

type countingWriter struct {
	w io.Writer
	n *atomic.Uint64
}

func (c countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 {
		c.n.Add(uint64(n))
	}
	return n, err
}

var _ ports.EntryProxy = (*Proxy)(nil)

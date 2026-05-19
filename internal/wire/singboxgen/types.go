package singboxgen

import "github.com/lannister-dev/go-node-agent/internal/domain"

type ListenSpec struct {
	Address string
	Port    uint16
	Sniff   bool
}

type RealitySpec struct {
	Enabled    bool
	ServerName string
	PublicKey  string
	ShortIDs   []string
	Handshake  string
}

type InboundSpec struct {
	Tag     string
	Listen  ListenSpec
	Reality RealitySpec
}

type BackendSpec struct {
	ID         domain.BackendID
	Address    string
	Port       uint16
	ServerName string
	Reality    RealitySpec
	Transport  domain.TransportKind
}

type LogSpec struct {
	Level    string
	Disabled bool
}

type ClashAPISpec struct {
	Enabled    bool
	ExternalCt string
	Secret     string
}

type NodeState struct {
	Log        LogSpec
	ClashAPI   ClashAPISpec
	Inbound    InboundSpec
	Backends   []BackendSpec
	Placements []domain.Placement
}

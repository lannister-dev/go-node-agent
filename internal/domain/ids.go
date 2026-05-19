package domain

type (
	PlacementID string
	KeyID       string
	ClientID    string
	NodeID      string
	BackendID   string
	OpVersion   uint64
)

type TransportKind string

const (
	TransportTCP     TransportKind = "tcp"
	TransportWS      TransportKind = "ws"
	TransportXHTTP   TransportKind = "xhttp"
	TransportReality TransportKind = "reality"
)

func (t TransportKind) Valid() bool {
	switch t {
	case TransportTCP, TransportWS, TransportXHTTP, TransportReality:
		return true
	}
	return false
}

type Protocol string

const (
	ProtocolVLESS Protocol = "vless"
)

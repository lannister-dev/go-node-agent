package ports

import "context"

type SingBoxConfig struct {
	Raw []byte
}

type SingBoxConnections struct {
	Total       uint64
	PerOutbound map[string]uint64
	Conns       []SingBoxConn
}

type SingBoxConn struct {
	ID       string
	Chains   []string
	Upload   uint64
	Download uint64
}

type SingBox interface {
	WriteConfig(ctx context.Context, cfg SingBoxConfig) error
	Reload(ctx context.Context) error
	Connections(ctx context.Context) (SingBoxConnections, error)
	Healthy(ctx context.Context) error
}

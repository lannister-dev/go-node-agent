package domain

type Backend struct {
	ID         BackendID
	NodeID     NodeID
	Address    string
	Port       uint16
	Transport  TransportKind
	Healthy    bool
	Draining   bool
	CapacityKB uint64
}

type Upstream struct {
	Name     string
	Backends []Backend
}

type Pool struct {
	Name  string
	Slots []BackendID
}

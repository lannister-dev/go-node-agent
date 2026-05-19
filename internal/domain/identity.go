package domain

import "time"

type NodeIdentity struct {
	NodeID          NodeID
	AgentInstanceID string
	AuthToken       string
	BootstrappedAt  time.Time
}

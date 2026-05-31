package domain

import "time"

type DesiredState string

const (
	DesiredActive   DesiredState = "active"
	DesiredInactive DesiredState = "inactive"
)

func (d DesiredState) Valid() bool {
	switch d {
	case DesiredActive, DesiredInactive:
		return true
	}
	return false
}

type AppliedState string

const (
	AppliedPending AppliedState = "pending"
	AppliedOk      AppliedState = "applied"
	AppliedError   AppliedState = "error"
)

func (a AppliedState) Valid() bool {
	switch a {
	case AppliedPending, AppliedOk, AppliedError:
		return true
	}
	return false
}

type ReportStatus string

const (
	ReportApplied           ReportStatus = "applied"
	ReportPending           ReportStatus = "pending"
	ReportError             ReportStatus = "error"
	ReportSkippedStale      ReportStatus = "skipped_stale"
	ReportSkippedIdempotent ReportStatus = "skipped_idempotent"
)

func (r ReportStatus) Valid() bool {
	switch r {
	case ReportApplied, ReportPending, ReportError, ReportSkippedStale, ReportSkippedIdempotent:
		return true
	}
	return false
}

type Placement struct {
	ID               PlacementID
	KeyID            KeyID
	ClientID         ClientID
	NodeID           NodeID
	BackendNodeID    BackendID
	Desired          DesiredState
	Applied          AppliedState
	OpVersion        OpVersion
	Protocol         Protocol
	Transport        TransportKind
	IsRevoked        bool
	ValidUntil       time.Time
	UpdatedAt        time.Time
	LastAppliedAt    time.Time
	EntryOverrideTag string
}

type PlacementReport struct {
	PlacementID  PlacementID
	NodeID       NodeID
	OpVersion    OpVersion
	AppliedState AppliedState
	Status       ReportStatus
	Retryable    bool
	Err          string
	AppliedAt    time.Time
}

type PlacementCommand struct {
	EventID          string
	NodeID           NodeID
	EmittedAt        time.Time
	SnapshotID       string
	SnapshotComplete bool
	Placement        Placement
}

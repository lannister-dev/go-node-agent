package domain

import "time"

type FlipState string

const (
	FlipSteady    FlipState = "steady"
	FlipAnnounced FlipState = "announced"
	FlipWarming   FlipState = "warming"
	FlipSwap      FlipState = "swap"
	FlipCooling   FlipState = "cooling"
	FlipCool      FlipState = "cool"
)

type FlipPlan struct {
	PlacementID  PlacementID
	OldBackend   BackendID
	NewBackend   BackendID
	State        FlipState
	StartedAt    time.Time
	DeadlineAt   time.Time
	DrainTimeout time.Duration
	OpVersion    OpVersion
	Desired      Placement
}

func (p FlipPlan) Next() FlipState {
	switch p.State {
	case FlipSteady:
		return FlipAnnounced
	case FlipAnnounced:
		return FlipWarming
	case FlipWarming:
		return FlipSwap
	case FlipSwap:
		return FlipCooling
	case FlipCooling:
		return FlipCool
	case FlipCool:
		return FlipSteady
	}
	return FlipSteady
}

func (p FlipPlan) Terminal() bool {
	return p.State == FlipSteady
}

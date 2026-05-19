package domain

import "time"

type TrafficDelta struct {
	ClientID  ClientID
	UpBytes   uint64
	DownBytes uint64
	Since     time.Time
	Until     time.Time
}

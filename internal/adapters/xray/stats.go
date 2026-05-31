package xray

import (
	"context"
	"errors"
	"fmt"
	"strings"

	stats "github.com/lannister-dev/go-node-agent/pkg/proto/xray/app/stats/command"
)

const (
	userStatPrefix     = "user>>>"
	userStatTrafficSep = ">>>traffic>>>"
)

// UserStat is a single (uplink|downlink) counter for one user.
type UserStat struct {
	ClientID string
	Link     string // "uplink" or "downlink"
	Value    int64
}

// QueryUserStats fetches per-user traffic counters from xray's stats service.
// pattern="user>>>" matches all user-traffic stats. If reset is true, xray
// zeroes the counters after the read so each call returns the delta since
// the previous call.
func (c *Client) QueryUserStats(ctx context.Context, reset bool) ([]UserStat, error) {
	if c.conn == nil {
		return nil, errors.New("xray: not initialized")
	}
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	cli := stats.NewStatsServiceClient(c.conn)
	resp, err := cli.QueryStats(ctx, &stats.QueryStatsRequest{
		Pattern: "user>>>",
		Reset_:  reset,
	})
	if err != nil {
		return nil, fmt.Errorf("xray: QueryStats: %w", err)
	}
	out := make([]UserStat, 0, len(resp.GetStat()))
	for _, s := range resp.GetStat() {
		clientID, link := parseUserStatName(s.GetName())
		if clientID == "" || link == "" {
			continue
		}
		out = append(out, UserStat{ClientID: clientID, Link: link, Value: s.GetValue()})
	}
	return out, nil
}

// parseUserStatName splits "user>>>EMAIL>>>traffic>>>uplink" (or downlink).
// Returns ("","") for malformed input or empty client id.
func parseUserStatName(name string) (clientID, link string) {
	if !strings.HasPrefix(name, userStatPrefix) {
		return "", ""
	}
	rest := name[len(userStatPrefix):]
	idx := strings.Index(rest, userStatTrafficSep)
	if idx <= 0 {
		return "", ""
	}
	return rest[:idx], rest[idx+len(userStatTrafficSep):]
}

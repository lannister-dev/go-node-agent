package xray

import "testing"

func TestParseUserStatName(t *testing.T) {
	cases := []struct {
		in       string
		clientID string
		link     string
	}{
		{"user>>>aaaa>>>traffic>>>uplink", "aaaa", "uplink"},
		{"user>>>bbbb-uuid>>>traffic>>>downlink", "bbbb-uuid", "downlink"},
		{"user>>>8add00c8-f944-4a7b-9839-e39c5eabdb6b>>>traffic>>>uplink", "8add00c8-f944-4a7b-9839-e39c5eabdb6b", "uplink"},
		{"inbound>>>x>>>traffic>>>uplink", "", ""},
		{"garbage", "", ""},
		{"", "", ""},
	}
	for _, c := range cases {
		id, link := parseUserStatName(c.in)
		if id != c.clientID || link != c.link {
			t.Errorf("%q → (%q,%q), want (%q,%q)", c.in, id, link, c.clientID, c.link)
		}
	}
}

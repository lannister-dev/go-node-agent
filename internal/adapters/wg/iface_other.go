//go:build !linux

package wg

import (
	"errors"
	"net"
)

func recreateInterface(_ string) error { return nil }

func ensureInterface(_ string, _ net.IP, _ net.IPMask) error {
	return errors.New("wg: only supported on linux")
}

func syncRoutes(_ string, _ []*net.IPNet) error {
	return errors.New("wg: only supported on linux")
}

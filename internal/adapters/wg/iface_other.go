//go:build !linux

package wg

import (
	"errors"
	"net"
)

func ensureInterface(_ string, _ net.IP, _ net.IPMask) error {
	return errors.New("wg: interface management only supported on Linux")
}

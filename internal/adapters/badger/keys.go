package badger

import (
	"encoding/binary"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

const (
	prefixPlacement = "p/"
	prefixCursor    = "c/"
	keyIdentityName = "i/self"
)

func keyPlacement(id domain.PlacementID) []byte {
	b := make([]byte, 0, len(prefixPlacement)+len(id))
	b = append(b, prefixPlacement...)
	b = append(b, id...)
	return b
}

func keyCursor(name string) []byte {
	b := make([]byte, 0, len(prefixCursor)+len(name))
	b = append(b, prefixCursor...)
	b = append(b, name...)
	return b
}

func encodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func decodeUint64(b []byte) (uint64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(b), true
}

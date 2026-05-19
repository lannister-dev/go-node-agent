package badger

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

func (s *Store) GetCursor(ctx context.Context, name string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var seq uint64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyCursor(name))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		return item.Value(func(val []byte) error {
			v, ok := decodeUint64(val)
			if !ok {
				return fmt.Errorf("badger: cursor %q has invalid encoding (len=%d)", name, len(val))
			}
			seq = v
			return nil
		})
	})
	if err != nil {
		return 0, fmt.Errorf("badger: get cursor %s: %w", name, err)
	}
	return seq, nil
}

func (s *Store) PutCursor(ctx context.Context, name string, seq uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyCursor(name), encodeUint64(seq))
	}); err != nil {
		return fmt.Errorf("badger: put cursor %s: %w", name, err)
	}
	return nil
}

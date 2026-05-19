package badger

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

func (s *Store) GetIdentity(ctx context.Context) (domain.NodeIdentity, bool, error) {
	if err := ctx.Err(); err != nil {
		return domain.NodeIdentity{}, false, err
	}
	var id domain.NodeIdentity
	var found bool
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(keyIdentityName))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		return item.Value(func(val []byte) error {
			parsed, perr := unmarshalIdentity(val)
			if perr != nil {
				return perr
			}
			id = parsed
			found = true
			return nil
		})
	})
	if err != nil {
		return domain.NodeIdentity{}, false, fmt.Errorf("badger: get identity: %w", err)
	}
	return id, found, nil
}

func (s *Store) PutIdentity(ctx context.Context, id domain.NodeIdentity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := marshalIdentity(id)
	if err != nil {
		return fmt.Errorf("badger: marshal identity: %w", err)
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(keyIdentityName), data)
	}); err != nil {
		return fmt.Errorf("badger: put identity: %w", err)
	}
	return nil
}

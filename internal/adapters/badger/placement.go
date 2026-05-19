package badger

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"

	"github.com/lannister-dev/go-node-agent/internal/domain"
)

func (s *Store) GetPlacement(ctx context.Context, id domain.PlacementID) (domain.Placement, bool, error) {
	if err := ctx.Err(); err != nil {
		return domain.Placement{}, false, err
	}
	var p domain.Placement
	var found bool
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyPlacement(id))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		return item.Value(func(val []byte) error {
			parsed, perr := unmarshalPlacement(val)
			if perr != nil {
				return perr
			}
			p = parsed
			found = true
			return nil
		})
	})
	if err != nil {
		return domain.Placement{}, false, fmt.Errorf("badger: get placement %s: %w", id, err)
	}
	return p, found, nil
}

func (s *Store) PutPlacement(ctx context.Context, p domain.Placement) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := marshalPlacement(p)
	if err != nil {
		return fmt.Errorf("badger: marshal placement %s: %w", p.ID, err)
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyPlacement(p.ID), data)
	}); err != nil {
		return fmt.Errorf("badger: put placement %s: %w", p.ID, err)
	}
	return nil
}

func (s *Store) DeletePlacement(ctx context.Context, id domain.PlacementID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(keyPlacement(id))
	}); err != nil {
		return fmt.Errorf("badger: delete placement %s: %w", id, err)
	}
	return nil
}

func (s *Store) ListPlacements(ctx context.Context) ([]domain.Placement, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []domain.Placement
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte(prefixPlacement)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			err := it.Item().Value(func(val []byte) error {
				p, perr := unmarshalPlacement(val)
				if perr != nil {
					return perr
				}
				out = append(out, p)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("badger: list placements: %w", err)
	}
	return out, nil
}

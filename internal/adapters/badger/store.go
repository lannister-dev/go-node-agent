package badger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dgraph-io/badger/v4"
)

type Store struct {
	db       *badger.DB
	log      *slog.Logger
	inMemory bool
}

type Options struct {
	Path     string
	InMemory bool
	Logger   *slog.Logger
}

func Open(opts Options) (*Store, error) {
	var bopts badger.Options
	if opts.InMemory {
		bopts = badger.DefaultOptions("").WithInMemory(true)
	} else {
		if opts.Path == "" {
			return nil, errors.New("badger: path required when InMemory=false")
		}
		bopts = badger.DefaultOptions(opts.Path)
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	bopts = bopts.WithLogger(badgerSlogAdapter{log: log.With("component", "badger")})

	db, err := badger.Open(bopts)
	if err != nil {
		return nil, fmt.Errorf("badger: open: %w", err)
	}
	return &Store{db: db, log: log, inMemory: opts.InMemory}, nil
}

func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("badger: close: %w", err)
	}
	s.db = nil
	return nil
}

func (s *Store) Snapshot(ctx context.Context) error {
	if s.inMemory {
		return nil
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("badger: sync: %w", err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.db.RunValueLogGC(0.5)
		if errors.Is(err, badger.ErrNoRewrite) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("badger: gc: %w", err)
		}
	}
}

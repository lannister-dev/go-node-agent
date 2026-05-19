package backends

import (
	"sync"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

type Registry struct {
	mu    sync.RWMutex
	specs map[domain.BackendID]singboxgen.BackendSpec
	order []domain.BackendID
}

func NewRegistry() *Registry {
	return &Registry{specs: map[domain.BackendID]singboxgen.BackendSpec{}}
}

func (r *Registry) Upsert(spec singboxgen.BackendSpec) (added bool) {
	if spec.ID == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.specs[spec.ID]; !exists {
		r.order = append(r.order, spec.ID)
		added = true
	}
	r.specs[spec.ID] = spec
	return
}

func (r *Registry) Get(id domain.BackendID) (singboxgen.BackendSpec, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.specs[id]
	return b, ok
}

func (r *Registry) All() []singboxgen.BackendSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]singboxgen.BackendSpec, 0, len(r.order))
	for _, id := range r.order {
		out = append(out, r.specs[id])
	}
	return out
}

func (r *Registry) Remove(id domain.BackendID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.specs[id]; !ok {
		return false
	}
	delete(r.specs, id)
	for i, v := range r.order {
		if v == id {
			r.order = append(r.order[:i], r.order[i+1:]...)
			break
		}
	}
	return true
}

func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.specs)
}

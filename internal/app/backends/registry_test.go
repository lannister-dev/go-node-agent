package backends

import (
	"sync"
	"testing"

	"github.com/lannister-dev/go-node-agent/internal/domain"
	"github.com/lannister-dev/go-node-agent/internal/wire/singboxgen"
)

func sample(id domain.BackendID, addr string, port uint16) singboxgen.BackendSpec {
	return singboxgen.BackendSpec{ID: id, Address: addr, Port: port, Transport: domain.TransportWS}
}

func TestRegistry_UpsertAddsAndUpdates(t *testing.T) {
	r := NewRegistry()
	if added := r.Upsert(sample("praha-02", "10.0.0.2", 9000)); !added {
		t.Error("first upsert should report added=true")
	}
	if r.Len() != 1 {
		t.Errorf("len: %d", r.Len())
	}

	if added := r.Upsert(sample("praha-02", "10.0.0.99", 9001)); added {
		t.Error("upsert of existing should report added=false")
	}
	got, ok := r.Get("praha-02")
	if !ok || got.Address != "10.0.0.99" || got.Port != 9001 {
		t.Errorf("update lost: %+v", got)
	}
}

func TestRegistry_GetMissingReturnsFalse(t *testing.T) {
	r := NewRegistry()
	if _, ok := r.Get("ghost"); ok {
		t.Error("expected ok=false for missing")
	}
}

func TestRegistry_UpsertRejectsEmptyID(t *testing.T) {
	r := NewRegistry()
	if r.Upsert(sample("", "1.1.1.1", 80)) {
		t.Error("empty id should be rejected")
	}
	if r.Len() != 0 {
		t.Error("empty id should not be stored")
	}
}

func TestRegistry_AllPreservesInsertionOrder(t *testing.T) {
	r := NewRegistry()
	for _, id := range []domain.BackendID{"z", "a", "m"} {
		r.Upsert(sample(id, "1.1.1.1", 80))
	}
	all := r.All()
	if len(all) != 3 || all[0].ID != "z" || all[1].ID != "a" || all[2].ID != "m" {
		t.Errorf("order broken: %+v", all)
	}
}

func TestRegistry_Remove(t *testing.T) {
	r := NewRegistry()
	r.Upsert(sample("a", "1.1.1.1", 80))
	r.Upsert(sample("b", "2.2.2.2", 80))
	if !r.Remove("a") {
		t.Error("Remove existing should return true")
	}
	if r.Remove("ghost") {
		t.Error("Remove missing should return false")
	}
	if r.Len() != 1 {
		t.Errorf("len after remove: %d", r.Len())
	}
	all := r.All()
	if len(all) != 1 || all[0].ID != "b" {
		t.Errorf("after remove: %+v", all)
	}
}

func TestRegistry_ConcurrentReadsAndWrites(t *testing.T) {
	r := NewRegistry()
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			id := domain.BackendID(rune('a' + (i % 26)))
			r.Upsert(sample(id, "1.1.1.1", uint16(80+i%100)))
		}(i)
		go func(i int) {
			defer wg.Done()
			id := domain.BackendID(rune('a' + (i % 26)))
			_, _ = r.Get(id)
			_ = r.All()
			_ = r.Len()
		}(i)
	}
	wg.Wait()
	if r.Len() == 0 {
		t.Error("expected at least one entry after concurrent upserts")
	}
}

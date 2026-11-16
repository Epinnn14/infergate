package router

import (
	"fmt"
	"math"
	"testing"

	"github.com/Epinnn14/infergate/internal/queue"
)

func workers(n, capacity int) []queue.Worker {
	out := make([]queue.Worker, n)
	for i := range out {
		out[i] = queue.Worker{ID: fmt.Sprintf("w%d", i), Capacity: capacity}
	}
	return out
}

func TestPrefixHash(t *testing.T) {
	sys := queue.Message{Role: "system", Content: "You are a helpful assistant."}
	a := PrefixHash([]queue.Message{sys, {Role: "user", Content: "hi"}})
	b := PrefixHash([]queue.Message{sys, {Role: "user", Content: "something else"}})
	if a == "" || a != b {
		t.Fatalf("same system prompt should give the same prefix: %q %q", a, b)
	}
	if PrefixHash([]queue.Message{{Role: "user", Content: "hi"}}) != "" {
		t.Fatal("a single message has no reusable prefix")
	}
}

func TestChooseIsStickyAndStable(t *testing.T) {
	ws := workers(8, 4)
	first := Choose("m", "abc", ws, Options{})
	for i := 0; i < 10; i++ {
		if d := Choose("m", "abc", ws, Options{}); d.Worker != first.Worker || d.Kind != Affinity {
			t.Fatalf("not sticky: %+v vs %+v", d, first)
		}
	}
	// removing an unrelated worker doesn't move the prefix (rendezvous hashing)
	var rest []queue.Worker
	for _, w := range ws {
		if w.ID != first.Worker && len(rest) < 5 {
			rest = append(rest, w)
		}
	}
	rest = append(rest, queue.Worker{ID: first.Worker, Capacity: 4})
	if d := Choose("m", "abc", rest, Options{}); d.Worker != first.Worker {
		t.Fatalf("prefix moved after removing other workers: %s → %s", first.Worker, d.Worker)
	}
}

func TestChooseSpillsAndFallsBack(t *testing.T) {
	ws := workers(4, 2)
	top := Choose("m", "p", ws, Options{})
	for i := range ws {
		if ws[i].ID == top.Worker {
			ws[i].Inflight = 2 // full
		}
	}
	spill := Choose("m", "p", ws, Options{})
	if spill.Kind != Spill || spill.Worker == top.Worker {
		t.Fatalf("want spill to the next worker, got %+v", spill)
	}
	for i := range ws {
		ws[i].Inflight = 2
	}
	if d := Choose("m", "p", ws, Options{}); d.Kind != Shared || d.Stream != queue.SharedStream("m") {
		t.Fatalf("all busy: want shared queue, got %+v", d)
	}
	if d := Choose("m", "", ws, Options{}); d.Kind != Shared {
		t.Fatalf("no prefix: want shared, got %+v", d)
	}
}

func TestPrefixesSpreadEvenly(t *testing.T) {
	ws := workers(5, 8)
	count := map[string]int{}
	for i := 0; i < 5000; i++ {
		count[Choose("m", fmt.Sprintf("conv-%d", i), ws, Options{}).Worker]++
	}
	for id, c := range count {
		if math.Abs(float64(c)-1000) > 150 {
			t.Fatalf("uneven spread: %s got %d of 5000", id, c)
		}
	}
}

func TestChooseVersionWeights(t *testing.T) {
	vs := []Version{{Name: "v1", Weight: 90}, {Name: "v2", Weight: 10}}
	n := map[string]int{}
	for i := 0; i < 10000; i++ {
		v, _ := ChooseVersion(vs, fmt.Sprintf("conv-%d", i))
		n[v]++
	}
	if n["v2"] < 800 || n["v2"] > 1200 {
		t.Fatalf("canary share off: %v", n)
	}
	a, _ := ChooseVersion(vs, "same")
	b, _ := ChooseVersion(vs, "same")
	if a != b {
		t.Fatal("not sticky")
	}
	if _, ok := ChooseVersion([]Version{{Name: "x", Weight: 0}}, "k"); ok {
		t.Fatal("all weights zero must not route")
	}
}

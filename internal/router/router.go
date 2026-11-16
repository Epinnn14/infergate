// Package router decides where a request goes.
//
// Two decisions:
//
//  1. Which version of a model serves it (canary rollouts): weights come from the registry,
//     and the choice is sticky per conversation, so one chat doesn't flip between versions.
//  2. Which worker of that version: requests that share a prompt prefix (system prompt plus
//     earlier turns) go to the same worker, whose KV/prefix cache already holds that prefix.
//     Workers are ranked by rendezvous hashing, so adding or removing a worker only moves
//     the conversations that hashed to it. A busy worker is skipped for the next in rank,
//     and when the top choices are all busy the request goes to the model's shared queue,
//     where any free worker takes it.
package router

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/Epinnn14/infergate/internal/queue"
)

// PrefixHash identifies the cacheable part of a chat: everything before the last message.
// A single-message chat has no reusable prefix and returns "".
func PrefixHash(msgs []queue.Message) string {
	if len(msgs) < 2 {
		return ""
	}
	h := sha256.New()
	for _, m := range msgs[:len(msgs)-1] {
		h.Write([]byte(m.Role))
		h.Write([]byte{0})
		h.Write([]byte(m.Content))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func score(key, worker string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	h.Write([]byte{0})
	h.Write([]byte(worker))
	return mix(h.Sum64())
}

// mix is murmur3's 64-bit finalizer: FNV alone clusters on similar keys ("w1", "w2").
func mix(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// Kind of routing decision, also a metric label.
type Kind string

const (
	Affinity Kind = "affinity" // the prefix's preferred worker
	Spill    Kind = "spill"    // preferred worker busy: the next one in rank
	Shared   Kind = "shared"   // no prefix, no live workers, or all candidates busy
)

// Decision is where to enqueue.
type Decision struct {
	Stream string
	Worker string
	Kind   Kind
}

// Options for Choose.
type Options struct {
	// a worker at or above this load (in-flight + waiting over capacity) is skipped
	MaxLoad float64
	// how many ranked workers to try before the shared queue
	Candidates int
}

// Choose picks the stream for a request of a model version.
func Choose(model, prefix string, workers []queue.Worker, o Options) Decision {
	shared := Decision{Stream: queue.SharedStream(model), Kind: Shared}
	if prefix == "" || len(workers) == 0 {
		return shared
	}
	if o.MaxLoad <= 0 {
		o.MaxLoad = 1
	}
	if o.Candidates <= 0 {
		o.Candidates = 2
	}
	ranked := append([]queue.Worker(nil), workers...)
	sort.Slice(ranked, func(i, j int) bool { return score(prefix, ranked[i].ID) > score(prefix, ranked[j].ID) })
	for i, w := range ranked {
		if i >= o.Candidates {
			break
		}
		if w.Load() < o.MaxLoad {
			k := Affinity
			if i > 0 {
				k = Spill
			}
			return Decision{Stream: queue.WorkerStream(w.ID), Worker: w.ID, Kind: k}
		}
	}
	return shared
}

// Version is one deployable version of a model alias, with its traffic weight.
type Version struct {
	Name   string `json:"name"`
	Weight int    `json:"weight"`
}

// ChooseVersion picks a version by weight. The same sticky key (API key + conversation)
// always lands on the same version while the weights don't change.
func ChooseVersion(versions []Version, sticky string) (string, bool) {
	total := 0
	for _, v := range versions {
		if v.Weight > 0 {
			total += v.Weight
		}
	}
	if total == 0 {
		return "", false
	}
	h := fnv.New64a()
	h.Write([]byte(sticky))
	pick := int(mix(h.Sum64()) % uint64(total))
	for _, v := range versions {
		if v.Weight <= 0 {
			continue
		}
		if pick < v.Weight {
			return v.Name, true
		}
		pick -= v.Weight
	}
	return versions[len(versions)-1].Name, true
}

// StickyKey is what keeps a conversation on one version: the API key and the first user turn.
func StickyKey(apiKey string, msgs []queue.Message) string {
	var b strings.Builder
	b.WriteString(apiKey)
	for _, m := range msgs {
		if m.Role == "user" {
			b.WriteString(m.Content)
			break
		}
	}
	return b.String()
}

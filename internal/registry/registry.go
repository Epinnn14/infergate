// Package registry keeps the gateway's view of models, versions, weights and API keys.
//
// The source of truth is the registry service (PostgreSQL behind a small API, see
// registry/ in this repo). The gateway polls GET /v1/routes and swaps the snapshot
// atomically; if the registry is down it keeps serving the last good snapshot.
// For running without the registry, a JSON file with the same shape works too.
package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/Epinnn14/infergate/internal/router"
)

// Model is an alias clients ask for ("chat"), served by one or more versions.
type Model struct {
	Name     string           `json:"name"`
	Versions []router.Version `json:"versions"`
}

// Key is an API key: only its SHA-256 is known here.
type Key struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Hash   string   `json:"hash"`
	RPM    int64    `json:"rpm"`    // requests per minute, 0 = unlimited
	TPM    int64    `json:"tpm"`    // tokens per minute, 0 = unlimited
	Models []string `json:"models"` // allowed aliases, empty = all
}

// Snapshot is one consistent view.
type Snapshot struct {
	Revision int64   `json:"revision"`
	Models   []Model `json:"models"`
	Keys     []Key   `json:"keys"`

	models map[string]Model
	keys   map[string]Key
}

func (s *Snapshot) index() {
	s.models = map[string]Model{}
	for _, m := range s.Models {
		s.models[m.Name] = m
	}
	s.keys = map[string]Key{}
	for _, k := range s.Keys {
		s.keys[k.Hash] = k
	}
}

// Model finds an alias. A version name asked for directly is served as itself.
func (s *Snapshot) Model(name string) (Model, bool) {
	if m, ok := s.models[name]; ok {
		return m, true
	}
	for _, m := range s.Models {
		for _, v := range m.Versions {
			if v.Name == name {
				return Model{Name: name, Versions: []router.Version{{Name: name, Weight: 1}}}, true
			}
		}
	}
	return Model{}, false
}

// Key looks up a presented API key.
func (s *Snapshot) Key(secret string) (Key, bool) {
	k, ok := s.keys[HashKey(secret)]
	return k, ok
}

// HashKey is how keys are stored: hex SHA-256.
func HashKey(secret string) string {
	h := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(h[:])
}

// Allowed says whether the key may use the alias.
func (k Key) Allowed(model string) bool {
	if len(k.Models) == 0 {
		return true
	}
	for _, m := range k.Models {
		if m == model {
			return true
		}
	}
	return false
}

// Parse reads a snapshot from JSON.
func Parse(b []byte) (*Snapshot, error) {
	var s Snapshot
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	s.index()
	return &s, nil
}

// Source holds the current snapshot.
type Source struct {
	cur atomic.Pointer[Snapshot]
	ok  atomic.Bool
}

func (s *Source) Get() *Snapshot { return s.cur.Load() }

// Ready is true once a snapshot has been loaded.
func (s *Source) Ready() bool { return s.ok.Load() }

func (s *Source) set(sn *Snapshot) {
	s.cur.Store(sn)
	s.ok.Store(true)
}

// NewStatic serves a fixed snapshot.
func NewStatic(sn *Snapshot) *Source {
	s := &Source{}
	sn.index()
	s.set(sn)
	return s
}

// FromFile loads a JSON snapshot file.
func FromFile(path string) (*Source, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	sn, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	s := &Source{}
	s.set(sn)
	return s, nil
}

// Poller refreshes from the registry service.
type Poller struct {
	URL      string
	Token    string
	Interval time.Duration
	Client   *http.Client
	Log      *slog.Logger
}

// Run loads once (so the gateway starts with routes), then keeps refreshing until ctx ends.
func (p Poller) Run(ctx context.Context, s *Source) {
	if p.Interval <= 0 {
		p.Interval = 5 * time.Second
	}
	if p.Client == nil {
		p.Client = &http.Client{Timeout: 5 * time.Second}
	}
	t := time.NewTicker(p.Interval)
	defer t.Stop()
	for {
		if err := p.once(ctx, s); err != nil && ctx.Err() == nil {
			p.Log.Warn("registry refresh failed; keeping the last routes", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (p Poller) once(ctx context.Context, s *Source) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL+"/v1/routes", nil)
	if err != nil {
		return err
	}
	if p.Token != "" {
		req.Header.Set("Authorization", "Bearer "+p.Token)
	}
	res, err := p.Client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("registry answered %s", res.Status)
	}
	var sn Snapshot
	if err := json.NewDecoder(res.Body).Decode(&sn); err != nil {
		return err
	}
	if old := s.Get(); old != nil && old.Revision == sn.Revision && sn.Revision != 0 {
		return nil
	}
	sn.index()
	s.set(&sn)
	p.Log.Info("routes updated", "revision", sn.Revision, "models", len(sn.Models), "keys", len(sn.Keys))
	return nil
}

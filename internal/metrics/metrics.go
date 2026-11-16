// Package metrics writes Prometheus text exposition format: counters, gauges and
// histograms with labels. Small on purpose; enough for /metrics on the gateway.
package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Registry holds metrics and writes them out.
type Registry struct {
	mu      sync.Mutex
	metrics []metric
}

type metric interface {
	write(w io.Writer)
}

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) add(m metric) {
	r.mu.Lock()
	r.metrics = append(r.metrics, m)
	r.mu.Unlock()
}

// Write writes every metric in exposition format.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	ms := append([]metric(nil), r.metrics...)
	r.mu.Unlock()
	for _, m := range ms {
		m.write(w)
	}
}

func labelKey(names, values []string) string {
	if len(names) != len(values) {
		panic(fmt.Sprintf("metrics: %d label names, %d values", len(names), len(values)))
	}
	parts := make([]string, len(names))
	for i := range names {
		parts[i] = names[i] + `="` + escape(values[i]) + `"`
	}
	return strings.Join(parts, ",")
}

func escape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

func braces(s string) string {
	if s == "" {
		return ""
	}
	return "{" + s + "}"
}

func num(f float64) string {
	if math.IsInf(f, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// ---------------------------------------------------------------- counter / gauge

// Vec is a counter or gauge with labels.
type Vec struct {
	name, help, kind string
	labels           []string
	mu               sync.Mutex
	vals             map[string]float64
}

func (r *Registry) Counter(name, help string, labels ...string) *Vec {
	v := &Vec{name: name, help: help, kind: "counter", labels: labels, vals: map[string]float64{}}
	r.add(v)
	return v
}

func (r *Registry) Gauge(name, help string, labels ...string) *Vec {
	v := &Vec{name: name, help: help, kind: "gauge", labels: labels, vals: map[string]float64{}}
	r.add(v)
	return v
}

func (v *Vec) Add(delta float64, values ...string) {
	k := labelKey(v.labels, values)
	v.mu.Lock()
	v.vals[k] += delta
	v.mu.Unlock()
}

func (v *Vec) Inc(values ...string) { v.Add(1, values...) }

func (v *Vec) Set(x float64, values ...string) {
	k := labelKey(v.labels, values)
	v.mu.Lock()
	v.vals[k] = x
	v.mu.Unlock()
}

// Get returns the current value (for tests).
func (v *Vec) Get(values ...string) float64 {
	k := labelKey(v.labels, values)
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.vals[k]
}

func (v *Vec) write(w io.Writer) {
	v.mu.Lock()
	defer v.mu.Unlock()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", v.name, v.help, v.name, v.kind)
	keys := make([]string, 0, len(v.vals))
	for k := range v.vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "%s%s %s\n", v.name, braces(k), num(v.vals[k]))
	}
}

// ---------------------------------------------------------------- histogram

// Histogram has cumulative buckets per label set.
type Histogram struct {
	name, help string
	labels     []string
	bounds     []float64
	mu         sync.Mutex
	series     map[string]*hist
}

type hist struct {
	counts []uint64
	sum    float64
	n      uint64
}

func (r *Registry) Histogram(name, help string, bounds []float64, labels ...string) *Histogram {
	h := &Histogram{name: name, help: help, labels: labels, bounds: bounds, series: map[string]*hist{}}
	r.add(h)
	return h
}

func (h *Histogram) Observe(x float64, values ...string) {
	k := labelKey(h.labels, values)
	h.mu.Lock()
	s := h.series[k]
	if s == nil {
		s = &hist{counts: make([]uint64, len(h.bounds))}
		h.series[k] = s
	}
	for i, b := range h.bounds {
		if x <= b {
			s.counts[i]++
		}
	}
	s.sum += x
	s.n++
	h.mu.Unlock()
}

func (h *Histogram) write(w io.Writer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
	keys := make([]string, 0, len(h.series))
	for k := range h.series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := h.series[k]
		sep := ""
		if k != "" {
			sep = ","
		}
		for i, b := range h.bounds {
			fmt.Fprintf(w, "%s_bucket{%s%sle=\"%s\"} %d\n", h.name, k, sep, num(b), s.counts[i])
		}
		fmt.Fprintf(w, "%s_bucket{%s%sle=\"+Inf\"} %d\n", h.name, k, sep, s.n)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.name, braces(k), num(s.sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.name, braces(k), s.n)
	}
}

// LatencyBuckets suit request latencies from 10 ms to a minute.
var LatencyBuckets = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

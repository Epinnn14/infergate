package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("ig_requests_total", "Requests.", "model", "code")
	c.Inc("chat", "200")
	c.Inc("chat", "200")
	c.Inc("chat", "429")
	g := r.Gauge("ig_queue_depth", "Waiting.", "model")
	g.Set(7, "chat")
	h := r.Histogram("ig_ttft_seconds", "TTFT.", []float64{0.1, 1}, "model")
	h.Observe(0.05, "chat")
	h.Observe(0.5, "chat")
	h.Observe(5, "chat")
	var b bytes.Buffer
	r.Write(&b)
	out := b.String()
	for _, want := range []string{
		`# TYPE ig_requests_total counter`,
		`ig_requests_total{model="chat",code="200"} 2`,
		`ig_queue_depth{model="chat"} 7`,
		`ig_ttft_seconds_bucket{model="chat",le="0.1"} 1`,
		`ig_ttft_seconds_bucket{model="chat",le="1"} 2`,
		`ig_ttft_seconds_bucket{model="chat",le="+Inf"} 3`,
		`ig_ttft_seconds_count{model="chat"} 3`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in\n%s", want, out)
		}
	}
}

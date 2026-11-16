// Package api is the gateway's HTTP surface: an OpenAI-compatible chat completions API in
// front of the request queues, plus health and metrics.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Epinnn14/infergate/internal/metrics"
	"github.com/Epinnn14/infergate/internal/queue"
	"github.com/Epinnn14/infergate/internal/ratelimit"
	"github.com/Epinnn14/infergate/internal/redis"
	"github.com/Epinnn14/infergate/internal/registry"
	"github.com/Epinnn14/infergate/internal/router"
)

// Config tunes the gateway.
type Config struct {
	// RequireKey rejects requests without a known API key.
	RequireKey bool
	// RequestTimeout bounds queueing + generation.
	RequestTimeout time.Duration
	// MaxQueue: above this many waiting requests for a version, new ones get 503.
	MaxQueue int64
	// MaxLoad: a worker this busy is skipped by affinity routing.
	MaxLoad float64
	// HeartbeatMaxAge: workers silent for longer are treated as gone.
	HeartbeatMaxAge time.Duration
	// DefaultMaxTokens when the request doesn't say.
	DefaultMaxTokens int
	// NoAffinity sends everything to the shared queue (for comparing routing strategies).
	NoAffinity bool
}

// Server handles requests.
type Server struct {
	cfg     Config
	rc      *redis.Client
	routes  *registry.Source
	limit   *ratelimit.Limiter
	log     *slog.Logger
	metrics *metrics.Registry

	requests *metrics.Vec
	routing  *metrics.Vec
	tokens   *metrics.Vec
	limited  *metrics.Vec
	inflight *metrics.Vec
	depth    *metrics.Vec
	workers  *metrics.Vec
	ttft     *metrics.Histogram
	duration *metrics.Histogram

	wmu   sync.Mutex
	wseen map[string]cachedWorkers
	// shared queues whose consumer group exists: KEDA reads the group's lag, even at zero workers
	groups sync.Map
}

type cachedWorkers struct {
	at time.Time
	ws []queue.Worker
}

// New wires a server.
func New(cfg Config, rc *redis.Client, routes *registry.Source, log *slog.Logger) *Server {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 2 * time.Minute
	}
	if cfg.MaxQueue <= 0 {
		cfg.MaxQueue = 1000
	}
	if cfg.MaxLoad <= 0 {
		cfg.MaxLoad = 1
	}
	if cfg.HeartbeatMaxAge <= 0 {
		cfg.HeartbeatMaxAge = 5 * time.Second
	}
	if cfg.DefaultMaxTokens <= 0 {
		cfg.DefaultMaxTokens = 256
	}
	m := metrics.NewRegistry()
	s := &Server{
		cfg: cfg, rc: rc, routes: routes, limit: ratelimit.New(rc), log: log, metrics: m,
		requests: m.Counter("infergate_requests_total", "Chat completion requests by model alias and HTTP status.", "model", "code"),
		routing:  m.Counter("infergate_routing_total", "Routing decisions: affinity, spill or shared.", "version", "kind"),
		tokens:   m.Counter("infergate_tokens_total", "Tokens by direction (prompt, completion).", "version", "direction"),
		limited:  m.Counter("infergate_rate_limited_total", "Requests refused by rate limits.", "key", "limit"),
		inflight: m.Gauge("infergate_inflight_requests", "Requests being served by this gateway replica.", "version"),
		depth:    m.Gauge("infergate_queue_depth", "Requests waiting in the shared queue (lag + pending).", "version"),
		workers:  m.Gauge("infergate_live_workers", "Workers with a recent heartbeat.", "version"),
		ttft:     m.Histogram("infergate_time_to_first_token_seconds", "Time to first token.", metrics.LatencyBuckets, "version"),
		duration: m.Histogram("infergate_request_duration_seconds", "Whole request duration.", metrics.LatencyBuckets, "version"),
		wseen:    map[string]cachedWorkers{},
	}
	return s
}

// Handler routes HTTP.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.chat)
	mux.HandleFunc("GET /v1/models", s.models)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /status", s.status)
	mux.Handle("GET /{$}", chatPage())
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		s.metrics.Write(w)
	})
	return mux
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if _, err := s.rc.Do(ctx, "PING"); err != nil {
		http.Error(w, "redis: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	if !s.routes.Ready() {
		http.Error(w, "no routes yet", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ready"))
}

// ---------------------------------------------------------------- OpenAI shapes

type chatRequest struct {
	Model         string          `json:"model"`
	Messages      []queue.Message `json:"messages"`
	Stream        bool            `json:"stream"`
	MaxTokens     int             `json:"max_tokens"`
	MaxCompTokens int             `json:"max_completion_tokens"`
	Temperature   *float64        `json:"temperature"`
	StreamOptions *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type apiError struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code,omitempty"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, typ, msg string) {
	var e apiError
	e.Error.Message = msg
	e.Error.Type = typ
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(e)
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

// estimateTokens: about four characters per token, the usual rule of thumb.
func estimateTokens(msgs []queue.Message) int {
	n := 0
	for _, m := range msgs {
		n += len(m.Content)/4 + 4
	}
	return n
}

// ---------------------------------------------------------------- /v1/models

func (s *Server) models(w http.ResponseWriter, r *http.Request) {
	sn := s.routes.Get()
	type model struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	out := struct {
		Object string  `json:"object"`
		Data   []model `json:"data"`
	}{Object: "list", Data: []model{}}
	if sn != nil {
		for _, m := range sn.Models {
			out.Data = append(out.Data, model{ID: m.Name, Object: "model", OwnedBy: "infergate"})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// ---------------------------------------------------------------- /status

// status is a public, read-only view of the cluster for the chat page: versions with their
// traffic weight, queue depth and live workers. No keys, prompts or request data.
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	type version struct {
		Name    string         `json:"name"`
		Weight  int            `json:"weight"`
		Queued  int64          `json:"queued"`
		Workers []queue.Worker `json:"workers"`
	}
	type model struct {
		Name     string    `json:"name"`
		Versions []version `json:"versions"`
	}
	out := struct {
		Time   int64   `json:"time"`
		Models []model `json:"models"`
	}{Time: time.Now().UnixMilli(), Models: []model{}}
	if sn := s.routes.Get(); sn != nil {
		for _, m := range sn.Models {
			mo := model{Name: m.Name}
			for _, v := range m.Versions {
				waiting, pending, _ := queue.Depth(r.Context(), s.rc, v.Name)
				ws := s.liveWorkers(r.Context(), v.Name)
				if ws == nil {
					ws = []queue.Worker{}
				}
				mo.Versions = append(mo.Versions, version{Name: v.Name, Weight: v.Weight, Queued: waiting + pending, Workers: ws})
			}
			out.Models = append(out.Models, mo)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(out)
}

// ---------------------------------------------------------------- /v1/chat/completions

func (s *Server) liveWorkers(ctx context.Context, version string) []queue.Worker {
	s.wmu.Lock()
	c, ok := s.wseen[version]
	s.wmu.Unlock()
	if ok && time.Since(c.at) < 250*time.Millisecond {
		return c.ws
	}
	ws, err := queue.LiveWorkers(ctx, s.rc, version, s.cfg.HeartbeatMaxAge)
	if err != nil {
		s.log.Warn("listing workers", "version", version, "err", err)
		return c.ws
	}
	s.wmu.Lock()
	s.wseen[version] = cachedWorkers{at: time.Now(), ws: ws}
	s.wmu.Unlock()
	return ws
}

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req chatRequest
	code := http.StatusOK
	alias := "unknown"
	defer func() { s.requests.Inc(alias, strconv.Itoa(code)) }()
	fail := func(status int, typ, msg string) {
		code = status
		writeError(w, status, typ, msg)
	}

	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&req); err != nil {
		fail(http.StatusBadRequest, "invalid_request_error", "body is not valid JSON: "+err.Error())
		return
	}
	if len(req.Messages) == 0 {
		fail(http.StatusBadRequest, "invalid_request_error", "messages must not be empty")
		return
	}
	sn := s.routes.Get()
	if sn == nil {
		fail(http.StatusServiceUnavailable, "server_error", "routes not loaded yet")
		return
	}

	// who is asking
	secret := bearer(r)
	key, known := sn.Key(secret)
	if !known {
		if s.cfg.RequireKey {
			fail(http.StatusUnauthorized, "authentication_error", "missing or unknown API key")
			return
		}
		key = registry.Key{ID: "anonymous", Name: "anonymous"}
	}

	model, ok := sn.Model(req.Model)
	if !ok {
		fail(http.StatusNotFound, "invalid_request_error", fmt.Sprintf("model %q does not exist", req.Model))
		return
	}
	alias = model.Name
	if !key.Allowed(model.Name) {
		fail(http.StatusForbidden, "permission_error", fmt.Sprintf("this key may not use %q", model.Name))
		return
	}

	maxTokens := req.MaxTokens
	if req.MaxCompTokens > 0 {
		maxTokens = req.MaxCompTokens
	}
	if maxTokens <= 0 {
		maxTokens = s.cfg.DefaultMaxTokens
	}
	temperature := 1.0
	if req.Temperature != nil {
		temperature = *req.Temperature
	}

	// rate limits: requests, then tokens (charged up front, unused part refunded at the end)
	ctx := r.Context()
	if res, err := s.limit.Take(ctx, key.ID+":rpm", key.RPM, 1); err == nil && !res.Allowed {
		s.limited.Inc(key.Name, "requests")
		w.Header().Set("Retry-After", strconv.Itoa(int(res.RetryAfter.Seconds())+1))
		fail(http.StatusTooManyRequests, "rate_limit_error", "request rate limit reached")
		return
	}
	charge := float64(estimateTokens(req.Messages) + maxTokens)
	if res, err := s.limit.Take(ctx, key.ID+":tpm", key.TPM, charge); err == nil && !res.Allowed {
		s.limited.Inc(key.Name, "tokens")
		w.Header().Set("Retry-After", strconv.Itoa(int(res.RetryAfter.Seconds())+1))
		fail(http.StatusTooManyRequests, "rate_limit_error", "token rate limit reached")
		return
	}
	used := 0
	defer func() { s.limit.Refund(context.WithoutCancel(ctx), key.ID+":tpm", key.TPM, charge-float64(used)) }()

	// which version, which worker
	version, ok := router.ChooseVersion(model.Versions, router.StickyKey(key.ID, req.Messages))
	if !ok {
		fail(http.StatusServiceUnavailable, "server_error", fmt.Sprintf("model %q has no version with traffic", model.Name))
		return
	}
	if waiting, _, err := queue.Depth(ctx, s.rc, version); err == nil && waiting >= s.cfg.MaxQueue {
		w.Header().Set("Retry-After", "5")
		fail(http.StatusServiceUnavailable, "overloaded_error", "too many requests waiting for this model, try again shortly")
		return
	}
	prefix := router.PrefixHash(req.Messages)
	var dec router.Decision
	if s.cfg.NoAffinity {
		dec = router.Choose(version, "", nil, router.Options{})
	} else {
		dec = router.Choose(version, prefix, s.liveWorkers(ctx, version), router.Options{MaxLoad: s.cfg.MaxLoad})
	}
	s.routing.Inc(version, string(dec.Kind))

	if _, ok := s.groups.Load(version); !ok {
		if err := queue.EnsureGroup(ctx, s.rc, queue.SharedStream(version)); err == nil {
			s.groups.Store(version, true)
		}
	}
	id := queue.NewID()
	deadline := time.Now().Add(s.cfg.RequestTimeout)
	if _, err := queue.Enqueue(ctx, s.rc, dec.Stream, queue.Request{
		ID: id, Model: version, Messages: req.Messages, MaxTokens: maxTokens, Temperature: temperature,
		PrefixHash: prefix, Deadline: deadline, Enqueued: start,
	}); err != nil {
		fail(http.StatusServiceUnavailable, "server_error", "queue unavailable")
		s.log.Error("enqueue", "err", err)
		return
	}
	s.inflight.Add(1, version)
	defer s.inflight.Add(-1, version)

	w.Header().Set("X-Infergate-Version", version)
	w.Header().Set("X-Infergate-Route", string(dec.Kind))

	result := s.relay(ctx, w, req, id, version, deadline, start)
	used = result.usage.TotalTokens
	code = result.code
	s.duration.Observe(time.Since(start).Seconds(), version)
	s.tokens.Add(float64(result.usage.PromptTokens), version, "prompt")
	s.tokens.Add(float64(result.usage.CompletionTokens), version, "completion")
	queue.Cleanup(context.WithoutCancel(ctx), s.rc, id)
}

type relayResult struct {
	code  int
	usage usage
}

// relay waits for the worker's events and passes them to the client, as SSE chunks or one JSON body.
func (s *Server) relay(ctx context.Context, w http.ResponseWriter, req chatRequest, id, version string, deadline, start time.Time) relayResult {
	created := start.Unix()
	var flusher http.Flusher
	if req.Stream {
		flusher, _ = w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
	}
	sent := false
	chunk := func(delta map[string]string, finish *string, u *usage) {
		type choice struct {
			Index        int               `json:"index"`
			Delta        map[string]string `json:"delta"`
			FinishReason *string           `json:"finish_reason"`
		}
		c := struct {
			ID      string   `json:"id"`
			Object  string   `json:"object"`
			Created int64    `json:"created"`
			Model   string   `json:"model"`
			Choices []choice `json:"choices"`
			Usage   *usage   `json:"usage,omitempty"`
		}{ID: id, Object: "chat.completion.chunk", Created: created, Model: version, Choices: []choice{}, Usage: u}
		if delta != nil {
			c.Choices = append(c.Choices, choice{Delta: delta, FinishReason: finish})
		}
		b, _ := json.Marshal(c)
		fmt.Fprintf(w, "data: %s\n\n", b)
		if flusher != nil {
			flusher.Flush()
		}
		sent = true
	}

	var text strings.Builder
	var u usage
	last := "0"
	first := true
	worker := ""
	for {
		if ctx.Err() != nil {
			// the client went away: stop the worker
			_ = queue.Cancel(context.WithoutCancel(ctx), s.rc, id)
			return relayResult{code: 499, usage: u}
		}
		if time.Now().After(deadline) {
			_ = queue.Cancel(context.WithoutCancel(ctx), s.rc, id)
			if req.Stream && sent {
				fmt.Fprintf(w, "data: {\"error\":{\"message\":\"timed out\",\"type\":\"timeout\"}}\n\n")
				return relayResult{code: http.StatusGatewayTimeout, usage: u}
			}
			writeError(w, http.StatusGatewayTimeout, "timeout", "no worker finished the request in time")
			return relayResult{code: http.StatusGatewayTimeout, usage: u}
		}
		events, next, err := queue.ReadEvents(ctx, s.rc, id, last, time.Second)
		if err != nil {
			if ctx.Err() != nil {
				continue
			}
			s.log.Warn("reading replies", "id", id, "err", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		last = next
		for _, e := range events {
			if e.Worker != "" && worker == "" {
				worker = e.Worker
				if !sent && !req.Stream {
					w.Header().Set("X-Infergate-Worker", worker)
				}
			}
			switch e.Kind {
			case "restart":
				// the first worker died and another one runs the request again
				if req.Stream && !first {
					// the client already has part of the old answer: can't splice, so fail cleanly
					b, _ := json.Marshal(map[string]any{"error": map[string]string{"message": "the worker restarted mid-answer; please retry", "type": "server_error"}})
					fmt.Fprintf(w, "data: %s\n\n", b)
					_ = queue.Cancel(context.WithoutCancel(ctx), s.rc, id)
					return relayResult{code: http.StatusBadGateway, usage: u}
				}
				text.Reset()
				first = true
				worker = ""
			case "token":
				if first {
					first = false
					s.ttft.Observe(time.Since(start).Seconds(), version)
					if req.Stream {
						w.Header().Set("X-Infergate-Worker", worker)
						chunk(map[string]string{"role": "assistant", "content": ""}, nil, nil)
					}
				}
				if req.Stream {
					chunk(map[string]string{"content": e.Text}, nil, nil)
				} else {
					text.WriteString(e.Text)
				}
			case "done":
				u = usage{PromptTokens: e.PromptTokens, CompletionTokens: e.OutputTokens, TotalTokens: e.PromptTokens + e.OutputTokens}
				reason := e.FinishReason
				if reason == "" {
					reason = "stop"
				}
				if req.Stream {
					if first {
						chunk(map[string]string{"role": "assistant", "content": ""}, nil, nil)
					}
					chunk(map[string]string{}, &reason, nil)
					if req.StreamOptions != nil && req.StreamOptions.IncludeUsage {
						chunk(nil, nil, &u)
					}
					fmt.Fprint(w, "data: [DONE]\n\n")
					if flusher != nil {
						flusher.Flush()
					}
					return relayResult{code: http.StatusOK, usage: u}
				}
				type message struct {
					Role    string `json:"role"`
					Content string `json:"content"`
				}
				type choice struct {
					Index        int     `json:"index"`
					Message      message `json:"message"`
					FinishReason string  `json:"finish_reason"`
				}
				body := struct {
					ID      string   `json:"id"`
					Object  string   `json:"object"`
					Created int64    `json:"created"`
					Model   string   `json:"model"`
					Choices []choice `json:"choices"`
					Usage   usage    `json:"usage"`
				}{id, "chat.completion", created, version, []choice{{Message: message{"assistant", text.String()}, FinishReason: reason}}, u}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(body)
				return relayResult{code: http.StatusOK, usage: u}
			case "error":
				msg := e.Text
				if msg == "" {
					msg = "the model worker failed"
				}
				if req.Stream && sent {
					b, _ := json.Marshal(map[string]any{"error": map[string]string{"message": msg, "type": "server_error"}})
					fmt.Fprintf(w, "data: %s\n\n", b)
					return relayResult{code: http.StatusBadGateway, usage: u}
				}
				status := http.StatusBadGateway
				if strings.Contains(msg, "deadline") || strings.Contains(msg, "timed out") {
					status = http.StatusGatewayTimeout
				}
				writeError(w, status, "server_error", msg)
				return relayResult{code: status, usage: u}
			}
		}
	}
}

// ---------------------------------------------------------------- background

// WatchQueues keeps the queue-depth and live-worker gauges fresh for /metrics.
func (s *Server) WatchQueues(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if sn := s.routes.Get(); sn != nil {
			for _, m := range sn.Models {
				for _, v := range m.Versions {
					if waiting, pending, err := queue.Depth(ctx, s.rc, v.Name); err == nil {
						s.depth.Set(float64(waiting+pending), v.Name)
					}
					if ws, err := queue.LiveWorkers(ctx, s.rc, v.Name, s.cfg.HeartbeatMaxAge); err == nil {
						s.workers.Set(float64(len(ws)), v.Name)
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

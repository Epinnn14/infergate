package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Epinnn14/infergate/internal/queue"
	"github.com/Epinnn14/infergate/internal/redis"
	"github.com/Epinnn14/infergate/internal/redistest"
	"github.com/Epinnn14/infergate/internal/registry"
	"github.com/Epinnn14/infergate/internal/router"
)

const secret = "sk-test-123"

func routes() *registry.Snapshot {
	return &registry.Snapshot{
		Revision: 1,
		Models:   []registry.Model{{Name: "chat", Versions: []router.Version{{Name: "tiny-v1", Weight: 100}}}},
		Keys:     []registry.Key{{ID: "k1", Name: "team-a", Hash: registry.HashKey(secret), RPM: 1000}},
	}
}

// fakeWorker is a minimal stand-in for the Python worker: same Redis protocol.
type fakeWorker struct {
	id        string
	model     string
	rc        *redis.Client
	words     []string
	delay     time.Duration
	restartAt int // emit a "restart" after this many tokens (0 = never), like a rescued request
	cancelled atomic.Int32
	served    atomic.Int32
}

func (f *fakeWorker) run(ctx context.Context, t *testing.T) {
	for _, s := range []string{queue.WorkerStream(f.id), queue.SharedStream(f.model)} {
		if err := queue.EnsureGroup(ctx, f.rc, s); err != nil {
			t.Error(err)
			return
		}
	}
	beat := func() {
		_, _ = f.rc.Do(ctx, "HSET", queue.WorkerHash(f.id), "inflight", 0, "capacity", 4)
		_, _ = f.rc.Do(ctx, "ZADD", queue.LiveSet(f.model), time.Now().UnixMilli(), f.id)
	}
	beat()
	for ctx.Err() == nil {
		beat()
		res, err := redis.Streams(f.rc.Do(ctx, "XREADGROUP", "GROUP", queue.Group, f.id, "COUNT", 1, "BLOCK", 200,
			"STREAMS", queue.WorkerStream(f.id), queue.SharedStream(f.model), ">", ">"))
		if err != nil {
			continue
		}
		for stream, entries := range res {
			for _, e := range entries {
				go f.serve(ctx, stream, e)
			}
		}
	}
}

func (f *fakeWorker) serve(ctx context.Context, stream string, e redis.StreamEntry) {
	reply := e.Fields["reply"]
	id := e.Fields["id"]
	if f.restartAt > 0 {
		for _, w := range f.words[:f.restartAt] {
			_, _ = f.rc.Do(ctx, "XADD", reply, "*", "type", "token", "text", w, "worker", "dead-one")
		}
		_, _ = f.rc.Do(ctx, "XADD", reply, "*", "type", "restart", "worker", f.id)
	}
	for i, w := range f.words {
		if v, _ := redis.String(f.rc.Do(ctx, "GET", queue.CancelKey(id))); v == "1" {
			f.cancelled.Add(1)
			break
		}
		text := w
		if i > 0 {
			text = " " + w
		}
		_, _ = f.rc.Do(ctx, "XADD", reply, "*", "type", "token", "text", text, "worker", f.id)
		time.Sleep(f.delay)
	}
	_, _ = f.rc.Do(ctx, "XADD", reply, "*", "type", "done", "prompt_tokens", 7, "output_tokens", len(f.words), "finish_reason", "stop", "worker", f.id)
	_, _ = f.rc.Do(ctx, "EXPIRE", reply, 60)
	_, _ = f.rc.Do(ctx, "XACK", stream, queue.Group, e.ID)
	if stream == queue.WorkerStream(f.id) {
		_, _ = f.rc.Do(ctx, "XDEL", stream, e.ID) // keeps XLEN = backlog, like the real worker
	}
	f.served.Add(1)
}

func setup(t *testing.T, cfg Config, workers ...string) (*httptest.Server, []*fakeWorker, *redis.Client) {
	rc, _ := redistest.Start(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var fws []*fakeWorker
	for _, id := range workers {
		fw := &fakeWorker{id: id, model: "tiny-v1", rc: rc, words: []string{"Halo", "dari", "infergate"}, delay: 5 * time.Millisecond}
		go fw.run(ctx, t)
		fws = append(fws, fw)
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	cfg.RequireKey = true
	srv := New(cfg, rc, registry.NewStatic(routes()), slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	time.Sleep(100 * time.Millisecond) // first heartbeat
	return ts, fws, rc
}

func post(t *testing.T, ts *httptest.Server, key string, body any) *http.Response {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", ts.URL+"/v1/chat/completions", strings.NewReader(string(b)))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

var hello = []map[string]string{{"role": "system", "content": "Kamu asisten."}, {"role": "user", "content": "Halo?"}}

func TestCompletion(t *testing.T) {
	ts, _, _ := setup(t, Config{}, "w1")
	res := post(t, ts, secret, map[string]any{"model": "chat", "messages": hello})
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status %d: %s", res.StatusCode, b)
	}
	var out struct {
		Object  string
		Model   string
		Choices []struct {
			Message      struct{ Role, Content string }
			FinishReason string `json:"finish_reason"`
		}
		Usage usage
	}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Object != "chat.completion" || out.Model != "tiny-v1" || out.Choices[0].Message.Content != "Halo dari infergate" || out.Choices[0].FinishReason != "stop" {
		t.Fatalf("unexpected body %+v", out)
	}
	if out.Usage.TotalTokens != 10 {
		t.Fatalf("usage %+v", out.Usage)
	}
	if res.Header.Get("X-Infergate-Route") != "affinity" || res.Header.Get("X-Infergate-Worker") != "w1" {
		t.Fatalf("headers %v", res.Header)
	}
}

func TestStreaming(t *testing.T) {
	ts, _, _ := setup(t, Config{}, "w1")
	res := post(t, ts, secret, map[string]any{"model": "chat", "messages": hello, "stream": true, "stream_options": map[string]bool{"include_usage": true}})
	defer res.Body.Close()
	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	var text strings.Builder
	var sawDone, sawUsage, sawFinish bool
	sc := bufio.NewScanner(res.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			sawDone = true
			break
		}
		var c struct {
			Choices []struct {
				Delta        map[string]string
				FinishReason *string `json:"finish_reason"`
			}
			Usage *usage
		}
		if err := json.Unmarshal([]byte(data), &c); err != nil {
			t.Fatal(err)
		}
		if c.Usage != nil {
			sawUsage = c.Usage.CompletionTokens == 3
		}
		for _, ch := range c.Choices {
			text.WriteString(ch.Delta["content"])
			if ch.FinishReason != nil && *ch.FinishReason == "stop" {
				sawFinish = true
			}
		}
	}
	if text.String() != "Halo dari infergate" || !sawDone || !sawUsage || !sawFinish {
		t.Fatalf("text %q done %v usage %v finish %v", text.String(), sawDone, sawUsage, sawFinish)
	}
}

func TestErrors(t *testing.T) {
	ts, _, _ := setup(t, Config{}, "w1")
	cases := []struct {
		key  string
		body any
		code int
	}{
		{"", map[string]any{"model": "chat", "messages": hello}, 401},
		{"sk-wrong", map[string]any{"model": "chat", "messages": hello}, 401},
		{secret, map[string]any{"model": "gpt-9", "messages": hello}, 404},
		{secret, map[string]any{"model": "chat", "messages": []any{}}, 400},
	}
	for _, c := range cases {
		res := post(t, ts, c.key, c.body)
		res.Body.Close()
		if res.StatusCode != c.code {
			t.Errorf("%v: got %d want %d", c, res.StatusCode, c.code)
		}
	}
}

func TestRateLimit(t *testing.T) {
	rc, _ := redistest.Start(t)
	sn := routes()
	sn.Keys[0].RPM = 2
	srv := New(Config{RequireKey: true, RequestTimeout: 300 * time.Millisecond}, rc, registry.NewStatic(sn), slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	codes := []int{}
	for i := 0; i < 3; i++ {
		res := post(t, ts, secret, map[string]any{"model": "chat", "messages": hello})
		res.Body.Close()
		codes = append(codes, res.StatusCode)
		if i == 2 && res.Header.Get("Retry-After") == "" {
			t.Error("429 needs Retry-After")
		}
	}
	// no worker: the first two time out in the queue (504), the third is refused (429)
	if codes[2] != 429 || codes[0] != 504 {
		t.Fatalf("codes %v", codes)
	}
}

func TestAffinityKeepsConversationOnOneWorker(t *testing.T) {
	ts, _, _ := setup(t, Config{}, "w1", "w2", "w3", "w4")
	for conv := 0; conv < 6; conv++ {
		// a real multi-turn chat: the history grows every turn
		msgs := []map[string]string{{"role": "user", "content": "conversation " + string(rune('A'+conv))}}
		var first string
		for turn := 0; turn < 4; turn++ {
			res := post(t, ts, secret, map[string]any{"model": "chat", "messages": msgs})
			var out struct {
				Choices []struct{ Message struct{ Content string } }
			}
			_ = json.NewDecoder(res.Body).Decode(&out)
			res.Body.Close()
			w, route := res.Header.Get("X-Infergate-Worker"), res.Header.Get("X-Infergate-Route")
			if route != "affinity" {
				t.Fatalf("conversation %d turn %d routed %q", conv, turn, route)
			}
			if turn == 0 {
				first = w
			} else if w != first {
				t.Fatalf("conversation %d moved from %s to %s at turn %d", conv, first, w, turn)
			}
			msgs = append(msgs, map[string]string{"role": "assistant", "content": out.Choices[0].Message.Content},
				map[string]string{"role": "user", "content": "and then?"})
		}
	}
}

func TestClientDisconnectCancels(t *testing.T) {
	ts, fws, _ := setup(t, Config{}, "w1")
	fws[0].words = strings.Fields(strings.Repeat("kata ", 200))
	fws[0].delay = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	b, _ := json.Marshal(map[string]any{"model": "chat", "messages": hello, "stream": true})
	req, _ := http.NewRequestWithContext(ctx, "POST", ts.URL+"/v1/chat/completions", strings.NewReader(string(b)))
	req.Header.Set("Authorization", "Bearer "+secret)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	_, _ = res.Body.Read(buf) // first tokens arrive
	cancel()
	res.Body.Close()
	deadline := time.Now().Add(3 * time.Second)
	for fws[0].cancelled.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("worker never saw the cancel")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestOverload(t *testing.T) {
	ts, _, rc := setup(t, Config{MaxQueue: 3, RequestTimeout: 200 * time.Millisecond})
	ctx := context.Background()
	if err := queue.EnsureGroup(ctx, rc, queue.SharedStream("tiny-v1")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		_, _ = rc.Do(ctx, "XADD", queue.SharedStream("tiny-v1"), "*", "id", "x")
	}
	res := post(t, ts, secret, map[string]any{"model": "chat", "messages": hello})
	res.Body.Close()
	if res.StatusCode != 503 {
		t.Fatalf("want 503 with a full queue, got %d", res.StatusCode)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	ts, _, _ := setup(t, Config{}, "w1")
	post(t, ts, secret, map[string]any{"model": "chat", "messages": hello}).Body.Close()
	res, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	for _, want := range []string{
		`infergate_requests_total{model="chat",code="200"} 1`,
		`infergate_routing_total{version="tiny-v1",kind="affinity"} 1`,
		`infergate_time_to_first_token_seconds_count{version="tiny-v1"} 1`,
	} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("missing %q\n%s", want, b)
		}
	}
}

func TestRestartAfterWorkerDied(t *testing.T) {
	ts, fws, _ := setup(t, Config{}, "w1")
	fws[0].restartAt = 2
	// non-streaming: the half answer from the dead worker is dropped, the client gets one clean answer
	res := post(t, ts, secret, map[string]any{"model": "chat", "messages": hello})
	var out struct {
		Choices []struct{ Message struct{ Content string } }
	}
	_ = json.NewDecoder(res.Body).Decode(&out)
	res.Body.Close()
	if res.StatusCode != 200 || out.Choices[0].Message.Content != "Halo dari infergate" {
		t.Fatalf("status %d content %+v", res.StatusCode, out)
	}
	// streaming: tokens already went out, so the stream ends with an error instead of repeating itself
	res = post(t, ts, secret, map[string]any{"model": "chat", "messages": hello, "stream": true})
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if !strings.Contains(string(b), "worker restarted") || strings.Contains(string(b), "[DONE]") {
		t.Fatalf("stream: %s", b)
	}
}

func TestStatusAndChatPage(t *testing.T) {
	ts, _, _ := setup(t, Config{}, "w1", "w2")
	res, err := http.Get(ts.URL + "/status") // public: no key
	if err != nil {
		t.Fatal(err)
	}
	var st struct {
		Models []struct {
			Name     string
			Versions []struct {
				Name    string
				Weight  int
				Workers []struct{ ID string }
			}
		}
	}
	if err := json.NewDecoder(res.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if len(st.Models) != 1 || st.Models[0].Versions[0].Weight != 100 || len(st.Models[0].Versions[0].Workers) != 2 {
		t.Fatalf("status = %+v", st)
	}
	b, _ := json.Marshal(st)
	if strings.Contains(string(b), "team-a") || strings.Contains(string(b), secret) {
		t.Fatal("status leaks key data")
	}

	res, err = http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || !strings.Contains(res.Header.Get("Content-Type"), "text/html") || !strings.Contains(string(page), "/v1/chat/completions") {
		t.Fatalf("chat page: %d %s", res.StatusCode, res.Header.Get("Content-Type"))
	}
	if res, _ := http.Get(ts.URL + "/nope"); res.StatusCode != 404 {
		t.Fatalf("unknown path served %d", res.StatusCode)
	}
}

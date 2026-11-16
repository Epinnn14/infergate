// Package queue is the contract between the gateway and the model workers, on Redis.
//
//	ig:q:<model>          shared request stream of a model version (consumer group "workers")
//	ig:w:<worker>         a worker's own request stream (prefix-cache affinity), same group name
//	ig:live:<model>       sorted set: worker id → last heartbeat (unix ms)
//	ig:draining:<model>   workers shutting down: not routed to, still watched for a kill mid-drain
//	ig:worker:<worker>    hash: model, inflight, capacity, cached prefixes … (refreshed each heartbeat)
//	ig:r:<request>        reply stream: token / done / error events, expires after the request
//	ig:cancel:<request>   set when the client went away; workers stop generating
//
// The same layout is implemented in Python in worker/infergate_worker/protocol.py.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Epinnn14/infergate/internal/redis"
)

const Group = "workers"

func SharedStream(model string) string  { return "ig:q:" + model }
func WorkerStream(worker string) string { return "ig:w:" + worker }
func LiveSet(model string) string       { return "ig:live:" + model }
func WorkerHash(worker string) string   { return "ig:worker:" + worker }
func ReplyStream(req string) string     { return "ig:r:" + req }
func CancelKey(req string) string       { return "ig:cancel:" + req }

// Message is one chat message (OpenAI shape).
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Request is what a worker receives.
type Request struct {
	ID          string    `json:"id"`
	Model       string    `json:"model"` // the version that serves it
	Messages    []Message `json:"messages"`
	MaxTokens   int       `json:"max_tokens"`
	Temperature float64   `json:"temperature"`
	PrefixHash  string    `json:"prefix_hash"`
	Deadline    time.Time `json:"-"`
	Enqueued    time.Time `json:"-"`
}

// Enqueue appends the request to a stream (capped so a stuck model can't grow Redis forever).
func Enqueue(ctx context.Context, rc *redis.Client, stream string, r Request) (string, error) {
	msgs, _ := json.Marshal(r.Messages)
	return redis.String(rc.Do(ctx, "XADD", stream, "MAXLEN", "~", 100000, "*",
		"id", r.ID,
		"model", r.Model,
		"messages", string(msgs),
		"max_tokens", r.MaxTokens,
		"temperature", r.Temperature,
		"prefix", r.PrefixHash,
		"reply", ReplyStream(r.ID),
		"deadline_ms", r.Deadline.UnixMilli(),
		"enqueued_ms", r.Enqueued.UnixMilli(),
	))
}

// Event is one reply event from a worker.
type Event struct {
	Kind         string // token, done, error
	Text         string
	Worker       string
	FinishReason string
	PromptTokens int
	OutputTokens int
	CacheHit     bool
}

// ReadEvents waits up to block for events after lastID on the reply stream.
func ReadEvents(ctx context.Context, rc *redis.Client, req, lastID string, block time.Duration) ([]Event, string, error) {
	res, err := redis.Streams(rc.Do(ctx, "XREAD", "COUNT", 256, "BLOCK", block, "STREAMS", ReplyStream(req), lastID))
	if errors.Is(err, redis.Nil) {
		return nil, lastID, nil
	}
	if err != nil {
		return nil, lastID, err
	}
	var out []Event
	for _, e := range res[ReplyStream(req)] {
		lastID = e.ID
		f := e.Fields
		pt, _ := strconv.Atoi(f["prompt_tokens"])
		ot, _ := strconv.Atoi(f["output_tokens"])
		out = append(out, Event{
			Kind: f["type"], Text: f["text"], Worker: f["worker"], FinishReason: f["finish_reason"],
			PromptTokens: pt, OutputTokens: ot, CacheHit: f["cache_hit"] == "1",
		})
	}
	return out, lastID, nil
}

// Cancel tells workers to stop generating for this request.
func Cancel(ctx context.Context, rc *redis.Client, req string) error {
	_, err := rc.Do(ctx, "SET", CancelKey(req), "1", "EX", 120)
	return err
}

// Cleanup drops the reply stream once the gateway has everything.
func Cleanup(ctx context.Context, rc *redis.Client, req string) {
	_, _ = rc.Do(ctx, "DEL", ReplyStream(req))
}

// Worker is a live worker as seen by the gateway.
type Worker struct {
	ID       string
	Inflight int
	Capacity int
	Backlog  int // messages waiting in its own stream
}

// Load is how busy the worker is: in-flight plus waiting, over capacity.
func (w Worker) Load() float64 {
	if w.Capacity <= 0 {
		return 1
	}
	return float64(w.Inflight+w.Backlog) / float64(w.Capacity)
}

// LiveWorkers lists workers of a model that sent a heartbeat within maxAge.
func LiveWorkers(ctx context.Context, rc *redis.Client, model string, maxAge time.Duration) ([]Worker, error) {
	min := time.Now().Add(-maxAge).UnixMilli()
	ids, err := redis.Strings(rc.Do(ctx, "ZRANGEBYSCORE", LiveSet(model), min, "+inf"))
	if err != nil {
		return nil, err
	}
	out := make([]Worker, 0, len(ids))
	for _, id := range ids {
		h, err := redis.StringMap(rc.Do(ctx, "HGETALL", WorkerHash(id)))
		if err != nil || len(h) == 0 {
			continue
		}
		inflight, _ := strconv.Atoi(h["inflight"])
		capacity, _ := strconv.Atoi(h["capacity"])
		// backlog = routed here but not picked up yet; in-flight ones are already in "inflight"
		backlog, _, _ := streamDepth(ctx, rc, WorkerStream(id))
		out = append(out, Worker{ID: id, Inflight: inflight, Capacity: capacity, Backlog: int(backlog)})
	}
	return out, nil
}

// Depth is how many requests wait for a model: unread (lag) + read but unacknowledged (pending).
func Depth(ctx context.Context, rc *redis.Client, model string) (waiting, pending int64, err error) {
	return streamDepth(ctx, rc, SharedStream(model))
}

func streamDepth(ctx context.Context, rc *redis.Client, stream string) (waiting, pending int64, err error) {
	v, err := rc.Do(ctx, "XINFO", "GROUPS", stream)
	if err != nil {
		var re redis.Error
		if errors.As(err, &re) {
			return 0, 0, nil // no stream yet
		}
		return 0, 0, err
	}
	groups, _ := v.([]any)
	for _, g := range groups {
		m, _ := redis.StringMap(g, nil)
		if m["name"] != Group {
			continue
		}
		lag, _ := strconv.ParseInt(m["lag"], 10, 64)
		pend, _ := strconv.ParseInt(m["pending"], 10, 64)
		return lag, pend, nil
	}
	return 0, 0, nil
}

// EnsureGroup creates the consumer group on a stream if it isn't there.
func EnsureGroup(ctx context.Context, rc *redis.Client, stream string) error {
	_, err := rc.Do(ctx, "XGROUP", "CREATE", stream, Group, "0", "MKSTREAM")
	var re redis.Error
	if errors.As(err, &re) && len(re) >= 9 && string(re[:9]) == "BUSYGROUP" {
		return nil
	}
	return err
}

// NewID makes a request id like OpenAI's: chatcmpl-<time><random>.
func NewID() string {
	return fmt.Sprintf("chatcmpl-%x%06x", time.Now().UnixNano(), randUint()&0xffffff)
}

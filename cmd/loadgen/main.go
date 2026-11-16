// Command loadgen drives an OpenAI-compatible endpoint with multi-turn chat traffic and
// reports time to first token, latency, throughput and how requests were routed.
//
// Traffic shape: -conversations distinct conversations, each with its own long system
// prompt (like many apps or users with their own instructions). Every request picks a
// conversation at random and asks a short new question, so a worker that has the
// conversation's prefix cached can skip most of the prefill.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type sample struct {
	ok      bool
	code    int
	ttft    time.Duration
	total   time.Duration
	tokens  int
	route   string
	worker  string
	version string
}

func main() {
	url := flag.String("url", "http://127.0.0.1:8080", "gateway base URL")
	key := flag.String("key", os.Getenv("IG_API_KEY"), "API key")
	model := flag.String("model", "chat", "model alias")
	conc := flag.Int("c", 16, "concurrent clients")
	dur := flag.Duration("d", 30*time.Second, "how long to run")
	convs := flag.Int("conversations", 64, "distinct conversations (system prompts)")
	sysLen := flag.Int("system-chars", 6000, "length of each conversation's system prompt")
	maxTok := flag.Int("max-tokens", 64, "max_tokens per request")
	stream := flag.Bool("stream", true, "use streaming")
	jsonOut := flag.String("json", "", "write the summary as JSON to this file")
	label := flag.String("label", "", "label for the summary")
	flag.Parse()

	systems := make([]string, *convs)
	for i := range systems {
		var sb strings.Builder
		fmt.Fprintf(&sb, "You are assistant #%d. ", i)
		for sb.Len() < *sysLen {
			fmt.Fprintf(&sb, "Rule %d: be concise and accurate. ", i)
		}
		systems[i] = sb.String()[:*sysLen]
	}

	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: *conc * 2}, Timeout: 5 * time.Minute}
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var mu sync.Mutex
	var samples []sample
	var sent atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for c := 0; c < *conc; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				conv := rand.IntN(*convs)
				s := one(client, *url, *key, *model, systems[conv], fmt.Sprintf("Question %d about topic %d?", sent.Add(1), conv), *maxTok, *stream)
				if ctx.Err() != nil && !s.ok {
					return // cut off by the end of the run
				}
				mu.Lock()
				samples = append(samples, s)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	sum := summarize(samples, elapsed)
	sum["label"] = *label
	sum["concurrency"] = *conc
	sum["conversations"] = *convs
	printSummary(sum)
	if *jsonOut != "" {
		b, _ := json.MarshalIndent(sum, "", "  ")
		_ = os.WriteFile(*jsonOut, b, 0o644)
	}
}

func one(client *http.Client, url, key, model, system, question string, maxTok int, stream bool) sample {
	body, _ := json.Marshal(map[string]any{
		"model":          model,
		"messages":       []map[string]string{{"role": "system", "content": system}, {"role": "user", "content": question}},
		"max_tokens":     maxTok,
		"stream":         stream,
		"stream_options": map[string]bool{"include_usage": true},
	})
	req, _ := http.NewRequest("POST", url+"/v1/chat/completions", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	t0 := time.Now()
	res, err := client.Do(req)
	if err != nil {
		return sample{code: -1, total: time.Since(t0)}
	}
	defer res.Body.Close()
	s := sample{code: res.StatusCode, route: res.Header.Get("X-Infergate-Route"), worker: res.Header.Get("X-Infergate-Worker"), version: res.Header.Get("X-Infergate-Version")}
	if res.StatusCode != 200 {
		s.total = time.Since(t0)
		return s
	}
	if !stream {
		var out struct {
			Usage struct {
				CompletionTokens int `json:"completion_tokens"`
			}
		}
		_ = json.NewDecoder(res.Body).Decode(&out)
		s.total = time.Since(t0)
		s.ttft = s.total
		s.tokens = out.Usage.CompletionTokens
		s.ok = true
		return s
	}
	sc := bufio.NewScanner(res.Body)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := line[6:]
		if data == "[DONE]" {
			s.ok = true
			break
		}
		var c struct {
			Choices []struct{ Delta map[string]string }
			Usage   *struct {
				CompletionTokens int `json:"completion_tokens"`
			}
			Error any
		}
		if json.Unmarshal([]byte(data), &c) != nil || c.Error != nil {
			break
		}
		if s.ttft == 0 {
			for _, ch := range c.Choices {
				if ch.Delta["content"] != "" {
					s.ttft = time.Since(t0)
				}
			}
		}
		if c.Usage != nil {
			s.tokens = c.Usage.CompletionTokens
		}
	}
	s.total = time.Since(t0)
	return s
}

func pct(ds []time.Duration, p float64) float64 {
	if len(ds) == 0 {
		return 0
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	i := int(p * float64(len(ds)-1))
	return float64(ds[i].Microseconds()) / 1000
}

func summarize(ss []sample, elapsed time.Duration) map[string]any {
	var ttft, total []time.Duration
	codes := map[string]int{}
	routes := map[string]int{}
	workers := map[string]int{}
	versions := map[string]int{}
	tokens, ok := 0, 0
	for _, s := range ss {
		codes[fmt.Sprint(s.code)]++
		if !s.ok {
			continue
		}
		ok++
		ttft = append(ttft, s.ttft)
		total = append(total, s.total)
		tokens += s.tokens
		routes[s.route]++
		workers[s.worker]++
		versions[s.version]++
	}
	secs := elapsed.Seconds()
	return map[string]any{
		"requests": len(ss), "ok": ok, "codes": codes, "seconds": round(secs),
		"requests_per_s": round(float64(ok) / secs), "tokens_per_s": round(float64(tokens) / secs),
		"ttft_ms":    map[string]float64{"p50": pct(ttft, 0.5), "p90": pct(ttft, 0.9), "p99": pct(ttft, 0.99)},
		"latency_ms": map[string]float64{"p50": pct(total, 0.5), "p90": pct(total, 0.9), "p99": pct(total, 0.99)},
		"routes":     routes, "workers": workers, "versions": versions,
	}
}

func round(f float64) float64 { return float64(int(f*10+0.5)) / 10 }

func printSummary(s map[string]any) {
	t := s["ttft_ms"].(map[string]float64)
	l := s["latency_ms"].(map[string]float64)
	fmt.Printf("%s\n  requests %v ok %v in %vs  codes %v\n  throughput %v req/s, %v tokens/s\n  TTFT ms    p50 %.0f  p90 %.0f  p99 %.0f\n  latency ms p50 %.0f  p90 %.0f  p99 %.0f\n  routes %v\n  per worker %v\n  per version %v\n",
		s["label"], s["requests"], s["ok"], s["seconds"], s["codes"], s["requests_per_s"], s["tokens_per_s"],
		t["p50"], t["p90"], t["p99"], l["p50"], l["p90"], l["p99"], s["routes"], s["workers"], s["versions"])
}

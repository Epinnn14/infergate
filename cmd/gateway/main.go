// Command gateway is infergate's API gateway: OpenAI-compatible HTTP in, Redis queues out.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Epinnn14/infergate/internal/api"
	"github.com/Epinnn14/infergate/internal/redis"
	"github.com/Epinnn14/infergate/internal/registry"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return d
	}
	return def
}

func envNum(k string, def float64) float64 {
	if f, err := strconv.ParseFloat(os.Getenv(k), 64); err == nil {
		return f
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, _ := strconv.Atoi(env("IG_REDIS_DB", "0"))
	rc := redis.New(redis.Options{
		Addr: env("IG_REDIS_ADDR", "127.0.0.1:6379"), Password: os.Getenv("IG_REDIS_PASSWORD"), DB: db,
		MaxConns: int(envNum("IG_REDIS_MAX_CONNS", 512)),
	})
	defer rc.Close()

	// routes: from the registry service, or a JSON file for running without it
	var routes *registry.Source
	if f := os.Getenv("IG_ROUTES_FILE"); f != "" {
		var err error
		if routes, err = registry.FromFile(f); err != nil {
			log.Error("routes file", "err", err)
			os.Exit(1)
		}
	} else {
		routes = &registry.Source{}
		go registry.Poller{
			URL: env("IG_REGISTRY_URL", "http://127.0.0.1:8090"), Token: os.Getenv("IG_REGISTRY_TOKEN"),
			Interval: envDur("IG_REGISTRY_INTERVAL", 5*time.Second), Log: log,
		}.Run(ctx, routes)
	}

	srv := api.New(api.Config{
		RequireKey:       env("IG_REQUIRE_KEY", "true") != "false",
		RequestTimeout:   envDur("IG_REQUEST_TIMEOUT", 2*time.Minute),
		MaxQueue:         int64(envNum("IG_MAX_QUEUE", 1000)),
		MaxLoad:          envNum("IG_MAX_LOAD", 1),
		HeartbeatMaxAge:  envDur("IG_HEARTBEAT_MAX_AGE", 5*time.Second),
		DefaultMaxTokens: int(envNum("IG_DEFAULT_MAX_TOKENS", 256)),
		NoAffinity:       env("IG_AFFINITY", "true") == "false",
	}, rc, routes, log)
	go srv.WatchQueues(ctx, 2*time.Second)

	hs := &http.Server{
		Addr:              env("IG_LISTEN", ":8080"),
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	go func() {
		log.Info("gateway listening", "addr", hs.Addr)
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("listen", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	// stop taking new connections, let in-flight streams finish (Kubernetes gives terminationGracePeriodSeconds)
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), envDur("IG_SHUTDOWN_GRACE", 25*time.Second))
	defer cancel()
	_ = hs.Shutdown(sctx)
}

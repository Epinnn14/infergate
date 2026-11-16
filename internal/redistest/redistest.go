// Package redistest gives tests a real Redis: $REDIS_ADDR if set (CI service container),
// otherwise a throwaway redis-server on a free port. Tests skip when neither is available.
package redistest

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/Epinnn14/infergate/internal/redis"
)

// Start returns a client on an empty database.
func Start(t testing.TB) (*redis.Client, string) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		bin, err := exec.LookPath("redis-server")
		if err != nil {
			t.Skip("no REDIS_ADDR and no redis-server on PATH")
		}
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := l.Addr().(*net.TCPAddr).Port
		l.Close()
		cmd := exec.Command(bin, "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		addr = "127.0.0.1:" + strconv.Itoa(port)
	}
	rc := redis.New(redis.Options{Addr: addr, MaxConns: 128})
	t.Cleanup(rc.Close)
	ctx := context.Background()
	for i := 0; ; i++ {
		if _, err := rc.Do(ctx, "PING"); err == nil {
			break
		} else if i > 100 {
			t.Fatalf("redis at %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := rc.Do(ctx, "FLUSHDB"); err != nil {
		t.Fatal(err)
	}
	return rc, addr
}

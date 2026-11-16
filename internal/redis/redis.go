// Package redis is a small Redis client: RESP2 over TCP with a connection pool.
//
// It covers what the gateway needs (streams, sorted sets, hashes, Lua scripts) without
// third-party dependencies. Blocking commands (XREAD BLOCK) hold a pooled connection for
// their duration, so the pool grows to the number of in-flight requests up to MaxConns.
package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// Nil is returned for a nil reply (missing key, XREAD timeout).
var Nil = errors.New("redis: nil")

// Error is an error reply from the server ("-ERR ...", "-NOGROUP ...").
type Error string

func (e Error) Error() string { return string(e) }

// Options configures a Client.
type Options struct {
	Addr        string
	Password    string
	DB          int
	MaxConns    int
	DialTimeout time.Duration
}

// Client is safe for concurrent use.
type Client struct {
	opt  Options
	idle chan *conn
	sem  chan struct{}
}

type conn struct {
	c  net.Conn
	r  *bufio.Reader
	w  *bufio.Writer
	ok bool
}

// New returns a client; connections are opened lazily.
func New(opt Options) *Client {
	if opt.MaxConns <= 0 {
		opt.MaxConns = 64
	}
	if opt.DialTimeout <= 0 {
		opt.DialTimeout = 3 * time.Second
	}
	return &Client{opt: opt, idle: make(chan *conn, opt.MaxConns), sem: make(chan struct{}, opt.MaxConns)}
}

func (cl *Client) dial(ctx context.Context) (*conn, error) {
	d := net.Dialer{Timeout: cl.opt.DialTimeout}
	nc, err := d.DialContext(ctx, "tcp", cl.opt.Addr)
	if err != nil {
		return nil, err
	}
	c := &conn{c: nc, r: bufio.NewReaderSize(nc, 32<<10), w: bufio.NewWriterSize(nc, 32<<10), ok: true}
	if cl.opt.Password != "" {
		if _, err := c.do(ctx, []any{"AUTH", cl.opt.Password}); err != nil {
			nc.Close()
			return nil, err
		}
	}
	if cl.opt.DB != 0 {
		if _, err := c.do(ctx, []any{"SELECT", cl.opt.DB}); err != nil {
			nc.Close()
			return nil, err
		}
	}
	return c, nil
}

func (cl *Client) get(ctx context.Context) (*conn, error) {
	select {
	case cl.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case c := <-cl.idle:
		return c, nil
	default:
	}
	c, err := cl.dial(ctx)
	if err != nil {
		<-cl.sem
		return nil, err
	}
	return c, nil
}

func (cl *Client) put(c *conn) {
	if c.ok {
		select {
		case cl.idle <- c:
		default:
			c.c.Close()
		}
	} else {
		c.c.Close()
	}
	<-cl.sem
}

// Close closes idle connections.
func (cl *Client) Close() {
	for {
		select {
		case c := <-cl.idle:
			c.c.Close()
		default:
			return
		}
	}
}

// Do sends one command and returns its reply: string, int64, []any, nil (with Nil), or an Error.
func (cl *Client) Do(ctx context.Context, args ...any) (any, error) {
	c, err := cl.get(ctx)
	if err != nil {
		return nil, err
	}
	defer cl.put(c)
	return c.do(ctx, args)
}

func (c *conn) do(ctx context.Context, args []any) (any, error) {
	if dl, ok := ctx.Deadline(); ok {
		_ = c.c.SetDeadline(dl)
	} else {
		_ = c.c.SetDeadline(time.Time{})
	}
	// a cancelled context interrupts a blocking read
	stop := context.AfterFunc(ctx, func() { _ = c.c.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	if err := writeCommand(c.w, args); err != nil {
		c.ok = false
		return nil, err
	}
	if err := c.w.Flush(); err != nil {
		c.ok = false
		return nil, err
	}
	v, err := readReply(c.r)
	if err != nil {
		var re Error
		if !errors.As(err, &re) && !errors.Is(err, Nil) {
			c.ok = false // broken stream: don't reuse
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		return nil, err
	}
	return v, nil
}

func writeCommand(w *bufio.Writer, args []any) error {
	fmt.Fprintf(w, "*%d\r\n", len(args))
	for _, a := range args {
		var s string
		switch v := a.(type) {
		case string:
			s = v
		case []byte:
			s = string(v)
		case int:
			s = strconv.Itoa(v)
		case int64:
			s = strconv.FormatInt(v, 10)
		case float64:
			s = strconv.FormatFloat(v, 'f', -1, 64)
		case time.Duration:
			s = strconv.FormatInt(v.Milliseconds(), 10)
		default:
			s = fmt.Sprint(v)
		}
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
	}
	return nil
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return "", fmt.Errorf("redis: bad line %q", line)
	}
	return line[:len(line)-2], nil
}

func readReply(r *bufio.Reader) (any, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, Error(line[1:])
	case ':':
		return strconv.ParseInt(line[1:], 10, 64)
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, Nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, Nil
		}
		out := make([]any, n)
		for i := range out {
			v, err := readReply(r)
			if err != nil && !errors.Is(err, Nil) {
				var re Error
				if !errors.As(err, &re) {
					return nil, err
				}
				v = err
			}
			out[i] = v
		}
		return out, nil
	}
	return nil, fmt.Errorf("redis: unknown reply %q", line)
}

// ---------------------------------------------------------------- typed helpers

// String reads a reply as a string.
func String(v any, err error) (string, error) {
	if err != nil {
		return "", err
	}
	switch s := v.(type) {
	case string:
		return s, nil
	case int64:
		return strconv.FormatInt(s, 10), nil
	}
	return "", fmt.Errorf("redis: want string, got %T", v)
}

// Int reads a reply as an integer.
func Int(v any, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	switch n := v.(type) {
	case int64:
		return n, nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	}
	return 0, fmt.Errorf("redis: want int, got %T", v)
}

// Strings reads an array reply of strings (nil entries become "").
func Strings(v any, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("redis: want array, got %T", v)
	}
	out := make([]string, len(arr))
	for i, x := range arr {
		switch s := x.(type) {
		case string:
			out[i] = s
		case int64:
			out[i] = strconv.FormatInt(s, 10)
		}
	}
	return out, nil
}

// StringMap reads a flat [k1 v1 k2 v2 ...] reply (HGETALL, XINFO) into a map.
func StringMap(v any, err error) (map[string]string, error) {
	ss, err := Strings(v, err)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, len(ss)/2)
	for i := 0; i+1 < len(ss); i += 2 {
		m[ss[i]] = ss[i+1]
	}
	return m, nil
}

// StreamEntry is one stream message.
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

// Streams parses an XREAD / XREADGROUP reply: stream name → entries.
func Streams(v any, err error) (map[string][]StreamEntry, error) {
	if err != nil {
		return nil, err
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("redis: want array, got %T", v)
	}
	out := map[string][]StreamEntry{}
	for _, s := range arr {
		pair, ok := s.([]any)
		if !ok || len(pair) != 2 {
			continue
		}
		name, _ := pair[0].(string)
		entries, _ := pair[1].([]any)
		for _, e := range entries {
			ep, ok := e.([]any)
			if !ok || len(ep) != 2 {
				continue
			}
			id, _ := ep[0].(string)
			fields, _ := StringMap(ep[1], nil)
			out[name] = append(out[name], StreamEntry{ID: id, Fields: fields})
		}
	}
	return out, nil
}

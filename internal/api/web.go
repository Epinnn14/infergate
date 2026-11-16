package api

import (
	_ "embed"
	"net/http"
)

//go:embed web/index.html
var indexHTML []byte

// chatPage serves the built-in chat and cluster page at /. It is a single static file that
// talks to the same gateway: /v1/chat/completions with the user's key, and the public /status.
func chatPage() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		h := w.Header()
		h.Set("Content-Type", "text/html; charset=utf-8")
		h.Set("Cache-Control", "no-cache")
		h.Set("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data:")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		_, _ = w.Write(indexHTML)
	})
}

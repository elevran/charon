package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Server wraps net/http.Server.
type Server struct {
	srv *http.Server
}

// NewServer builds a Server using the default prometheus registry.
func NewServer(addr string, h *Handler, log *slog.Logger) *Server {
	return NewServerWithRegistry(addr, h, log, prometheus.DefaultGatherer)
}

// RegisterHandlers registers the response API routes on mux.
// If mux is nil, http.DefaultServeMux is used.
func RegisterHandlers(mux *http.ServeMux, h *Handler) {
	if mux == nil {
		mux = http.DefaultServeMux
	}
	mux.HandleFunc("GET /healthz", h.HandleHealthz)
	mux.HandleFunc("GET /readyz", h.HandleReadyz)
	mux.HandleFunc("GET /responses/{id}/context", h.HandleResolve)
	mux.HandleFunc("POST /responses/{id}", h.HandleStore)
	mux.HandleFunc("PATCH /responses/{id}", h.HandleAppendChunk)
	mux.HandleFunc("GET /responses/{id}", h.HandleRetrieve)
	mux.HandleFunc("DELETE /responses/{id}", h.HandleDelete)
}

// WrapH2c wraps a handler with H2c support (HTTP/2 cleartext).
// Use this when creating an httptest.NewServer for the Charon internal API
// so that tests match the production server behaviour; the charon.Client
// uses an H2c transport by default.
func WrapH2c(handler http.Handler) http.Handler {
	return h2c.NewHandler(handler, &http2.Server{})
}

// newServer is the shared constructor. It wraps handler in the middleware
// stack and in an h2c.Handler so that the Charon internal API and proxy
// both accept HTTP/2 cleartext (H2c) in addition to HTTP/1.1.
//
// H2c allows the proxy to multiplex concurrent PATCH /responses/{id} chunk
// requests over a single TCP connection without head-of-line blocking.
// Go's net/http.Client automatically upgrades to H2 when the server supports
// it — no API surface change required.
func newServer(addr string, handler http.Handler) *Server {
	h2s := &http2.Server{}
	return &Server{srv: &http.Server{
		Addr:         addr,
		Handler:      h2c.NewHandler(handler, h2s),
		ReadTimeout:  35 * time.Second,
		WriteTimeout: 35 * time.Second,
		IdleTimeout:  120 * time.Second,
	}}
}

// NewServerFromMux builds a Server wrapping a pre-configured mux with the
// standard middleware stack (recovery, request logging, timeout) and H2c.
func NewServerFromMux(addr string, mux *http.ServeMux, log *slog.Logger) *Server {
	handler := Chain(mux, Recovery(log), RequestLogger(log), Timeout(30*time.Second))
	return newServer(addr, handler)
}

// NewServerWithRegistry builds a Server with a custom prometheus Gatherer for
// /metrics and H2c support.
func NewServerWithRegistry(addr string, h *Handler, log *slog.Logger, reg prometheus.Gatherer) *Server {
	mux := http.NewServeMux()
	RegisterHandlers(mux, h)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	handler := Chain(mux,
		Recovery(log),
		RequestLogger(log),
		Timeout(30*time.Second),
	)
	return newServer(addr, handler)
}

// Start begins listening. Returns when the server stops.
func (s *Server) Start() error {
	return s.srv.ListenAndServe()
}

// Shutdown initiates a graceful drain with the provided context.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

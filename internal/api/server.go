package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/trace"
)

// Server wraps net/http.Server.
type Server struct {
	srv *http.Server
}

// NewServer builds a Server using the default prometheus registry, no tracing.
func NewServer(addr string, h *Handler, log *slog.Logger) *Server {
	return NewServerWithRegistry(addr, h, log, prometheus.DefaultGatherer, nil)
}

// RegisterHandlers registers the response API routes on mux.
// If mux is nil, http.DefaultServeMux is used.
func RegisterHandlers(mux *http.ServeMux, h *Handler) {
	if mux == nil {
		mux = http.DefaultServeMux
	}
	mux.HandleFunc("GET /healthz", h.HandleHealthz)
	mux.HandleFunc("GET /readyz", h.HandleReadyz)
	mux.HandleFunc("POST /responses", h.HandleResolve)
	mux.HandleFunc("POST /responses/staging", h.HandleOpenStaging)
	mux.HandleFunc("POST /responses/{id}", h.HandleStore)
	mux.HandleFunc("PUT /responses/staging/{id}/chunks/{k}", h.HandleAppendChunk)
	mux.HandleFunc("PUT /responses/staging/{id}/complete", h.HandleComplete)
	mux.HandleFunc("PUT /responses/staging/{id}/abort", h.HandleAbort)
	mux.HandleFunc("GET /responses/staging/{id}", h.HandleStagingStatus)
	mux.HandleFunc("GET /responses/by-response-id/{rid}", h.HandleByResponseID)
	mux.HandleFunc("GET /responses/{id}", h.HandleRetrieve)
	mux.HandleFunc("DELETE /responses/{id}", h.HandleDelete)
}

// newServer is the shared constructor.
func newServer(addr string, handler http.Handler) *Server {
	return &Server{srv: &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second, // bound slow-header goroutine leaks
		ReadTimeout:       35 * time.Second,
		WriteTimeout:      35 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16, // 64 KB; internal API headers are small
	}}
}

// NewServerFromMux builds a Server wrapping a pre-configured mux with the
// standard middleware stack (recovery, request logging, timeout, optional tracing).
// tp may be nil — tracing is skipped when disabled.
func NewServerFromMux(addr string, mux *http.ServeMux, log *slog.Logger, tp trace.TracerProvider) *Server {
	handler := Chain(mux,
		Tracing("proxy", tp),
		Recovery(log),
		RequestLogger(log),
		Timeout(30*time.Second),
	)
	return newServer(addr, handler)
}

// NewServerWithRegistry builds a Server with a custom prometheus Gatherer for /metrics.
// tp may be nil — tracing is skipped when disabled.
func NewServerWithRegistry(addr string, h *Handler, log *slog.Logger, reg prometheus.Gatherer, tp trace.TracerProvider) *Server {
	mux := http.NewServeMux()
	RegisterHandlers(mux, h)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	handler := Chain(mux,
		Tracing("charon", tp),
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

package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	mux.HandleFunc("GET /responses/{id}/context", h.HandleResolve)
	mux.HandleFunc("POST /responses/{id}", h.HandleStore)
	mux.HandleFunc("GET /responses/{id}", h.HandleRetrieve)
	mux.HandleFunc("DELETE /responses/{id}", h.HandleDelete)
}

// NewServerFromMux builds a Server wrapping a pre-configured mux with the
// standard middleware stack (recovery, request logging, timeout).
func NewServerFromMux(addr string, mux *http.ServeMux, log *slog.Logger) *Server {
	handler := Chain(mux, Recovery(log), RequestLogger(log), Timeout(30*time.Second))
	return &Server{srv: &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  35 * time.Second,
		WriteTimeout: 35 * time.Second,
		IdleTimeout:  120 * time.Second,
	}}
}

// NewServerWithRegistry builds a Server with a custom prometheus Gatherer for /metrics.
func NewServerWithRegistry(addr string, h *Handler, log *slog.Logger, reg prometheus.Gatherer) *Server {
	mux := http.NewServeMux()
	RegisterHandlers(mux, h)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	handler := Chain(mux,
		Recovery(log),
		RequestLogger(log),
		Timeout(30*time.Second),
	)

	return &Server{
		srv: &http.Server{
			Addr:         addr,
			Handler:      handler,
			ReadTimeout:  35 * time.Second,
			WriteTimeout: 35 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
	}
}

// Start begins listening. Returns when the server stops.
func (s *Server) Start() error {
	return s.srv.ListenAndServe()
}

// Shutdown initiates a graceful drain with the provided context.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

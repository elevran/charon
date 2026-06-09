package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/elevran/charon/internal/metrics"
)

// Middleware is a function that wraps an http.Handler.
type Middleware func(http.Handler) http.Handler

// Chain applies middlewares in order (first middleware is outermost).
func Chain(h http.Handler, mw ...Middleware) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}

type statusRecorder struct {
	http.ResponseWriter
	status int // 0 means WriteHeader has not been called (treat as 200)
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) effectiveStatus() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

// Recovery catches panics, logs them, and returns 500 if headers have not been sent.
func Recovery(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rec := &statusRecorder{ResponseWriter: w}
			defer func() {
				if v := recover(); v != nil {
					log.Error("panic recovered", "panic", fmt.Sprintf("%v", v))
					if rec.status == 0 {
						http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError) //nolint:gocritic // returnAfterHttpError: inside defer recovery, no further handler executes
					}
				}
			}()
			next.ServeHTTP(rec, r)
		})
	}
}

// RequestLogger logs method, path, status, and duration at INFO level,
// and records Prometheus metrics.
func RequestLogger(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r)
			dur := time.Since(start)
			status := rec.effectiveStatus()
			log.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", status,
				"duration_ms", dur.Milliseconds(),
			)
			endpoint := r.Method + " " + r.Pattern
			metrics.HTTPRequestsTotal.WithLabelValues(endpoint, strconv.Itoa(status)).Inc()
			metrics.HTTPRequestDuration.WithLabelValues(endpoint).Observe(dur.Seconds())
		})
	}
}

// Timeout wraps the handler with http.TimeoutHandler (stdlib built-in).
func Timeout(d time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		return http.TimeoutHandler(next, d, `{"error":"timeout"}`)
	}
}

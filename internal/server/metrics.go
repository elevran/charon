package server

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "http_requests_total"},
		[]string{"endpoint", "status"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "http_request_duration_seconds"},
		[]string{"endpoint"},
	)
)

// RegisterMetrics registers HTTP metrics into reg under the given namespace prefix.
// reg nil uses prometheus.DefaultRegisterer; empty namespace defaults to "responses".
// Returns an error if registration fails for any reason other than already-registered.
func RegisterMetrics(reg prometheus.Registerer, namespace string) error {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	if namespace == "" {
		namespace = "responses"
	}
	wrapped := prometheus.WrapRegistererWithPrefix(namespace+"_", reg)
	for _, c := range []prometheus.Collector{
		requestsTotal,
		requestDuration,
	} {
		if err := wrapped.Register(c); err != nil {
			var are prometheus.AlreadyRegisteredError
			if !errors.As(err, &are) {
				return err
			}
		}
	}
	return nil
}

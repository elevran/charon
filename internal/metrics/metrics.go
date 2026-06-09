package metrics

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "http_requests_total"},
		[]string{"endpoint", "status"},
	)
	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "http_request_duration_seconds"},
		[]string{"endpoint"},
	)
	WriteIntentFailuresTotal = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "write_intent_failures_total"},
	)
	ChainDepthAtResolve = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "chain_depth_at_resolve",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		},
	)
	ActiveWriteIntents = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "active_write_intents"},
	)

	// Phase 5: extended operational metrics.

	CheckpointWritesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "checkpoint_writes_total"},
	)
	CheckpointSizeBytes = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "checkpoint_size_bytes",
			Buckets: prometheus.ExponentialBuckets(1024, 4, 8), // 1 KB → 16 MB
		},
	)
	TTLExpirationsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "ttl_expirations_total"},
	)
	WorkerSweepDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "worker_sweep_duration_seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"worker"}, // "cleaner" or "reconciler"
	)
)

// Register registers all metrics into reg under the given namespace prefix.
// reg nil uses prometheus.DefaultRegisterer; empty namespace defaults to "responses".
// Returns an error if registration fails for any reason other than already-registered.
func Register(reg prometheus.Registerer, namespace string) error {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	if namespace == "" {
		namespace = "responses"
	}
	wrapped := prometheus.WrapRegistererWithPrefix(namespace+"_", reg)
	for _, c := range []prometheus.Collector{
		HTTPRequestsTotal,
		HTTPRequestDuration,
		WriteIntentFailuresTotal,
		ChainDepthAtResolve,
		ActiveWriteIntents,
		CheckpointWritesTotal,
		CheckpointSizeBytes,
		TTLExpirationsTotal,
		WorkerSweepDuration,
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

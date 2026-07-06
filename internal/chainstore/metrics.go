package chainstore

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// storeMetrics holds the Prometheus collectors for a Store.
// All fields are nil when the Store was opened without a Registerer.
type storeMetrics struct {
	reconstructLatency  *prometheus.HistogramVec
	chainDepth          prometheus.Histogram
	evictionsTotal      prometheus.Counter
	ttlExpirationsTotal prometheus.Counter
	stagingReapedTotal  prometheus.Counter
	stagingReapErrTotal prometheus.Counter
	entries             prometheus.Gauge
	bytes               prometheus.Gauge
	cacheHitsTotal      prometheus.Counter
	cacheMissesTotal    prometheus.Counter
	cacheEvictionsTotal prometheus.Counter
	cacheBytes          prometheus.Gauge
}

// newStoreMetrics creates and registers chainstore metrics under reg.
// Returns nil (no-op) when reg is nil.
func newStoreMetrics(reg prometheus.Registerer) (*storeMetrics, error) {
	if reg == nil {
		return nil, nil
	}
	m := &storeMetrics{
		reconstructLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "chainstore_reconstruct_duration_seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"status"}),
		chainDepth: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chainstore_chain_depth",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		}),
		evictionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chainstore_evictions_total",
		}),
		ttlExpirationsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chainstore_ttl_expirations_total",
		}),
		stagingReapedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chainstore_staging_reaped_total",
		}),
		stagingReapErrTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chainstore_staging_reap_errors_total",
		}),
		entries: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chainstore_entries_total",
		}),
		bytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chainstore_bytes_total",
		}),
		cacheHitsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chainstore_cache_hits_total",
		}),
		cacheMissesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chainstore_cache_misses_total",
		}),
		cacheEvictionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chainstore_cache_evictions_total",
		}),
		cacheBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chainstore_cache_bytes",
		}),
	}
	for _, c := range []prometheus.Collector{
		m.reconstructLatency,
		m.chainDepth,
		m.evictionsTotal,
		m.ttlExpirationsTotal,
		m.stagingReapedTotal,
		m.stagingReapErrTotal,
		m.entries,
		m.bytes,
		m.cacheHitsTotal,
		m.cacheMissesTotal,
		m.cacheEvictionsTotal,
		m.cacheBytes,
	} {
		if err := reg.Register(c); err != nil {
			var are prometheus.AlreadyRegisteredError
			if !errors.As(err, &are) {
				return nil, err
			}
		}
	}
	return m, nil
}

package chainstore_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/chainstore"
)

// TestMetrics_NamesRegistered verifies that opening a Store with a Registerer
// publishes exactly the metric names specified in the Phase 5 plan (plus the
// chainstore_* metrics introduced in earlier phases).
func TestMetrics_NamesRegistered(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	s := openMemStore(t, chainstore.Config{Registerer: reg})

	// Force at least one observation of the histogram so it surfaces in Gather.
	// A HistogramVec with labels has no series until WithLabelValues is called.
	require.NoError(t, s.Store(ctx, "r0", "", "", []byte("hi")))
	_, err := s.Resolve(ctx, "r0", "")
	require.NoError(t, err)

	mfs, err := reg.Gather()
	require.NoError(t, err)

	got := map[string]bool{}
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}

	// Phase 5 plan specifies these metric names.
	required := []string{
		"chainstore_entries_total",
		"chainstore_bytes_total",
		"chainstore_evictions_total",
		"chainstore_reconstruct_duration_seconds",
	}
	for _, name := range required {
		assert.True(t, got[name], "metric %q must be registered; got %v", name, keys(got))
	}

	// Guard against accidental rename back to the pre-Phase-5 names.
	for _, old := range []string{"chainstore_entries", "chainstore_bytes"} {
		assert.False(t, got[old], "metric %q should have been renamed in Phase 5", old)
	}
}

// TestMetrics_RoundTrip exercises the four metrics listed in the Phase 5 plan
// across a store + reconstruct + evict cycle. After the cycle, each metric
// must be non-zero (entries/bytes are gauges; evictions_total is a counter;
// reconstruct_duration_seconds is a histogram with at least one sample).
func TestMetrics_RoundTrip(t *testing.T) {
	ctx := context.Background()

	reg := prometheus.NewRegistry()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	cfg := chainstore.Config{
		Clock:            clk,
		MaxEntries:       1,              // forces eviction after the second Store
		EvictionInterval: 24 * time.Hour, // disable auto-tick; we nudge manually
		BucketDuration:   time.Hour,
		Registerer:       reg,
	}
	s := openMemStore(t, cfg)

	// Store + reconstruct cycle.
	require.NoError(t, s.Store(ctx, "r0", "", "", []byte("hello")))
	_, err := s.Resolve(ctx, "r0", "")
	require.NoError(t, err)

	// Gauges: entries/bytes should be > 0 after the store.
	assert.Greater(t, metricValue(t, reg, "chainstore_entries_total"), 0.0)
	assert.Greater(t, metricValue(t, reg, "chainstore_bytes_total"), 0.0)

	// Histogram: at least one observation for chainstore_reconstruct_duration_seconds.
	// metricValue returns the sample count for histograms.
	require.Greater(t, metricValue(t, reg, "chainstore_reconstruct_duration_seconds"), 0.0,
		"reconstruct histogram must have ≥1 sample after Resolve")

	// Force eviction by storing past capacity and running EvictOldest.
	require.NoError(t, s.Store(ctx, "r1", "r0", "", []byte("world")))
	s.EvictOldest(ctx)
	assert.Greater(t, metricValue(t, reg, "chainstore_evictions_total"), 0.0)
}

// metricValue returns the current value of the named metric in reg. Works for
// gauges, counters, and histograms (returns the histogram sample count).
// Fails the test if the metric is not found.
func metricValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name || len(mf.Metric) == 0 {
			continue
		}
		m := mf.Metric[0]
		if c := m.GetCounter(); c != nil {
			return c.GetValue()
		}
		if g := m.GetGauge(); g != nil {
			return g.GetValue()
		}
		if h := m.GetHistogram(); h != nil {
			return float64(h.GetSampleCount())
		}
	}
	t.Fatalf("metric %q not found in registry", name)
	return 0
}

// keys returns the sorted keys of m. Used only for friendly test-failure messages.
func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// dto import kept available for future per-metric deep assertions.
var _ = (*dto.Metric)(nil)

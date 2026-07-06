package chainstore_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	assertGauge(t, reg, "chainstore_entries_total", func(v float64) bool { return v > 0 })
	assertGauge(t, reg, "chainstore_bytes_total", func(v float64) bool { return v > 0 })

	// Histogram: at least one observation for chainstore_reconstruct_duration_seconds.
	countAndSum := gatherCount(t, reg, "chainstore_reconstruct_duration_seconds")
	require.Greater(t, countAndSum, uint64(0), "reconstruct histogram must have ≥1 sample after Resolve")

	// Force eviction by storing past capacity and running EvictOldest.
	require.NoError(t, s.Store(ctx, "r1", "r0", "", []byte("world")))
	s.EvictOldest(ctx)
	assertGauge(t, reg, "chainstore_evictions_total", func(v float64) bool { return v > 0 })
}

func assertGauge(t *testing.T, reg *prometheus.Registry, name string, ok func(float64) bool) {
	t.Helper()
	for _, mf := range gatherAll(t, reg) {
		if mf.GetName() != name || len(mf.Metric) == 0 {
			continue
		}
		// Gauges and counters both surface as Metric points; the first field
		// is the live value regardless of type.
		v := mf.Metric[0].GetGauge().GetValue()
		if v == 0 && mf.Metric[0].GetCounter() != nil {
			v = mf.Metric[0].GetCounter().GetValue()
		}
		assert.True(t, ok(v), "metric %q = %v; want predicate to hold", name, v)
		return
	}
	t.Fatalf("metric %q not found", name)
}

func gatherCount(t *testing.T, reg *prometheus.Registry, name string) uint64 {
	t.Helper()
	for _, mf := range gatherAll(t, reg) {
		if mf.GetName() != name || len(mf.Metric) == 0 {
			continue
		}
		switch {
		case mf.Metric[0].GetCounter() != nil:
			return uint64(mf.Metric[0].GetCounter().GetValue())
		case mf.Metric[0].GetHistogram() != nil:
			return mf.Metric[0].GetHistogram().GetSampleCount()
		}
	}
	return 0
}

func gatherAll(t *testing.T, reg *prometheus.Registry) []*dto.MetricFamily {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	return mfs
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// Stable order for friendlier test output.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && strings.Compare(out[j-1], out[j]) > 0; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// Keep testutil imported so future tests can use it without re-importing.
var _ = testutil.ToFloat64

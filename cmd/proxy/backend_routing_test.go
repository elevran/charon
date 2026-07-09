package main

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// routingRecorder wraps the Charon mux with a middleware that records
// every HTTP method+path the proxy hit. Tests then assert which Charon
// entry path the proxy chose for a given store value.
type routingRecorder struct {
	mu   sync.Mutex
	hits []string
}

func (r *routingRecorder) middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			path := req.Method + " " + req.URL.Path
			if q := req.URL.RawQuery; q != "" {
				path += "?" + q
			}
			r.mu.Lock()
			r.hits = append(r.hits, path)
			r.mu.Unlock()
			next.ServeHTTP(w, req)
		})
	}
}

func (r *routingRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.hits))
	copy(out, r.hits)
	return out
}

// newRoutingStack builds a testStack whose Charon mux is wrapped in a
// routingRecorder, so tests can inspect which endpoints were called.
func newRoutingStack(t *testing.T) (*testStack, *routingRecorder) {
	t.Helper()
	rec := &routingRecorder{}
	s := newTestStack(t, withCharonMiddleware(rec.middleware()))
	return s, rec
}

func hitsContaining(hits []string, substr string) int {
	n := 0
	for _, h := range hits {
		if strings.Contains(h, substr) {
			n++
		}
	}
	return n
}

// TestProxyBufferedStoreTrueUsesStaging verifies that the buffered
// (stream:false, store:true) path uses POST /staging (committing the
// request blob) and never falls back to GET /chain.
func TestProxyBufferedStoreTrueUsesStaging(t *testing.T) {
	s, rec := newRoutingStack(t)

	resp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": "hello",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	hits := rec.snapshot()
	assert.GreaterOrEqual(t, hitsContaining(hits, "POST /staging"), 1, "store:true must hit POST /staging to commit the request blob")
	assert.Equal(t, 0, hitsContaining(hits, "GET /chain/"), "store:true must not hit GET /chain")
}

// TestProxyBufferedStoreFalseUsesChain verifies that the buffered
// (stream:false, store:false) path uses GET /chain (no commit) and not
// POST /staging.
func TestProxyBufferedStoreFalseUsesChain(t *testing.T) {
	s, rec := newRoutingStack(t)

	resp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": "hello",
		"store": false,
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	hits := rec.snapshot()
	assert.Equal(t, 0, hitsContaining(hits, "POST /staging"), "store:false must not hit POST /staging")
	assert.Equal(t, 0, hitsContaining(hits, "GET /chain/"), "first turn store:false has no prev to fetch")
}

// TestProxyBufferedStoreFalseContinuationUsesChain verifies that a
// store:false turn that continues from a stored prior response uses
// GET /chain (for context) and not POST /staging.
func TestProxyBufferedStoreFalseContinuationUsesChain(t *testing.T) {
	s, rec := newRoutingStack(t)

	// Step 1: store:true first turn.
	r0 := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": "anchor",
	})
	anchor := decodeJSON[ResponseResource](t, r0)
	require.Equal(t, http.StatusOK, r0.StatusCode)

	// Reset recorder so we only see calls from step 2.
	rec.mu.Lock()
	rec.hits = nil
	rec.mu.Unlock()

	// Step 2: store:false continuation from step 1.
	r1 := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model":                "test",
		"input":                "follow",
		"previous_response_id": anchor.ID,
		"store":                false,
	})
	defer r1.Body.Close()
	require.Equal(t, http.StatusOK, r1.StatusCode)

	hits := rec.snapshot()
	assert.GreaterOrEqual(t, hitsContaining(hits, "GET /chain/"+anchor.ID), 1, "store:false continuation must fetch context via GET /chain/{prev}")
	assert.Equal(t, 0, hitsContaining(hits, "POST /staging"), "store:false must not open a staging record")
}

// TestProxyStreamedStoreTrueUsesStaging verifies that the streamed
// (stream:true, store:true) path goes through POST /staging.
func TestProxyStreamedStoreTrueUsesStaging(t *testing.T) {
	s, rec := newRoutingStack(t)

	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxyURL+"/responses", strings.NewReader(`{"model":"test","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_ = readSSE(t, resp)

	hits := rec.snapshot()
	assert.GreaterOrEqual(t, hitsContaining(hits, "POST /staging"), 1, "streamed store:true must commit request via POST /staging")
	assert.Equal(t, 0, hitsContaining(hits, "GET /chain/"), "streamed store:true must not fetch via GET /chain")
}

// TestProxyStreamedStoreFalseFirstTurnHasNoChain verifies that the
// streamed (stream:true, store:false) first-turn path hits neither
// /staging nor /chain (no prior context to fetch, no staging to open).
func TestProxyStreamedStoreFalseFirstTurnHasNoChain(t *testing.T) {
	s, rec := newRoutingStack(t)

	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxyURL+"/responses",
		strings.NewReader(`{"model":"test","input":"hello","stream":true,"store":false}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_ = readSSE(t, resp)

	hits := rec.snapshot()
	assert.Equal(t, 0, hitsContaining(hits, "POST /staging"), "store:false first turn must not POST /staging")
	assert.Equal(t, 0, hitsContaining(hits, "GET /chain/"), "store:false first turn has no prev to fetch")
}

// TestProxyStreamedStoreFalseContinuationUsesChain verifies that a
// streamed (stream:true, store:false) continuation fetches context via
// GET /chain (not POST /staging).
func TestProxyStreamedStoreFalseContinuationUsesChain(t *testing.T) {
	s, rec := newRoutingStack(t)

	// First: store:true turn that gets persisted.
	r0 := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": "anchor",
	})
	anchor := decodeJSON[ResponseResource](t, r0)
	require.Equal(t, http.StatusOK, r0.StatusCode)

	rec.mu.Lock()
	rec.hits = nil
	rec.mu.Unlock()

	// Now: streamed store:false continuation.
	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxyURL+"/responses",
		strings.NewReader(`{"model":"test","input":"follow","stream":true,"store":false,"previous_response_id":"`+anchor.ID+`"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_ = readSSE(t, resp)

	hits := rec.snapshot()
	assert.GreaterOrEqual(t, hitsContaining(hits, "GET /chain/"+anchor.ID), 1, "streamed store:false continuation must fetch via GET /chain/{prev}")
	assert.Equal(t, 0, hitsContaining(hits, "POST /staging"), "streamed store:false continuation must not open a staging record")
}

// TestProxyStreamedStoreTrueNoChainFetches verifies that the streamed
// store:true path goes via POST /staging (which already returns turns
// alongside the staging_id), and does not additionally need a GET
// /chain hop.
func TestProxyStreamedStoreTrueNoChainFetches(t *testing.T) {
	s, rec := newRoutingStack(t)

	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxyURL+"/responses", strings.NewReader(`{"model":"test","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_ = readSSE(t, resp)

	hits := rec.snapshot()
	assert.GreaterOrEqual(t, hitsContaining(hits, "POST /staging"), 1, "store:true must commit via POST /staging")
	assert.Equal(t, 0, hitsContaining(hits, "GET /chain/"), "store:true first turn has no prev to fetch via GET /chain")
}

// TestBufferedProxySingleChunk verifies that a small buffered response produces
// exactly 1 chunk PUT and 1 complete (default 1 MiB cap is not exceeded).
func TestBufferedProxySingleChunk(t *testing.T) {
	rec := &routingRecorder{}
	s := newTestStack(t, withCharonMiddleware(rec.middleware()))

	resp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": "hello",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	hits := rec.snapshot()
	assert.Equal(t, 1, hitsContaining(hits, "/chunks/"), "small response must produce exactly 1 AppendChunk call")
	assert.Equal(t, 1, hitsContaining(hits, "/complete"), "exactly 1 Complete call")
	assert.Equal(t, 0, hitsContaining(hits, "/abort"), "no abort on success")
}

// TestStreamedProxySingleChunk verifies that a small streamed response produces
// exactly 1 chunk PUT and 1 complete.
func TestStreamedProxySingleChunk(t *testing.T) {
	rec := &routingRecorder{}
	s := newTestStack(t, withCharonMiddleware(rec.middleware()))

	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxyURL+"/responses",
		strings.NewReader(`{"model":"test","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_ = readSSE(t, resp)

	hits := rec.snapshot()
	assert.Equal(t, 1, hitsContaining(hits, "/chunks/"), "small response must produce exactly 1 AppendChunk call")
	assert.Equal(t, 1, hitsContaining(hits, "/complete"), "exactly 1 Complete call")
	assert.Equal(t, 0, hitsContaining(hits, "/abort"), "no abort on success")
}

// TestBufferedProxyMultipleChunks verifies that a tiny chunk cap splits the
// buffered response blob into multiple chunk PUTs.
func TestBufferedProxyMultipleChunks(t *testing.T) {
	rec := &routingRecorder{}
	s := newTestStack(t, withMaxChunkBytes(64), withCharonMiddleware(rec.middleware()))

	resp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": "hello world, this request should produce a response blob that exceeds 64 bytes after marshaling",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	hits := rec.snapshot()
	assert.GreaterOrEqual(t, hitsContaining(hits, "/chunks/"), 2, "tiny cap must produce ≥2 AppendChunk calls")
	assert.Equal(t, 1, hitsContaining(hits, "/complete"), "exactly 1 Complete call")
	assert.Equal(t, 0, hitsContaining(hits, "/abort"), "no abort on success")
}

// TestBufferedProxyStoreFalseNoStagingCalls pins that a buffered store:false
// turn issues no Charon staging calls at all — neither AppendChunk, Complete,
// nor Abort. The empty-response abort path is covered by disruptive_test.go.
func TestBufferedProxyStoreFalseNoStagingCalls(t *testing.T) {
	rec := &routingRecorder{}
	s := newTestStack(t, withCharonMiddleware(rec.middleware()))

	resp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": "hello",
		"store": false,
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	hits := rec.snapshot()
	assert.Equal(t, 0, hitsContaining(hits, "/chunks/"), "store:false must not call AppendChunk")
	assert.Equal(t, 0, hitsContaining(hits, "/complete"), "store:false must not call Complete")
	assert.Equal(t, 0, hitsContaining(hits, "/abort"), "store:false must not call Abort")
}

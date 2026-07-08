package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Throughput benchmarks
// ---------------------------------------------------------------------------

// BenchmarkCreateSmallRequest measures POST /responses throughput with a
// minimal single-item input (single-hop: no previous_response_id).
func BenchmarkCreateSmallRequest(b *testing.B) {
	s := startStack(b)
	body, _ := json.Marshal(map[string]interface{}{
		"model": "test",
		"input": "hello",
	})
	b.ResetTimer()
	for i := range b.N {
		req, _ := http.NewRequestWithContext(context.Background(), "POST",
			s.proxySrv.URL+"/responses", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			b.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("unexpected status %d", resp.StatusCode)
		}
	}
}

// BenchmarkCreateLargeContext measures POST /responses throughput with a
// large input array (50 items simulating a long conversation).
func BenchmarkCreateLargeContext(b *testing.B) {
	s := startStack(b)

	items := make([]map[string]interface{}, 50)
	for i := range items {
		role := "user"
		if i%2 == 1 {
			role = "assistant"
		}
		items[i] = map[string]interface{}{
			"type":    "message",
			"role":    role,
			"content": fmt.Sprintf("turn %d: %s", i, strings.Repeat("x", 200)),
		}
	}
	body, _ := json.Marshal(map[string]interface{}{
		"model": "test",
		"input": items,
	})
	b.ResetTimer()
	for i := range b.N {
		req, _ := http.NewRequestWithContext(context.Background(), "POST",
			s.proxySrv.URL+"/responses", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			b.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("unexpected status %d", resp.StatusCode)
		}
	}
}

// BenchmarkCreateWithChain measures POST /responses throughput for continuation
// turns (with previous_response_id — involves a Charon resolve call).
func BenchmarkCreateWithChain(b *testing.B) {
	s := startStack(b)

	// Store turn 0 to seed the chain.
	seed, _ := json.Marshal(map[string]interface{}{"model": "test", "input": "seed"})
	req0, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxySrv.URL+"/responses", bytes.NewReader(seed))
	req0.Header.Set("Content-Type", "application/json")
	resp0, _ := http.DefaultClient.Do(req0)
	var r0 struct {
		ID string `json:"id"`
	}
	json.NewDecoder(resp0.Body).Decode(&r0)
	resp0.Body.Close()

	prevID := r0.ID
	b.ResetTimer()
	for i := range b.N {
		body, _ := json.Marshal(map[string]interface{}{
			"model":                "test",
			"input":                fmt.Sprintf("turn %d", i),
			"previous_response_id": prevID,
		})
		req, _ := http.NewRequestWithContext(context.Background(), "POST",
			s.proxySrv.URL+"/responses", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			b.Fatalf("request %d: %v", i, err)
		}
		var r struct {
			ID string `json:"id"`
		}
		json.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("unexpected status %d", resp.StatusCode)
		}
		prevID = r.ID
	}
}

// BenchmarkStreamResponse measures POST /responses with stream:true throughput.
func BenchmarkStreamResponse(b *testing.B) {
	s := startStack(b)
	body, _ := json.Marshal(map[string]interface{}{
		"model":  "test",
		"input":  "hello",
		"stream": true,
	})
	b.ResetTimer()
	for i := range b.N {
		req, _ := http.NewRequestWithContext(context.Background(), "POST",
			s.proxySrv.URL+"/responses", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			b.Fatalf("request %d: %v", i, err)
		}
		// Drain the SSE stream.
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 && bytes.Contains(buf[:n], []byte("response.completed")) {
				break
			}
			if err != nil {
				break
			}
		}
		resp.Body.Close()
	}
}

// ---------------------------------------------------------------------------
// Concurrency benchmarks
// ---------------------------------------------------------------------------

// BenchmarkConcurrent10 measures throughput with 10 concurrent clients.
func BenchmarkConcurrent10(b *testing.B) {
	benchmarkConcurrent(b, 10)
}

// BenchmarkConcurrent50 measures throughput with 50 concurrent clients.
func BenchmarkConcurrent50(b *testing.B) {
	benchmarkConcurrent(b, 50)
}

// BenchmarkConcurrent100 measures throughput with 100 concurrent clients.
func BenchmarkConcurrent100(b *testing.B) {
	benchmarkConcurrent(b, 100)
}

func benchmarkConcurrent(b *testing.B, concurrency int) {
	b.Helper()
	s := startStack(b)
	body, _ := json.Marshal(map[string]interface{}{"model": "test", "input": "hello"})

	b.ResetTimer()
	b.SetParallelism(concurrency)
	b.RunParallel(func(pb *testing.PB) {
		client := &http.Client{}
		for pb.Next() {
			req, _ := http.NewRequestWithContext(context.Background(), "POST",
				s.proxySrv.URL+"/responses", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				b.Errorf("request: %v", err)
				return
			}
			resp.Body.Close()
		}
	})
}

// ---------------------------------------------------------------------------
// Scale tests (TestScale*): run with go test -run TestScale -v
// ---------------------------------------------------------------------------

// TestScaleSequentialChain verifies 100-turn chain reconstruction is correct
// under the full proxy+Charon stack (exercises checkpointing + chain walk).
func TestScaleSequentialChain(t *testing.T) {
	s := startStack(t)

	var prevID string
	const turns = 100

	for i := range turns {
		body := map[string]interface{}{
			"model": "test",
			"input": fmt.Sprintf("turn %d", i),
		}
		if prevID != "" {
			body["previous_response_id"] = prevID
		}
		bodyBytes, _ := json.Marshal(body)
		req, _ := http.NewRequestWithContext(context.Background(), "POST",
			s.proxySrv.URL+"/responses", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("turn %d: %v", i, err)
		}
		var r struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
			t.Fatalf("turn %d decode: %v", i, err)
		}
		resp.Body.Close()
		if r.Status != "completed" {
			t.Fatalf("turn %d: status=%q", i, r.Status)
		}
		prevID = r.ID
	}
	t.Logf("completed %d-turn chain; final response ID: %s", turns, prevID)
}

// TestScaleConcurrentNewChains sends 50 independent new-chain requests
// concurrently and verifies all succeed.
func TestScaleConcurrentNewChains(t *testing.T) {
	s := startStack(t)
	const n = 50
	var wg sync.WaitGroup
	errors := make(chan error, n)

	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			body, _ := json.Marshal(map[string]interface{}{
				"model": "test",
				"input": fmt.Sprintf("concurrent request %d", idx),
			})
			req, _ := http.NewRequestWithContext(context.Background(), "POST",
				s.proxySrv.URL+"/responses", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errors <- fmt.Errorf("request %d: %w", idx, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errors <- fmt.Errorf("request %d: status %d", idx, resp.StatusCode)
			}
		}(i)
	}
	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

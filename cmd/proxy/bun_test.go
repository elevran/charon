//go:build openresponses_bun_compliance

package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// The 11 non-GPU tests that can be validated with a deterministic mock.
// response-output-phase-schema is excluded: it uses a local fixture and
// does not make HTTP requests, so the bun runner skips it automatically.
const bunFilterTests = "basic-response,streaming-response,system-prompt,multi-turn," +
	"websocket-response,websocket-sequential-responses,websocket-continuation," +
	"websocket-reconnect-store-false-recovery,websocket-previous-response-not-found," +
	"websocket-failed-continuation-evicts-cache,compact-missing-model"

// TestBunComplianceSuite runs the canonical openresponses.org compliance suite
// against a local stack. Requires:
//
//	bun     — in PATH (https://bun.sh)
//	OPENRESPONSES_DIR — path to a clone of https://github.com/openresponses/openresponses
//
// Run with:
//
//	go test -tags openresponses-bun-compliance ./cmd/proxy/... -run TestBunComplianceSuite -v
//
// Or via the Makefile:
//
//	make test-compliance-bun OPENRESPONSES_DIR=/path/to/openresponses
func TestBunComplianceSuite(t *testing.T) {
	bunPath, err := exec.LookPath("bun")
	if err != nil {
		t.Skip("bun not found in PATH — install from https://bun.sh to run openresponses compliance suite")
	}

	openresponsesDir := os.Getenv("OPENRESPONSES_DIR")
	if openresponsesDir == "" {
		t.Skip("OPENRESPONSES_DIR not set — clone https://github.com/openresponses/openresponses and set env var")
	}

	// Boot the full stack on real TCP ports (bun connects from a separate process).
	s := newTestStack(t, withRealListeners(), withTimeout(15*time.Second))
	proxyURL := s.proxyURL

	t.Logf("proxy listening at %s", proxyURL)
	t.Logf("running: bun run test:compliance --base-url %s --filter %s --json", proxyURL, bunFilterTests)

	// Note: build tag uses underscores (openresponses_bun_compliance) because
	// Go build tags do not allow hyphens in tag names.
	cmd := exec.Command(bunPath, "run", "test:compliance",
		"--base-url", proxyURL,
		"--filter", bunFilterTests,
		"--json",
	)
	cmd.Dir = openresponsesDir
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			t.Logf("bun stderr:\n%s", ee.Stderr)
		}
		t.Logf("bun stdout:\n%s", out)
	}

	// Parse JSON result.
	type testResult struct {
		Name   string `json:"name"`
		Status string `json:"status"` // "pass" | "fail" | "skip"
		Error  string `json:"error,omitempty"`
	}
	type suiteResult struct {
		Passed  int          `json:"passed"`
		Failed  int          `json:"failed"`
		Skipped int          `json:"skipped"`
		Results []testResult `json:"results"`
	}

	jsonStart := strings.Index(string(out), "{")
	if jsonStart < 0 {
		t.Fatalf("no JSON in bun output:\n%s", out)
	}
	var suite suiteResult
	if parseErr := json.Unmarshal(out[jsonStart:], &suite); parseErr != nil {
		t.Fatalf("parse bun output: %v\nraw:\n%s", parseErr, out)
	}

	t.Logf("bun compliance: passed=%d failed=%d skipped=%d", suite.Passed, suite.Failed, suite.Skipped)
	for _, r := range suite.Results {
		if r.Status == "fail" {
			t.Errorf("FAIL %s: %s", r.Name, r.Error)
		}
	}
}

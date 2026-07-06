// Command cache-check opens a Charon chainstore Pebble directory in read-only
// mode and verifies on-disk consistency: depth invariants and dangling LRU
// entries. It is intended for offline diagnostic use after a hard crash or
// for periodic sanity checks against a quiescent store.
//
// Usage:
//
//	cache-check --data-dir=/var/lib/charon/data
//
// Exits 0 when no errors are found, 1 otherwise.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"

	chainstorepebble "github.com/elevran/charon/internal/chainstore/pebble"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "cache-check:", err)
		os.Exit(1)
	}
}

func run() error {
	dataDir := flag.String("data-dir", "", "Pebble data directory to check (required)")
	flag.Parse()

	if *dataDir == "" {
		flag.Usage()
		return fmt.Errorf("--data-dir is required")
	}

	// Open in read-only mode — no background goroutines, no WAL writes.
	opts := &pebble.Options{
		ReadOnly: true,
	}
	db, err := pebble.Open(*dataDir, opts)
	if err != nil {
		return fmt.Errorf("open %s: %w", *dataDir, err)
	}
	defer func() { _ = db.Close() }()

	b := chainstorepebble.NewBackend(db)
	report, err := b.ConsistencyCheck(context.Background())
	if err != nil {
		return fmt.Errorf("consistency check: %w", err)
	}

	printReport(os.Stdout, report)

	if !report.OK {
		os.Exit(1)
	}
	return nil
}

// printReport writes a human-readable summary to w. Always called once before exit.
func printReport(w *os.File, r *chainstorepebble.ConsistencyReport) {
	fmt.Fprintf(w, "nodes scanned:      %d\n", r.NodesScanned)
	fmt.Fprintf(w, "LRU entries scanned: %d\n", r.LRUEntriesScanned)
	fmt.Fprintf(w, "depth errors:       %d\n", len(r.DepthErrors))
	fmt.Fprintf(w, "dangling LRU:       %d\n", len(r.DanglingLRU))
	fmt.Fprintf(w, "decode errors:      %d\n", len(r.DecodeErrors))

	if len(r.DepthErrors) > 0 {
		fmt.Fprintln(w, "\nDepth errors:")
		for _, e := range r.DepthErrors {
			fmt.Fprintf(w, "  %s\n", e)
		}
	}
	if len(r.DanglingLRU) > 0 {
		fmt.Fprintln(w, "\nDangling LRU entries:")
		for _, e := range r.DanglingLRU {
			fmt.Fprintf(w, "  %s\n", e)
		}
	}
	if len(r.DecodeErrors) > 0 {
		fmt.Fprintln(w, "\nDecode errors:")
		for _, e := range r.DecodeErrors {
			fmt.Fprintf(w, "  %s\n", e)
		}
	}

	if r.OK {
		fmt.Fprintln(w, "\nOK")
	} else {
		fmt.Fprintln(w, "\nFAILED")
	}
}

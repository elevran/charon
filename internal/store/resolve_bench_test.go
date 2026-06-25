package store_test

// Context-restore latency benchmarks across backends (memory, SQLite+filesystem).
// Measures Resolve() time as a function of chain depth: 1, 10, 50, 100, 500 turns.
//
// Run:
//   go test ./internal/store/... -bench=BenchmarkResolveContext -benchtime=5s -benchmem
//
// Profile (if latency is interesting):
//   go test ./internal/store/... -bench=BenchmarkResolveContext -cpuprofile=/tmp/cpu.pprof
//   go tool pprof -http=:6060 /tmp/cpu.pprof

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/elevran/charon/internal/config"
	"github.com/elevran/charon/internal/storage/memory"
	sqlitestore "github.com/elevran/charon/internal/storage/sqlite"
	"github.com/elevran/charon/internal/store"
)

var chainDepths = []int{1, 10, 50, 100, 500}

// buildChain stores N turns and returns the last response ID.
func buildChain(b *testing.B, svc *store.ContextStore, n int) string {
	b.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	_ = log

	item := json.RawMessage(`{"type":"message","role":"user","content":"` + fmt.Sprintf("%0200d", 0) + `"}`)

	var prevID *string
	var lastID string
	for i := range n {
		id := fmt.Sprintf("resp_%08d", i)
		req := storeRequest(prevID, []json.RawMessage{item}, []json.RawMessage{item})
		if err := svc.Store(benchCtx, id, req); err != nil {
			b.Fatalf("Store turn %d: %v", i, err)
		}
		prevID = &id
		lastID = id
	}
	return lastID
}

// benchCtx is a package-level context for benchmarks (ctx is already declared in store_test.go).
var benchCtx = ctx

// --- Memory backend ---

func BenchmarkResolveContext_Memory_1(b *testing.B)   { benchResolveMemory(b, 1) }
func BenchmarkResolveContext_Memory_10(b *testing.B)  { benchResolveMemory(b, 10) }
func BenchmarkResolveContext_Memory_50(b *testing.B)  { benchResolveMemory(b, 50) }
func BenchmarkResolveContext_Memory_100(b *testing.B) { benchResolveMemory(b, 100) }
func BenchmarkResolveContext_Memory_500(b *testing.B) { benchResolveMemory(b, 500) }

func benchResolveMemory(b *testing.B, depth int) {
	b.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := store.New(memory.NewIndexStore(), memory.NewPayloadStore(),
		store.Config{}, log)
	headID := buildChain(b, svc, depth)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		_, _, err := svc.Resolve(benchCtx, headID, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- SQLite + filesystem backend ---

func BenchmarkResolveContext_SQLite_1(b *testing.B)   { benchResolveSQLite(b, 1) }
func BenchmarkResolveContext_SQLite_10(b *testing.B)  { benchResolveSQLite(b, 10) }
func BenchmarkResolveContext_SQLite_50(b *testing.B)  { benchResolveSQLite(b, 50) }
func BenchmarkResolveContext_SQLite_100(b *testing.B) { benchResolveSQLite(b, 100) }
func BenchmarkResolveContext_SQLite_500(b *testing.B) { benchResolveSQLite(b, 500) }

func benchResolveSQLite(b *testing.B, depth int) {
	b.Helper()
	storageCfg := config.StorageConfig{
		DataDir: b.TempDir(),
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	idx, pay, cleanup, err := sqlitestore.Open(storageCfg, log)
	if err != nil {
		b.Fatalf("open sqlite: %v", err)
	}
	b.Cleanup(func() { _ = cleanup() })
	svc := store.New(idx, pay, store.Config{}, log)

	headID := buildChain(b, svc, depth)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		_, _, err := svc.Resolve(benchCtx, headID, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

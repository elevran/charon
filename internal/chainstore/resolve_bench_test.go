package chainstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	crdbpebble "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/elevran/charon/internal/chainstore"
	chainstorepebble "github.com/elevran/charon/internal/chainstore/pebble"
)

// benchResolve seeds a chain of the given depth then benchmarks ResolveAndStage
// from the leaf on the given store.
func benchResolve(b *testing.B, svc *chainstore.Store, depth int) {
	b.Helper()
	ctx := context.Background()

	reqBlob := []byte(`{"input":[{"type":"message","role":"user","content":"hello"}]}`)
	respBlob := []byte(`{"id":"x","model":"m","status":"completed","output":[{"type":"message","role":"assistant","content":"ok"}]}`)

	// Seed the chain.
	var leafID string
	for i := range depth {
		id := fmt.Sprintf("bench_%d_%08d", depth, i)
		stagingID, _, err := svc.ResolveAndStage(ctx, leafID, "", reqBlob)
		if err != nil {
			b.Fatalf("seed ResolveAndStage depth %d: %v", i, err)
		}
		if err := svc.StoreWithStaging(ctx, stagingID, id, "", "", respBlob); err != nil {
			b.Fatalf("seed StoreWithStaging depth %d: %v", i, err)
		}
		leafID = id
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		_, _, err := svc.ResolveAndStage(ctx, leafID, "", reqBlob)
		if err != nil {
			b.Fatalf("ResolveAndStage: %v", err)
		}
		// Abandoned staging records live in a separate key prefix; they don't
		// affect chain-walk performance and are reaped by the TTL reaper.
	}
	b.StopTimer()
}

// --- In-memory vfs (pebble with FS=vfs.NewMem()) ---

// openBenchStore opens a chainstore.Store with the given config. Pass dir=""
// and opts.FS=vfs.NewMem() for in-memory; pass a temp dir and a zero Options{}
// for on-disk. Benchmarks pass either the zero Config (cache off) or a Config
// with CacheMaxBytes > 0 (cache on) to compare the two paths.
func openBenchStore(b *testing.B, dir string, opts *crdbpebble.Options, cfg chainstore.Config) *chainstore.Store {
	b.Helper()
	svc, err := chainstorepebble.Open(context.Background(), dir, opts, cfg)
	if err != nil {
		b.Fatalf("open bench store: %v", err)
	}
	b.Cleanup(func() { _ = svc.Close() })
	return svc
}

func cachedCfg() chainstore.Config {
	return chainstore.Config{CacheMaxBytes: 64 * 1024 * 1024, CacheTTL: time.Hour}
}

func BenchmarkResolveContext_Pebble_Mem_1(b *testing.B) {
	benchResolve(b, openBenchStore(b, "", &crdbpebble.Options{FS: vfs.NewMem()}, chainstore.Config{}), 1)
}

func BenchmarkResolveContext_Pebble_Mem_10(b *testing.B) {
	benchResolve(b, openBenchStore(b, "", &crdbpebble.Options{FS: vfs.NewMem()}, chainstore.Config{}), 10)
}

func BenchmarkResolveContext_Pebble_Mem_100(b *testing.B) {
	benchResolve(b, openBenchStore(b, "", &crdbpebble.Options{FS: vfs.NewMem()}, chainstore.Config{}), 100)
}

func BenchmarkResolveContext_Pebble_Mem_500(b *testing.B) {
	benchResolve(b, openBenchStore(b, "", &crdbpebble.Options{FS: vfs.NewMem()}, chainstore.Config{}), 500)
}

func BenchmarkResolveContext_Pebble_Mem_Cached_1(b *testing.B) {
	benchResolve(b, openBenchStore(b, "", &crdbpebble.Options{FS: vfs.NewMem()}, cachedCfg()), 1)
}

func BenchmarkResolveContext_Pebble_Mem_Cached_10(b *testing.B) {
	benchResolve(b, openBenchStore(b, "", &crdbpebble.Options{FS: vfs.NewMem()}, cachedCfg()), 10)
}

func BenchmarkResolveContext_Pebble_Mem_Cached_100(b *testing.B) {
	benchResolve(b, openBenchStore(b, "", &crdbpebble.Options{FS: vfs.NewMem()}, cachedCfg()), 100)
}

func BenchmarkResolveContext_Pebble_Mem_Cached_500(b *testing.B) {
	benchResolve(b, openBenchStore(b, "", &crdbpebble.Options{FS: vfs.NewMem()}, cachedCfg()), 500)
}

// --- On-disk pebble (real filesystem) ---

func BenchmarkResolveContext_Pebble_Disk_1(b *testing.B) {
	benchResolve(b, openBenchStore(b, b.TempDir(), &crdbpebble.Options{}, chainstore.Config{}), 1)
}

func BenchmarkResolveContext_Pebble_Disk_10(b *testing.B) {
	benchResolve(b, openBenchStore(b, b.TempDir(), &crdbpebble.Options{}, chainstore.Config{}), 10)
}

func BenchmarkResolveContext_Pebble_Disk_100(b *testing.B) {
	benchResolve(b, openBenchStore(b, b.TempDir(), &crdbpebble.Options{}, chainstore.Config{}), 100)
}

func BenchmarkResolveContext_Pebble_Disk_500(b *testing.B) {
	benchResolve(b, openBenchStore(b, b.TempDir(), &crdbpebble.Options{}, chainstore.Config{}), 500)
}

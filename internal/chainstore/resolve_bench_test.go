package chainstore_test

import (
	"context"
	"fmt"
	"testing"

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

func openBenchMemStore(b *testing.B) *chainstore.Store {
	b.Helper()
	opts := &crdbpebble.Options{FS: vfs.NewMem()}
	svc, err := chainstorepebble.Open(context.Background(), "", opts, chainstore.Config{})
	if err != nil {
		b.Fatalf("open mem store: %v", err)
	}
	b.Cleanup(func() { _ = svc.Close() })
	return svc
}

func BenchmarkResolveContext_Pebble_Mem_1(b *testing.B) {
	benchResolve(b, openBenchMemStore(b), 1)
}

func BenchmarkResolveContext_Pebble_Mem_10(b *testing.B) {
	benchResolve(b, openBenchMemStore(b), 10)
}

func BenchmarkResolveContext_Pebble_Mem_100(b *testing.B) {
	benchResolve(b, openBenchMemStore(b), 100)
}

func BenchmarkResolveContext_Pebble_Mem_500(b *testing.B) {
	benchResolve(b, openBenchMemStore(b), 500)
}

// --- On-disk pebble (real filesystem) ---

func openBenchDiskStore(b *testing.B) *chainstore.Store {
	b.Helper()
	dir := b.TempDir()
	svc, err := chainstorepebble.Open(context.Background(), dir, &crdbpebble.Options{}, chainstore.Config{})
	if err != nil {
		b.Fatalf("open disk store: %v", err)
	}
	b.Cleanup(func() { _ = svc.Close() })
	return svc
}

func BenchmarkResolveContext_Pebble_Disk_1(b *testing.B) {
	benchResolve(b, openBenchDiskStore(b), 1)
}

func BenchmarkResolveContext_Pebble_Disk_10(b *testing.B) {
	benchResolve(b, openBenchDiskStore(b), 10)
}

func BenchmarkResolveContext_Pebble_Disk_100(b *testing.B) {
	benchResolve(b, openBenchDiskStore(b), 100)
}

func BenchmarkResolveContext_Pebble_Disk_500(b *testing.B) {
	benchResolve(b, openBenchDiskStore(b), 500)
}

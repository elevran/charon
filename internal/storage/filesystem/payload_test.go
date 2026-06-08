package filesystem_test

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/elevran/charon/internal/storage"
	"github.com/elevran/charon/internal/storage/filesystem"
)

var ctx = context.Background()

func newStore(t *testing.T) *filesystem.PayloadStore {
	t.Helper()
	s, err := filesystem.New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestPutGetRoundTrip(t *testing.T) {
	s := newStore(t)
	data := []byte(`{"type":"message","content":"hello"}`)
	key := "chain_root/00000001_resp_abc.json"

	if err := s.Put(ctx, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("data mismatch: got %q, want %q", got, data)
	}
}

func TestGetNotFound(t *testing.T) {
	s := newStore(t)
	_, err := s.Get(ctx, "chain/00000000_missing.json")
	if err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestDeleteIdempotent(t *testing.T) {
	s := newStore(t)
	key := "chain/00000000_resp_del.json"
	_ = s.Put(ctx, key, []byte("data"))

	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("first Delete: %v", err)
	}
	// Second delete of absent key must not error.
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("second Delete on absent key: %v", err)
	}
	if _, err := s.Get(ctx, key); err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestParentDirectoryCreatedOnPut(t *testing.T) {
	s := newStore(t)
	// Key whose parent directory does not yet exist.
	key := "new_chain_root/00000000_resp_x.json"
	if err := s.Put(ctx, key, []byte("content")); err != nil {
		t.Fatalf("Put with new parent dir: %v", err)
	}
	got, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after new parent dir: %v", err)
	}
	if string(got) != "content" {
		t.Errorf("unexpected content: %q", got)
	}
}

func TestPutOverwriteIsAtomic(t *testing.T) {
	// Overwriting an existing key must produce the new content — no partial
	// state visible to concurrent readers.
	s := newStore(t)
	key := "chain/00000000_resp_overwrite.json"
	original := bytes.Repeat([]byte("A"), 64*1024) // 64 KB
	updated := bytes.Repeat([]byte("B"), 64*1024)

	_ = s.Put(ctx, key, original)

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := s.Get(ctx, key)
			if err != nil {
				return
			}
			// Must be all-A or all-B — never a mix.
			if !bytes.Equal(data, original) && !bytes.Equal(data, updated) {
				t.Errorf("partial write observed: len=%d", len(data))
			}
		}()
	}
	_ = s.Put(ctx, key, updated)
	wg.Wait()

	final, _ := s.Get(ctx, key)
	if !bytes.Equal(final, updated) {
		t.Errorf("final content is not updated data")
	}
}

func TestConcurrentPutGet(t *testing.T) {
	s := newStore(t)
	const n = 20
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := filepath.Join("chain", filepath.FromSlash(
				"00000000_resp_concurrent_"+string(rune('a'+idx))+".json",
			))
			data := []byte{byte(idx)}
			_ = s.Put(ctx, key, data)
			_, _ = s.Get(ctx, key)
		}(i)
	}
	wg.Wait()
}

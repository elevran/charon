package filesystem

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/elevran/charon/internal/storage"
)

var _ storage.PayloadStore = (*PayloadStore)(nil)

// PayloadStore stores blobs on the local filesystem under baseDir.
// Keys are relative paths of the form "chainRootID/XXXXXXXX_responseID.json".
// Writes are atomic: content is written to a temp file in the same directory,
// then renamed into place so readers never see a partial write.
type PayloadStore struct{ baseDir string }

// New creates a PayloadStore rooted at baseDir, creating the directory if absent.
func New(baseDir string) (*PayloadStore, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}
	return &PayloadStore{baseDir: baseDir}, nil
}

func (s *PayloadStore) fullPath(key string) string {
	return filepath.Join(s.baseDir, filepath.FromSlash(key))
}

func (s *PayloadStore) Put(_ context.Context, key string, data []byte) error {
	dst := s.fullPath(key)
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// Write to a temp file in the same directory so os.Rename is atomic
	// (same filesystem, no cross-device move).
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, dst)
}

func (s *PayloadStore) Get(_ context.Context, key string) ([]byte, error) {
	data, err := os.ReadFile(s.fullPath(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, storage.ErrNotFound
	}
	return data, err
}

// Delete removes the payload file. Idempotent — no error if the file did not exist.
func (s *PayloadStore) Delete(_ context.Context, key string) error {
	err := os.Remove(s.fullPath(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

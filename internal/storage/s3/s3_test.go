//go:build integration

package s3_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/config"
	"github.com/elevran/charon/internal/storage"
	s3store "github.com/elevran/charon/internal/storage/s3"
)

// TestS3PayloadStore exercises the S3 PayloadStore against a real bucket.
// Set S3_BUCKET (and optionally S3_ENDPOINT_URL, S3_REGION, S3_ACCESS_KEY_ID,
// S3_SECRET_ACCESS_KEY, S3_PATH_STYLE) to run:
//
//	S3_BUCKET=mybucket S3_ENDPOINT_URL=http://localhost:9000 S3_PATH_STYLE=true \
//	  go test -tags integration ./internal/storage/s3/...
func TestS3PayloadStore(t *testing.T) {
	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		t.Skip("S3_BUCKET not set; skipping integration test")
	}

	pathStyle := os.Getenv("S3_PATH_STYLE") == "true"

	cfg := config.StorageConfig{
		S3: config.S3Config{
			Bucket:          bucket,
			Region:          envOrDefault("S3_REGION", "us-east-1"),
			EndpointURL:     os.Getenv("S3_ENDPOINT_URL"),
			AccessKeyID:     os.Getenv("S3_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"),
			PathStyle:       pathStyle,
		},
	}

	store, err := s3store.Open(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	key := fmt.Sprintf("integration-test/%d/payload.json", time.Now().UnixNano())
	data := []byte(`{"hello":"world"}`)

	t.Run("PutAndGet", func(t *testing.T) {
		require.NoError(t, store.Put(ctx, key, data))

		got, err := store.Get(ctx, key)
		require.NoError(t, err)
		require.Equal(t, data, got)
	})

	t.Run("GetNotFound", func(t *testing.T) {
		_, err := store.Get(ctx, "no/such/key/"+key)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("DeleteIdempotent", func(t *testing.T) {
		require.NoError(t, store.Delete(ctx, key))
		// Second delete is idempotent
		require.NoError(t, store.Delete(ctx, key))
	})

	t.Run("GetAfterDelete", func(t *testing.T) {
		_, err := store.Get(ctx, key)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

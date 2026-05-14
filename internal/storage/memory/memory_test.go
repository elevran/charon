package memory_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
	"github.com/elevran/charon/internal/storage/memory"
)

var ctx = context.Background()

// --- PayloadStore tests ---

func TestPayloadPutGetDelete(t *testing.T) {
	s := memory.NewPayloadStore()
	require.NoError(t, s.Put(ctx, "k1", []byte("hello")))

	got, err := s.Get(ctx, "k1")
	require.NoError(t, err)
	assert.Equal(t, "hello", string(got))

	require.NoError(t, s.Delete(ctx, "k1"))
	_, err = s.Get(ctx, "k1")
	assert.ErrorIs(t, err, storage.ErrNotFound)
}

func TestPayloadGetNotFound(t *testing.T) {
	s := memory.NewPayloadStore()
	_, err := s.Get(ctx, "missing")
	assert.ErrorIs(t, err, storage.ErrNotFound)
}

func TestPayloadConcurrent(t *testing.T) {
	s := memory.NewPayloadStore()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Put(ctx, "k", []byte("v"))
			_, _ = s.Get(ctx, "k")
		}()
	}
	wg.Wait()
}

// --- IndexStore tests ---

func newMeta(id string, expiresAt *int64) model.ResponseMeta {
	return model.ResponseMeta{
		ID:          id,
		ChainRootID: id,
		Status:      model.StatusCompleted,
		CreatedAt:   time.Now().Unix(),
		ExpiresAt:   expiresAt,
	}
}

func TestIndexPutGetDelete(t *testing.T) {
	s := memory.NewIndexStore()
	m := newMeta("id1", nil)
	require.NoError(t, s.Put(ctx, m))

	got, err := s.Get(ctx, "id1")
	require.NoError(t, err)
	assert.Equal(t, "id1", got.ID)

	require.NoError(t, s.Delete(ctx, "id1"))
	_, err = s.Get(ctx, "id1")
	assert.ErrorIs(t, err, storage.ErrNotFound)
}

func TestIndexGetNotFound(t *testing.T) {
	s := memory.NewIndexStore()
	_, err := s.Get(ctx, "missing")
	assert.ErrorIs(t, err, storage.ErrNotFound)
}

func TestIndexListExpired(t *testing.T) {
	s := memory.NewIndexStore()
	past := time.Now().Add(-time.Hour).Unix()
	future := time.Now().Add(time.Hour).Unix()
	require.NoError(t, s.Put(ctx, newMeta("exp1", &past)))
	require.NoError(t, s.Put(ctx, newMeta("exp2", &past)))
	require.NoError(t, s.Put(ctx, newMeta("live", &future)))
	require.NoError(t, s.Put(ctx, newMeta("noexpiry", nil)))

	expired, err := s.ListExpired(ctx, time.Now().Unix())
	require.NoError(t, err)
	assert.Len(t, expired, 2)
}

func TestIndexListStaleWriteIntents(t *testing.T) {
	s := memory.NewIndexStore()
	oldTime := time.Now().Add(-10 * time.Minute).Unix()

	pending := model.WriteIntent{IntentID: "i1", ResponseID: "r1", Phase: model.WriteIntentPending, CreatedAt: oldTime, UpdatedAt: oldTime}
	fileWritten := model.WriteIntent{IntentID: "i2", ResponseID: "r2", Phase: model.WriteIntentFileWritten, CreatedAt: oldTime, UpdatedAt: oldTime}
	committed := model.WriteIntent{IntentID: "i3", ResponseID: "r3", Phase: model.WriteIntentCommitted, CreatedAt: oldTime, UpdatedAt: oldTime}
	recent := model.WriteIntent{IntentID: "i4", ResponseID: "r4", Phase: model.WriteIntentPending, CreatedAt: time.Now().Unix(), UpdatedAt: time.Now().Unix()}

	require.NoError(t, s.InsertWriteIntent(ctx, pending))
	require.NoError(t, s.InsertWriteIntent(ctx, fileWritten))
	require.NoError(t, s.InsertWriteIntent(ctx, committed))
	require.NoError(t, s.InsertWriteIntent(ctx, recent))

	stale, err := s.ListStaleWriteIntents(ctx, 5*time.Minute)
	require.NoError(t, err)
	assert.Len(t, stale, 2)
}

func TestIndexWriteIntentLifecycle(t *testing.T) {
	s := memory.NewIndexStore()
	intent := model.WriteIntent{IntentID: "wi1", ResponseID: "r1", Phase: model.WriteIntentPending, CreatedAt: time.Now().Unix(), UpdatedAt: time.Now().Unix()}
	require.NoError(t, s.InsertWriteIntent(ctx, intent))

	err := s.InsertWriteIntent(ctx, intent)
	assert.ErrorIs(t, err, storage.ErrAlreadyExists)

	require.NoError(t, s.UpdateWriteIntent(ctx, "wi1", model.WriteIntentCommitted))
	require.NoError(t, s.DeleteWriteIntent(ctx, "wi1"))
}

func TestIndexConcurrent(t *testing.T) {
	s := memory.NewIndexStore()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m := newMeta("shared", nil)
			_ = s.Put(ctx, m)
			_, _ = s.Get(ctx, "shared")
		}()
	}
	wg.Wait()
}

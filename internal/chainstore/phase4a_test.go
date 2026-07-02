package chainstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/chainstore"
)

// parseStagingID converts the opaque stagingID string returned by ResolveAndStage
// into the BlobID the backend uses as a key. Tests use this to inspect staging
// records directly; production code must treat stagingID as opaque.
func parseStagingID(t *testing.T, stagingID string) chainstore.BlobID {
	t.Helper()
	uid, err := uuid.Parse(stagingID)
	require.NoError(t, err, "stagingID must be a valid UUID")
	return chainstore.BlobID(uid)
}

// TestResolveAndStage_WithBlob checks that a non-nil requestBlob is staged and
// the correct turn chain is returned.
func TestResolveAndStage_WithBlob(t *testing.T) {
	ctx := context.Background()
	s, b := openMemStoreAndBackend(t, chainstore.Config{})

	require.NoError(t, s.Store(ctx, "r0", "", "", []byte("root-req")))
	require.NoError(t, s.Complete(ctx, "r0", "", []byte("root-resp")))

	stagingID, turns, err := s.ResolveAndStage(ctx, "r0", "", []byte("new-req"))
	require.NoError(t, err)
	assert.NotEmpty(t, stagingID)
	require.Len(t, turns, 1)
	assert.Equal(t, "r0", turns[0].ResponseID)

	// Staging record must be present in the backend.
	sid := parseStagingID(t, stagingID)
	staged, err := b.GetStagingNode(ctx, sid)
	require.NoError(t, err)
	assert.Equal(t, uint32(len("new-req")), staged.RequestBlobSize)
	assert.NotEqual(t, chainstore.BlobID{}, staged.RequestBlobID)
}

// TestResolveAndStage_NilBlob checks that a nil requestBlob still produces a valid stagingID.
func TestResolveAndStage_NilBlob(t *testing.T) {
	ctx := context.Background()
	s, b := openMemStoreAndBackend(t, chainstore.Config{})

	stagingID, turns, err := s.ResolveAndStage(ctx, "", "", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, stagingID)
	assert.Empty(t, turns)

	sid := parseStagingID(t, stagingID)
	staged, err := b.GetStagingNode(ctx, sid)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), staged.RequestBlobSize)
}

// TestResolveAndStage_EmptyPreviousResponseID returns empty turns for a new conversation.
func TestResolveAndStage_EmptyPreviousResponseID(t *testing.T) {
	ctx := context.Background()
	s := openMemStore(t, chainstore.Config{})

	stagingID, turns, err := s.ResolveAndStage(ctx, "", "", []byte("first-request"))
	require.NoError(t, err)
	assert.NotEmpty(t, stagingID)
	assert.Empty(t, turns)
}

// TestStoreWithStaging_RoundTrip checks the full staging → commit cycle:
// blobs round-trip correctly and the staging record is cleaned up.
func TestStoreWithStaging_RoundTrip(t *testing.T) {
	ctx := context.Background()
	s, b := openMemStoreAndBackend(t, chainstore.Config{})

	// Seed a prior turn so there is a real chain to walk.
	require.NoError(t, s.Store(ctx, "r0", "", "", []byte("req0")))
	require.NoError(t, s.Complete(ctx, "r0", "", []byte("resp0")))

	stagingID, _, err := s.ResolveAndStage(ctx, "r0", "", []byte("req1"))
	require.NoError(t, err)

	err = s.StoreWithStaging(ctx, stagingID, "r1", "r0", "", []byte("resp1"))
	require.NoError(t, err)

	// Final node must have both blobs.
	nodeID := chainstore.NodeIDFor("", "r1")
	node, err := b.GetNode(ctx, nodeID)
	require.NoError(t, err)
	assert.NotEqual(t, chainstore.BlobID{}, node.RequestBlobID, "RequestBlobID must be set")
	assert.NotEqual(t, chainstore.BlobID{}, node.ResponseBlobID, "ResponseBlobID must be set")
	assert.Equal(t, uint32(len("req1")), node.RequestBlobSize)
	assert.Equal(t, uint32(len("resp1")), node.ResponseBlobSize)

	// Staging record must be gone.
	sid := parseStagingID(t, stagingID)
	_, err = b.GetStagingNode(ctx, sid)
	assert.True(t, errors.Is(err, chainstore.ErrNotFound), "staging record must be deleted after commit")
}

// TestStoreWithStaging_StagingGone checks that blobs are retrievable via Resolve after commit.
func TestStoreWithStaging_BlobsAccessibleViaResolve(t *testing.T) {
	ctx := context.Background()
	s := openMemStore(t, chainstore.Config{})

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req1"))
	require.NoError(t, err)
	require.NoError(t, s.StoreWithStaging(ctx, stagingID, "r1", "", "", []byte("resp1")))

	turns, err := s.Resolve(ctx, "r1", "")
	require.NoError(t, err)
	require.Len(t, turns, 1)
	assert.Equal(t, []byte("req1"), turns[0].RequestBlob)
	assert.Equal(t, []byte("resp1"), turns[0].ResponseBlob)
}

// TestStoreWithStaging_UnknownStagingID returns ErrNotFound for a bogus ID.
func TestStoreWithStaging_UnknownStagingID(t *testing.T) {
	ctx := context.Background()
	s := openMemStore(t, chainstore.Config{})

	bogus := uuid.New().String()
	err := s.StoreWithStaging(ctx, bogus, "r1", "", "", []byte("resp"))
	assert.True(t, errors.Is(err, chainstore.ErrNotFound))
}

// TestStoreWithStaging_NoStaging checks that stagingID="" writes responseBlob directly.
func TestStoreWithStaging_NoStaging(t *testing.T) {
	ctx := context.Background()
	s := openMemStore(t, chainstore.Config{})

	err := s.StoreWithStaging(ctx, "", "r1", "", "", []byte("resp1"))
	require.NoError(t, err)

	turns, err := s.Resolve(ctx, "r1", "")
	require.NoError(t, err)
	require.Len(t, turns, 1)
	assert.Equal(t, []byte("resp1"), turns[0].ResponseBlob)
}

// TestDelete_Cascade checks that keepDescendants=false removes node and its children.
func TestDelete_Cascade(t *testing.T) {
	ctx := context.Background()
	s := openMemStore(t, chainstore.Config{})

	require.NoError(t, s.Store(ctx, "r0", "", "", []byte("root")))
	require.NoError(t, s.Store(ctx, "r1", "r0", "", []byte("child")))

	require.NoError(t, s.Delete(ctx, "r0", "", false))

	_, err := s.Resolve(ctx, "r0", "")
	assert.True(t, errors.Is(err, chainstore.ErrNotFound), "root must be gone")
	_, err = s.Resolve(ctx, "r1", "")
	assert.True(t, errors.Is(err, chainstore.ErrNotFound), "child must be gone (cascade)")
}

// TestDelete_KeepDescendants checks that keepDescendants=true removes only the named node.
func TestDelete_KeepDescendants(t *testing.T) {
	ctx := context.Background()
	s := openMemStore(t, chainstore.Config{})

	require.NoError(t, s.Store(ctx, "r0", "", "", []byte("root")))
	require.NoError(t, s.Store(ctx, "r1", "r0", "", []byte("child")))

	require.NoError(t, s.Delete(ctx, "r0", "", true))

	_, err := s.Resolve(ctx, "r0", "")
	assert.True(t, errors.Is(err, chainstore.ErrNotFound), "root must be gone")

	// Child still exists but its chain walk fails because the parent is missing.
	// That surfaces as ErrChainExpired (ancestor capacity-evicted path).
	_, err = s.Resolve(ctx, "r1", "")
	assert.True(t, errors.Is(err, chainstore.ErrChainExpired) || errors.Is(err, chainstore.ErrNotFound),
		"child chain walk must fail after parent deletion: %v", err)
}

// TestPublicFromNode_Fields verifies that only the five safe fields are populated
// and that ExpiresAt is computed correctly.
func TestPublicFromNode_Fields(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	node := chainstore.Node{
		Version:        3,
		CreatedAt:      now.Unix(),
		LastAccessUnix: now.Unix(),
		Depth:          5,
		Status:         chainstore.NodeStatusFailed,
	}

	ttl := 2 * time.Hour
	pub := chainstore.PublicFromNode(node, ttl)

	assert.Equal(t, node.CreatedAt, pub.CreatedAt)
	assert.Equal(t, node.LastAccessUnix+int64(ttl.Seconds()), pub.ExpiresAt)
	assert.Equal(t, node.Depth, pub.Depth)
	assert.Equal(t, node.Status, pub.Status)
	assert.Equal(t, node.Version, pub.Version)
}

// TestPublicFromNode_NoTTL checks that ExpiresAt is zero when TTL is disabled.
func TestPublicFromNode_NoTTL(t *testing.T) {
	node := chainstore.Node{LastAccessUnix: time.Now().Unix()}
	pub := chainstore.PublicFromNode(node, 0)
	assert.Equal(t, int64(0), pub.ExpiresAt)
}

// TestRetrieve_NotImplemented checks the stub returns ErrNotImplemented.
func TestRetrieve_NotImplemented(t *testing.T) {
	ctx := context.Background()
	s := openMemStore(t, chainstore.Config{})
	_, err := s.Retrieve(ctx, "r0", "")
	assert.True(t, errors.Is(err, chainstore.ErrNotImplemented))
}

// TestPing_ReachableBackend checks Ping returns nil for a healthy backend.
func TestPing_ReachableBackend(t *testing.T) {
	ctx := context.Background()
	s := openMemStore(t, chainstore.Config{})
	assert.NoError(t, s.Ping(ctx))
}

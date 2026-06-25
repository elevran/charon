package telemetry_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/telemetry"
)

func TestInitDisabled(t *testing.T) {
	tp, err := telemetry.Init(context.Background(), "svc", "")
	require.NoError(t, err)
	assert.Nil(t, tp, "should return nil when exporter URL is empty")
}

func TestInitInvalidURL(t *testing.T) {
	// An unreachable endpoint: Init should succeed (connection is lazy for HTTP exporter)
	// but return a non-nil provider.
	tp, err := telemetry.Init(context.Background(), "svc", "http://127.0.0.1:0")
	require.NoError(t, err)
	require.NotNil(t, tp)
	// Clean up the provider.
	require.NoError(t, tp.Shutdown(context.Background()))
}

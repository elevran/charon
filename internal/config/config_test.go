package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/config"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := config.Load("")
	require.NoError(t, err)
	// Proxy defaults
	assert.False(t, cfg.Proxy.Enabled, "proxy must be off by default")
	assert.Equal(t, ":8080", cfg.Proxy.Listen)
	assert.Equal(t, "http://127.0.0.1:8081", cfg.Proxy.CharonURL)
	assert.Equal(t, "http://localhost:11434", cfg.Proxy.Inference.BaseURL)
	assert.Equal(t, 120, cfg.Proxy.Inference.TimeoutSeconds)
	assert.Equal(t, 65536, cfg.Proxy.Inference.StoreBufferBytes)
	// Charon defaults
	assert.Equal(t, ":8081", cfg.Charon.Listen)
	assert.Equal(t, "memory", cfg.Charon.Storage.Backend)
	assert.Equal(t, 10, cfg.Charon.Storage.CheckpointInterval)
	assert.Equal(t, 30, cfg.Charon.Storage.TTLDays)
	assert.Equal(t, 5*time.Minute, cfg.Charon.Storage.WriteIntentStaleThreshold)
	assert.Equal(t, time.Hour, cfg.Charon.Workers.TTLInterval)
	assert.Equal(t, 5*time.Minute, cfg.Charon.Workers.RecoveryInterval)
}

func TestCharonURLDerivedFromListen(t *testing.T) {
	// When charon.listen is set and proxy.charon_url is not, charon_url is auto-derived.
	cfg, err := config.Load("testdata/config.yaml")
	require.NoError(t, err)
	assert.Equal(t, "http://127.0.0.1:9090", cfg.Proxy.CharonURL)
}

func TestLoadFromFile(t *testing.T) {
	cfg, err := config.Load("testdata/config.yaml")
	require.NoError(t, err)
	assert.Equal(t, ":0", cfg.Proxy.Listen)
	assert.Equal(t, 10, cfg.Charon.Storage.CheckpointInterval)
}

func TestLoadStrictRejectsUnknownFields(t *testing.T) {
	_, err := config.Load("testdata/invalid.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse config")
}

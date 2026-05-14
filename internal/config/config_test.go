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
	assert.Equal(t, ":8080", cfg.Server.Listen)
	assert.Equal(t, 10, cfg.Storage.CheckpointInterval)
	assert.Equal(t, 30, cfg.Storage.TTLDays)
	assert.Equal(t, 5*time.Minute, cfg.Storage.WriteIntentStaleThreshold)
	assert.Equal(t, time.Hour, cfg.Workers.TTLInterval)
	assert.Equal(t, 5*time.Minute, cfg.Workers.RecoveryInterval)
}

func TestLoadFromFile(t *testing.T) {
	cfg, err := config.Load("testdata/config.yaml")
	require.NoError(t, err)
	assert.Equal(t, ":0", cfg.Server.Listen)
	assert.Equal(t, 10, cfg.Storage.CheckpointInterval)
}

func TestLoadStrictRejectsUnknownFields(t *testing.T) {
	_, err := config.Load("testdata/invalid.yaml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse config")
}

package main

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/config"
)

func TestConfigLoading(t *testing.T) {
	cfg, err := config.Load("")
	require.NoError(t, err)
	require.Equal(t, ":8080", cfg.Server.Listen)
	require.Equal(t, 10, cfg.Storage.CheckpointInterval)
}

func TestListenerBinds(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	_ = l.Close()
}

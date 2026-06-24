package main

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/config"
)

func TestConfigDefaults(t *testing.T) {
	opts := config.NewServerOptions()
	require.False(t, opts.ProxyEnabled)
	require.Equal(t, ":8080", opts.ProxyListen)
	require.Equal(t, 10, opts.Storage.CheckpointInterval)
}

func TestListenerBinds(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	_ = l.Close()
}

package main

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/charonconfig"
)

func TestConfigDefaults(t *testing.T) {
	opts := charonconfig.NewOptions()
	require.Equal(t, ":8081", opts.Listen)
	require.Equal(t, 30, opts.TTLDays)
}

func TestListenerBinds(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	_ = l.Close()
}

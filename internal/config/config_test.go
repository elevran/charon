package config_test

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/config"
)

func TestLoadDefaults(t *testing.T) {
	opts := config.NewServerOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse(nil))
	require.NoError(t, opts.Complete(fs))

	// Proxy defaults
	assert.False(t, opts.ProxyEnabled, "proxy must be off by default")
	assert.Equal(t, ":8080", opts.ProxyListen)
	assert.Equal(t, "http://127.0.0.1:8081", opts.ProxyCharonURL)
	assert.Equal(t, "http://localhost:11434", opts.ProxyBackend)
	assert.Equal(t, 120, opts.InferenceTimeoutSeconds)
	assert.Equal(t, 65536, opts.InferenceStoreBufferBytes)
	// Charon defaults
	assert.Equal(t, ":8081", opts.CharonListen)
	assert.Equal(t, "./data", opts.DataDir)
	assert.Equal(t, 30, opts.TTLDays)
	assert.Equal(t, time.Hour, opts.WorkerTTLInterval)
}

func TestCharonURLDerivedFromListen(t *testing.T) {
	// When charon.listen is set in the file and proxy.charon_url is not,
	// ProxyCharonURL is auto-derived from the file's charon.listen.
	opts := config.NewServerOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml"}))
	require.NoError(t, opts.Complete(fs))
	assert.Equal(t, "http://127.0.0.1:9090", opts.ProxyCharonURL)
}

func TestLoadFromFile(t *testing.T) {
	opts := config.NewServerOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, ":0", opts.ProxyListen)
	assert.Equal(t, 30, opts.TTLDays)
}

func TestLoadStrictRejectsUnknownFields(t *testing.T) {
	opts := config.NewServerOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/invalid.yaml"}))
	err := opts.Complete(fs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse config")
}

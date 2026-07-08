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
	charon := config.NewCharonOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	charon.AddFlags(fs)
	require.NoError(t, fs.Parse(nil))
	require.NoError(t, charon.Complete(fs))

	assert.Equal(t, ":8081", charon.Listen)
	assert.Equal(t, "./data", charon.DataDir)
	assert.Equal(t, 30, charon.TTLDays)
	assert.Equal(t, time.Hour, charon.WorkerTTLInterval)
	assert.Equal(t, "charon", charon.Telemetry.ServiceName)

	proxy := config.NewProxyOptions()
	fs2 := flag.NewFlagSet("test", flag.ContinueOnError)
	proxy.AddFlags(fs2)
	require.NoError(t, fs2.Parse(nil))
	require.NoError(t, proxy.Complete(fs2))

	assert.Equal(t, ":8080", proxy.Listen)
	assert.Equal(t, "http://localhost:11434", proxy.Backend)
	assert.Equal(t, "http://127.0.0.1:8081", proxy.CharonURL)
	assert.Equal(t, 120, proxy.TimeoutSeconds)
	assert.Equal(t, "charon-proxy", proxy.Telemetry.ServiceName)
}

func TestCharonURLDerivedFromListen(t *testing.T) {
	// When charon.listen is set in the file and proxy.charon_url is not,
	// CharonURL is auto-derived from the file's charon.listen.
	proxy := config.NewProxyOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	proxy.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml"}))
	require.NoError(t, proxy.Complete(fs))
	assert.Equal(t, "http://127.0.0.1:9090", proxy.CharonURL)
}

func TestLoadFromFile(t *testing.T) {
	proxy := config.NewProxyOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	proxy.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml"}))
	require.NoError(t, proxy.Complete(fs))
	assert.Equal(t, ":0", proxy.Listen)

	charon := config.NewCharonOptions()
	fs2 := flag.NewFlagSet("test", flag.ContinueOnError)
	charon.AddFlags(fs2)
	require.NoError(t, fs2.Parse([]string{"--config", "testdata/config.yaml"}))
	require.NoError(t, charon.Complete(fs2))
	assert.Equal(t, 30, charon.TTLDays)
}

func TestLoadStrictRejectsUnknownFields(t *testing.T) {
	charon := config.NewCharonOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	charon.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/invalid.yaml"}))
	err := charon.Complete(fs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse config")
}

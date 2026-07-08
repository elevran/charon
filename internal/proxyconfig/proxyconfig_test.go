package proxyconfig_test

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/proxyconfig"
)

func TestProxyOptionsDefaultsWithNoConfig(t *testing.T) {
	opts := proxyconfig.NewProxyOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse(nil))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, ":8080", opts.Listen)
	assert.Equal(t, "http://localhost:11434", opts.Backend)
	assert.Equal(t, "http://127.0.0.1:8081", opts.CharonURL)
	assert.Equal(t, 120, opts.TimeoutSeconds)
	assert.Equal(t, "charon-proxy", opts.Telemetry.ServiceName)
}

func TestProxyOptionsCompleteLoadsFile(t *testing.T) {
	opts := proxyconfig.NewProxyOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, ":0", opts.Listen)
	// charon_url not set in testdata/config.yaml → auto-derived from charon.listen ":9090"
	assert.Equal(t, "http://127.0.0.1:9090", opts.CharonURL)
}

func TestProxyOptionsCLIOverridesFile(t *testing.T) {
	opts := proxyconfig.NewProxyOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml", "--listen", ":5555"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, ":5555", opts.Listen, "CLI flag must take precedence over config file")
}

func TestProxyOptionsBackendCLIOverridesFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	err := os.WriteFile(cfgPath, []byte("proxy:\n  inference:\n    base_url: http://file-backend:11434\n"), 0600)
	require.NoError(t, err)

	opts := proxyconfig.NewProxyOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", cfgPath, "--backend", "http://cli-backend:11434"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, "http://cli-backend:11434", opts.Backend, "CLI --backend must take precedence over config file")
}

func TestProxyOptionsCharonURLCLIOverridesFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	err := os.WriteFile(cfgPath, []byte("proxy:\n  charon_url: http://file-charon:8081\n"), 0600)
	require.NoError(t, err)

	opts := proxyconfig.NewProxyOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", cfgPath, "--charon-url", "http://cli-charon:9999"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, "http://cli-charon:9999", opts.CharonURL, "CLI --charon-url must take precedence over config file")
}

func TestProxyOptionsValidateOK(t *testing.T) {
	opts := proxyconfig.NewProxyOptions()
	require.NoError(t, opts.Validate())
}

func TestProxyOptionsValidateEmptyBackendFails(t *testing.T) {
	opts := proxyconfig.NewProxyOptions()
	opts.Backend = ""
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backend")
}

func TestCharonURLDerivedFromListen(t *testing.T) {
	opts := proxyconfig.NewProxyOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml"}))
	require.NoError(t, opts.Complete(fs))
	assert.Equal(t, "http://127.0.0.1:9090", opts.CharonURL)
}

func TestLoadFromFile(t *testing.T) {
	proxy := proxyconfig.NewProxyOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	proxy.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml"}))
	require.NoError(t, proxy.Complete(fs))
	assert.Equal(t, ":0", proxy.Listen)
}

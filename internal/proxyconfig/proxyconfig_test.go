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

// configFromBytes writes yaml to a temp file and returns the path.
func configFromBytes(t *testing.T, yaml []byte) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(p, yaml, 0600))
	return p
}

func TestProxyOptionsDefaultsWithNoConfig(t *testing.T) {
	opts := proxyconfig.NewOptions()
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
	yaml := []byte("proxy:\n  listen: \":0\"\ncharon:\n  listen: \":9090\"\n")
	cfg := configFromBytes(t, yaml)
	opts := proxyconfig.NewOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", cfg}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, ":0", opts.Listen)
	// charon_url not set → auto-derived from charon.listen ":9090"
	assert.Equal(t, "http://127.0.0.1:9090", opts.CharonURL)
}

func TestProxyOptionsCLIOverridesFile(t *testing.T) {
	yaml := []byte("proxy:\n  listen: \":0\"\n")
	cfg := configFromBytes(t, yaml)
	opts := proxyconfig.NewOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", cfg, "--listen", ":5555"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, ":5555", opts.Listen, "CLI flag must take precedence over config file")
}

func TestProxyOptionsBackendCLIOverridesFile(t *testing.T) {
	yaml := []byte("proxy:\n  inference:\n    base_url: http://file-backend:11434\n")
	cfg := configFromBytes(t, yaml)
	opts := proxyconfig.NewOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", cfg, "--backend", "http://cli-backend:11434"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, "http://cli-backend:11434", opts.Backend, "CLI --backend must take precedence over config file")
}

func TestProxyOptionsCharonURLCLIOverridesFile(t *testing.T) {
	yaml := []byte("proxy:\n  charon_url: http://file-charon:8081\n")
	cfg := configFromBytes(t, yaml)
	opts := proxyconfig.NewOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", cfg, "--charon-url", "http://cli-charon:9999"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, "http://cli-charon:9999", opts.CharonURL, "CLI --charon-url must take precedence over config file")
}

func TestProxyOptionsValidateOK(t *testing.T) {
	opts := proxyconfig.NewOptions()
	require.NoError(t, opts.Validate())
}

func TestProxyOptionsValidateEmptyBackendFails(t *testing.T) {
	opts := proxyconfig.NewOptions()
	opts.Backend = ""
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backend")
}

func TestCharonURLDerivedFromListen(t *testing.T) {
	yaml := []byte("proxy:\n  listen: \":0\"\ncharon:\n  listen: \":9090\"\n")
	cfg := configFromBytes(t, yaml)
	opts := proxyconfig.NewOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", cfg}))
	require.NoError(t, opts.Complete(fs))
	assert.Equal(t, "http://127.0.0.1:9090", opts.CharonURL)
}

func TestLoadStrictRejectsUnknownFields(t *testing.T) {
	tests := []struct {
		name string
		yaml []byte
	}{
		{"top-level unknown key", []byte("unknown_field: true\n")},
		{"unknown key under proxy", []byte("proxy:\n  bad_key: 1\n")},
		{"unknown key under inference", []byte("proxy:\n  inference:\n    not_a_field: true\n")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := configFromBytes(t, tc.yaml)
			opts := proxyconfig.NewOptions()
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			opts.AddFlags(fs)
			require.NoError(t, fs.Parse([]string{"--config", cfg}))
			err := opts.Complete(fs)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "parse config")
		})
	}
}

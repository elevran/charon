package config_test

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/config"
)

// ─── ServerOptions tests ──────────────────────────────────────────────────────

func TestServerOptionsDefaultsWithNoConfig(t *testing.T) {
	opts := config.NewServerOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse(nil))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, ":8081", opts.CharonListen)
	assert.Equal(t, ":8080", opts.ProxyListen)
	assert.False(t, opts.ProxyEnabled)
	assert.Equal(t, "http://localhost:11434", opts.ProxyBackend)
	assert.Equal(t, "http://127.0.0.1:8081", opts.ProxyCharonURL, "ProxyCharonURL must be auto-derived")
	assert.Equal(t, "./data", opts.Storage.DataDir)
	assert.Equal(t, 30, opts.Storage.TTLDays)
	assert.Equal(t, time.Hour, opts.WorkerTTLInterval)
}

func TestServerOptionsCompleteLoadsFile(t *testing.T) {
	opts := config.NewServerOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	// Pass --config pointing at the existing test fixture.
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml"}))
	require.NoError(t, opts.Complete(fs))

	// config.yaml sets proxy.listen: ":0" and charon.listen: ":9090"
	assert.Equal(t, ":0", opts.ProxyListen)
	assert.Equal(t, ":9090", opts.CharonListen)
	// ProxyCharonURL should be auto-derived from charon.listen ":9090"
	assert.Equal(t, "http://127.0.0.1:9090", opts.ProxyCharonURL)
}

func TestServerOptionsCLIOverridesFile(t *testing.T) {
	opts := config.NewServerOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	// --listen overrides charon.listen from the file (":9090")
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml", "--listen", ":7777"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, ":7777", opts.CharonListen, "CLI flag must take precedence over config file")
}

func TestServerOptionsValidateOK(t *testing.T) {
	opts := config.NewServerOptions()
	require.NoError(t, opts.Validate())
}

func TestServerOptionsValidateProxyEnabledRequiresBackend(t *testing.T) {
	opts := config.NewServerOptions()
	opts.ProxyEnabled = true
	opts.ProxyBackend = ""
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "proxy backend")
}

func TestServerOptionsValidateProxyEnabledWithBackendOK(t *testing.T) {
	opts := config.NewServerOptions()
	opts.ProxyEnabled = true
	// ProxyBackend defaults to "http://localhost:11434" — should be OK.
	require.NoError(t, opts.Validate())
}

func TestServerOptionsDataDirCLIOverridesFile(t *testing.T) {
	// Write a temp config with data_dir set
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	err := os.WriteFile(cfgPath, []byte("charon:\n  storage:\n    data_dir: /tmp/test-data\n"), 0600)
	require.NoError(t, err)

	opts := config.NewServerOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	// CLI explicitly sets data dir — must win over file.
	require.NoError(t, fs.Parse([]string{"--config", cfgPath, "--storage-data-dir", "./my-data"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, "./my-data", opts.Storage.DataDir)
}

// ─── ReconcileOptions tests ───────────────────────────────────────────────────

func TestReconcileOptionsDefaultsWithNoConfig(t *testing.T) {
	opts := config.NewReconcileOptions()
	fs := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse(nil))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, "./data", opts.Storage.DataDir)
	assert.Equal(t, 30, opts.Storage.TTLDays)
}

func TestReconcileOptionsValidateOK(t *testing.T) {
	opts := config.NewReconcileOptions()
	require.NoError(t, opts.Validate())
}

func TestReconcileOptionsCLIOverridesFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	err := os.WriteFile(cfgPath, []byte("charon:\n  storage:\n    data_dir: /tmp/from-file\n"), 0600)
	require.NoError(t, err)

	opts := config.NewReconcileOptions()
	fs := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", cfgPath, "--storage-data-dir", "./my-data"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, "./my-data", opts.Storage.DataDir)
}

func TestReconcileOptionsCompleteLoadsFile(t *testing.T) {
	opts := config.NewReconcileOptions()
	fs := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml"}))
	require.NoError(t, opts.Complete(fs))

	// config.yaml sets charon.listen: ":9090" but no ttl override, defaults apply.
	assert.Equal(t, 30, opts.Storage.TTLDays)
}

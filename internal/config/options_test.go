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
	assert.Equal(t, "memory", opts.Storage.Backend)
	assert.Equal(t, "./data", opts.Storage.DataDir)
	assert.Equal(t, 30, opts.Storage.TTLDays)
	assert.Equal(t, 5*time.Minute, opts.Storage.WriteIntentStaleThreshold)
	assert.Equal(t, time.Hour, opts.WorkerTTLInterval)
	assert.Equal(t, 5*time.Minute, opts.WorkerRecoveryInterval)
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

func TestServerOptionsValidateMemoryBackendOK(t *testing.T) {
	opts := config.NewServerOptions()
	// memory backend with no DSN is valid.
	require.NoError(t, opts.Validate())
}

func TestServerOptionsValidatePostgresRequiresDSN(t *testing.T) {
	opts := config.NewServerOptions()
	opts.Storage.Backend = "postgres"
	// No DSN set — must fail validation.
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres.dsn")
}

func TestServerOptionsValidatePostgresS3RequiresDSN(t *testing.T) {
	opts := config.NewServerOptions()
	opts.Storage.Backend = "postgres+s3"
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres.dsn")
}

func TestServerOptionsValidatePostgresWithDSNOK(t *testing.T) {
	opts := config.NewServerOptions()
	opts.Storage.Backend = "postgres"
	opts.Storage.Postgres.DSN = "postgres://user:pass@localhost:5432/charon"
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

func TestServerOptionsStorageBackendCLIOverridesFile(t *testing.T) {
	// Write a temp config with backend: sqlite
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	err := os.WriteFile(cfgPath, []byte("charon:\n  storage:\n    backend: sqlite\n"), 0600)
	require.NoError(t, err)

	opts := config.NewServerOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	// CLI explicitly requests memory — must win over file's sqlite.
	require.NoError(t, fs.Parse([]string{"--config", cfgPath, "--storage-backend", "memory"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, "memory", opts.Storage.Backend)
}

// ─── ReconcileOptions tests ───────────────────────────────────────────────────

func TestReconcileOptionsDefaultsWithNoConfig(t *testing.T) {
	opts := config.NewReconcileOptions()
	fs := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse(nil))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, "memory", opts.Storage.Backend)
	assert.Equal(t, "./data", opts.Storage.DataDir)
}

func TestReconcileOptionsValidatePostgresRequiresDSN(t *testing.T) {
	opts := config.NewReconcileOptions()
	opts.Storage.Backend = "postgres"
	err := opts.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres.dsn")
}

func TestReconcileOptionsCLIOverridesFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	err := os.WriteFile(cfgPath, []byte("charon:\n  storage:\n    backend: sqlite\n"), 0600)
	require.NoError(t, err)

	opts := config.NewReconcileOptions()
	fs := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", cfgPath, "--storage-backend", "memory"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, "memory", opts.Storage.Backend)
}

func TestReconcileOptionsCompleteLoadsFile(t *testing.T) {
	opts := config.NewReconcileOptions()
	fs := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml"}))
	require.NoError(t, opts.Complete(fs))

}

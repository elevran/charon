package charonconfig_test

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/charonconfig"
)

// configFromBytes writes yaml to a temp file and returns the path.
func configFromBytes(t *testing.T, yaml []byte) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(p, yaml, 0600))
	return p
}

func TestCharonOptionsDefaultsWithNoConfig(t *testing.T) {
	opts := charonconfig.NewOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse(nil))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, ":8081", opts.Listen)
	assert.Equal(t, "./data", opts.DataDir)
	assert.Equal(t, 30, opts.TTLDays)
	assert.Equal(t, time.Hour, opts.WorkerTTLInterval)
	assert.Equal(t, "charon", opts.Telemetry.ServiceName)
}

func TestCharonOptionsCompleteLoadsFile(t *testing.T) {
	yaml := []byte(`
charon:
  listen: ":9090"
  storage:
    data_dir: ./data
`)
	cfg := configFromBytes(t, yaml)
	opts := charonconfig.NewOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", cfg}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, ":9090", opts.Listen)
	assert.Equal(t, "./data", opts.DataDir)
}

func TestCharonOptionsCLIOverridesFile(t *testing.T) {
	yaml := []byte("charon:\n  listen: \":9090\"\n")
	cfg := configFromBytes(t, yaml)
	opts := charonconfig.NewOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", cfg, "--listen", ":7777"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, ":7777", opts.Listen, "CLI flag must take precedence over config file")
}

func TestCharonOptionsValidateOK(t *testing.T) {
	opts := charonconfig.NewOptions()
	require.NoError(t, opts.Validate())
}

func TestCharonOptionsDataDirCLIOverridesFile(t *testing.T) {
	yaml := []byte("charon:\n  storage:\n    data_dir: /tmp/test-data\n")
	cfg := configFromBytes(t, yaml)
	opts := charonconfig.NewOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", cfg, "--storage-data-dir", "./my-data"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, "./my-data", opts.DataDir)
}

func TestLoadStrictRejectsUnknownFields(t *testing.T) {
	tests := []struct {
		name string
		yaml []byte
	}{
		{"top-level unknown key", []byte("unknown_field: true\n")},
		{"unknown key under charon", []byte("charon:\n  not_a_field: true\n")},
		{"unknown key under storage", []byte("charon:\n  storage:\n    bad_key: 1\n")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := configFromBytes(t, tc.yaml)
			opts := charonconfig.NewOptions()
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			opts.AddFlags(fs)
			require.NoError(t, fs.Parse([]string{"--config", cfg}))
			err := opts.Complete(fs)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "parse config")
		})
	}
}

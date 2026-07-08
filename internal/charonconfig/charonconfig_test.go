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

func TestCharonOptionsDefaultsWithNoConfig(t *testing.T) {
	opts := charonconfig.NewCharonOptions()
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
	opts := charonconfig.NewCharonOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, ":9090", opts.Listen)
	assert.Equal(t, "./data", opts.DataDir)
}

func TestCharonOptionsCLIOverridesFile(t *testing.T) {
	opts := charonconfig.NewCharonOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml", "--listen", ":7777"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, ":7777", opts.Listen, "CLI flag must take precedence over config file")
}

func TestCharonOptionsValidateOK(t *testing.T) {
	opts := charonconfig.NewCharonOptions()
	require.NoError(t, opts.Validate())
}

func TestCharonOptionsDataDirCLIOverridesFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "cfg.yaml")
	err := os.WriteFile(cfgPath, []byte("charon:\n  storage:\n    data_dir: /tmp/test-data\n"), 0600)
	require.NoError(t, err)

	opts := charonconfig.NewCharonOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", cfgPath, "--storage-data-dir", "./my-data"}))
	require.NoError(t, opts.Complete(fs))

	assert.Equal(t, "./my-data", opts.DataDir)
}

func TestLoadDefaults(t *testing.T) {
	charon := charonconfig.NewCharonOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	charon.AddFlags(fs)
	require.NoError(t, fs.Parse(nil))
	require.NoError(t, charon.Complete(fs))

	assert.Equal(t, ":8081", charon.Listen)
	assert.Equal(t, "./data", charon.DataDir)
	assert.Equal(t, 30, charon.TTLDays)
	assert.Equal(t, time.Hour, charon.WorkerTTLInterval)
	assert.Equal(t, "charon", charon.Telemetry.ServiceName)
}

func TestLoadFromFile(t *testing.T) {
	charon := charonconfig.NewCharonOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	charon.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/config.yaml"}))
	require.NoError(t, charon.Complete(fs))
	assert.Equal(t, 30, charon.TTLDays)
}

func TestLoadStrictRejectsUnknownFields(t *testing.T) {
	charon := charonconfig.NewCharonOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	charon.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/invalid.yaml"}))
	err := charon.Complete(fs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse config")
}

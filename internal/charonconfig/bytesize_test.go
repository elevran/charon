package charonconfig_test

import (
	"encoding/json"
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/charonconfig"
)

func unmarshalByteSize(t *testing.T, input string) charonconfig.ByteSize {
	t.Helper()
	var b charonconfig.ByteSize
	require.NoError(t, json.Unmarshal([]byte(input), &b))
	return b
}

func TestByteSizeUnmarshal(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"0", 0},
		{"1024", 1024},
		{"1048576", 1048576},
		{`"0"`, 0},
		{`"1024"`, 1024},
		{`"512B"`, 512},
		{`"512b"`, 512},
		{`"1KB"`, 1024},
		{`"2kb"`, 2048},
		{`"1 KB"`, 1024},
		{`"10MB"`, 10 * 1024 * 1024},
		{`"10mb"`, 10 * 1024 * 1024},
		{`"2GB"`, 2 * 1024 * 1024 * 1024},
		{`"2gb"`, 2 * 1024 * 1024 * 1024},
		{`"1Mb"`, 1024 * 1024},
		{`"1gB"`, 1024 * 1024 * 1024},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := unmarshalByteSize(t, tc.input)
			assert.Equal(t, charonconfig.ByteSize(tc.want), got)
		})
	}
}

func TestByteSizeUnmarshalErrors(t *testing.T) {
	tests := []struct {
		input   string
		errFrag string
	}{
		{`"10TB"`, "unknown unit"},
		{`"abc"`, "no numeric value"},
		{`"-1"`, "negative"},
		{`"-1MB"`, "negative"},
		{`true`, "expected number or string"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			var b charonconfig.ByteSize
			err := json.Unmarshal([]byte(tc.input), &b)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errFrag)
		})
	}
}

func TestByteSizeInConfig(t *testing.T) {
	opts := charonconfig.NewCharonOptions()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	opts.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{"--config", "testdata/bytesize.yaml"}))
	require.NoError(t, opts.Complete(fs))
	assert.Equal(t, charonconfig.ByteSize(10*1024*1024), opts.MaxPayload)
}

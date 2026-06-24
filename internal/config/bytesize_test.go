package config_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/config"
)

func unmarshalByteSize(t *testing.T, input string) config.ByteSize {
	t.Helper()
	var b config.ByteSize
	require.NoError(t, json.Unmarshal([]byte(input), &b))
	return b
}

func TestByteSizeUnmarshal(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		// bare integer
		{"0", 0},
		{"1024", 1024},
		{"1048576", 1048576},
		// string: no unit → bytes
		{`"0"`, 0},
		{`"1024"`, 1024},
		// B suffix
		{`"512B"`, 512},
		{`"512b"`, 512},
		// KB
		{`"1KB"`, 1024},
		{`"2kb"`, 2048},
		{`"1 KB"`, 1024},
		// MB
		{`"10MB"`, 10 * 1024 * 1024},
		{`"10mb"`, 10 * 1024 * 1024},
		// GB
		{`"2GB"`, 2 * 1024 * 1024 * 1024},
		{`"2gb"`, 2 * 1024 * 1024 * 1024},
		// mixed case
		{`"1Mb"`, 1024 * 1024},
		{`"1gB"`, 1024 * 1024 * 1024},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := unmarshalByteSize(t, tc.input)
			assert.Equal(t, config.ByteSize(tc.want), got)
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
			var b config.ByteSize
			err := json.Unmarshal([]byte(tc.input), &b)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errFrag)
		})
	}
}

func TestByteSizeInConfig(t *testing.T) {
	cfg, err := config.Load("testdata/bytesize.yaml")
	require.NoError(t, err)
	assert.Equal(t, config.ByteSize(10*1024*1024), cfg.Charon.Storage.MaxPayload)
}

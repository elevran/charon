package bytesize_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/bytesize"
)

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
			var b bytesize.ByteSize
			require.NoError(t, json.Unmarshal([]byte(tc.input), &b))
			assert.Equal(t, bytesize.ByteSize(tc.want), b)
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
			var b bytesize.ByteSize
			err := json.Unmarshal([]byte(tc.input), &b)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errFrag)
		})
	}
}

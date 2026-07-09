// Package bytesize provides a configurable byte-size type that unmarshals
// from either a plain integer (bytes) or a string with an optional unit suffix
// (B, KB, MB, GB), plus named binary multipliers (KiB, MiB, GiB) so call sites
// can stop writing "1 << 20".
package bytesize

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Binary multipliers. K = 1024 to match the unit parsing below.
const (
	KiB int64 = 1 << 10
	MiB int64 = 1 << 20
	GiB int64 = 1 << 30
	TiB int64 = 1 << 40
)

// ByteSize is an int64 that unmarshals from either a plain integer (bytes) or
// a string with an optional unit suffix: B, KB, MB, GB. K=1024.
type ByteSize int64

var unitMultipliers = map[string]int64{
	"b":  1,
	"kb": 1024,
	"mb": 1024 * 1024,
	"gb": 1024 * 1024 * 1024,
}

// UnmarshalJSON accepts a JSON number (raw bytes) or string (number with
// optional unit suffix).
func (b *ByteSize) UnmarshalJSON(data []byte) error {
	var n int64
	if err := json.Unmarshal(data, &n); err == nil {
		*b = ByteSize(n)
		return nil
	}

	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("bytesize: expected number or string, got %s", data)
	}

	s = strings.TrimSpace(s)
	lower := strings.ToLower(s)

	i := 0
	for i < len(s) && (s[i] == '-' || s[i] == '+' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	if i == 0 {
		return fmt.Errorf("bytesize: no numeric value in %q", s)
	}

	digits := s[:i]
	unit := strings.TrimSpace(strings.ToLower(lower[i:]))
	if unit == "" {
		unit = "b"
	}

	mult, ok := unitMultipliers[unit]
	if !ok {
		return fmt.Errorf("bytesize: unknown unit %q in %q (use B, KB, MB, or GB)", unit, s)
	}

	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return fmt.Errorf("bytesize: invalid number in %q: %w", s, err)
	}
	if n < 0 {
		return fmt.Errorf("bytesize: negative size %q", s)
	}

	*b = ByteSize(n * mult)
	return nil
}

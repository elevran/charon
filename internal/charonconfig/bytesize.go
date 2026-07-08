package charonconfig

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// byteSizeType is the concrete type; ByteSize aliases it so tests can reference it.
type byteSizeType int64

var unitMultipliers = map[string]int64{
	"b":  1,
	"kb": 1024,
	"mb": 1024 * 1024,
	"gb": 1024 * 1024 * 1024,
}

func (b *byteSizeType) UnmarshalJSON(data []byte) error {
	var n int64
	if err := json.Unmarshal(data, &n); err == nil {
		*b = byteSizeType(n)
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

	*b = byteSizeType(n * mult)
	return nil
}

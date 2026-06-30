package pebble

import (
	"encoding/binary"
	"io"

	gogopebble "github.com/cockroachdb/pebble"
)

// statsMerger implements pebble.ValueMerger for the stats key.
// It accumulates two signed int64 counters: entry_count and byte_count.
// Each MERGE operand is a 16-byte big-endian record:
//
//	[0:8]  int64 entry delta
//	[8:16] int64 byte delta
type statsMerger struct {
	entries int64
	bytes   int64
}

func (m *statsMerger) add(value []byte) {
	if len(value) < 16 {
		return
	}
	m.entries += int64(binary.BigEndian.Uint64(value[0:8]))
	m.bytes += int64(binary.BigEndian.Uint64(value[8:16]))
}

func (m *statsMerger) MergeNewer(value []byte) error {
	m.add(value)
	return nil
}

func (m *statsMerger) MergeOlder(value []byte) error {
	m.add(value)
	return nil
}

func (m *statsMerger) Finish(_ bool) ([]byte, io.Closer, error) {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(m.entries))
	binary.BigEndian.PutUint64(buf[8:16], uint64(m.bytes))
	return buf[:], nil, nil
}

// StatsMerger is the pebble.Merger that must be configured in pebble.Options
// for the stats MERGE key to accumulate correctly across compactions and reads.
var StatsMerger = &gogopebble.Merger{
	Name: "chainstore.stats.v1",
	Merge: func(key, value []byte) (gogopebble.ValueMerger, error) {
		m := &statsMerger{}
		m.add(value)
		return m, nil
	},
}

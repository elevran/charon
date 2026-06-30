package pebble

import (
	gogopebble "github.com/cockroachdb/pebble"
	"github.com/elevran/charon/pkg/chainstore"
)

// Open creates a pebble.Backend at dir, wires it into cfg, and returns a
// fully-started *chainstore.Store. It is the standard entry point for production use.
// Pass dir="" with vfs.NewMem() in Options.FS for in-memory use in tests.
// opts may be nil; StatsMerger is always set to enable stats accumulation.
func Open(dir string, opts *gogopebble.Options, cfg chainstore.Config) (*chainstore.Store, error) {
	if opts == nil {
		opts = &gogopebble.Options{}
	}
	opts.Merger = StatsMerger
	db, err := gogopebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	cfg.Backend = &Backend{db: db}
	return chainstore.New(cfg)
}

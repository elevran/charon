package chainstore

import "errors"

var (
	ErrNotFound       = errors.New("not found")
	ErrChainCorrupted = errors.New("chain corrupted: missing node in parent chain")
	ErrStoreFull      = errors.New("store full: configured capacity exceeded")
	ErrNotImplemented = errors.New("not implemented (Phase 6)")
)

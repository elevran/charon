package chainstore

import "errors"

// Sentinel errors returned by *Store methods.
var (
	ErrNotFound       = errors.New("not found")
	ErrChainCorrupted = errors.New("chain corrupted: missing node in parent chain")
	ErrStoreFull      = errors.New("store full: configured capacity exceeded")
)

// Turn is the data returned to callers by Resolve.
// RequestBlob and ResponseBlob are verbatim bytes stored by the proxy at turn creation
// and turn completion respectively. ResponseBlob is nil for turns not yet completed.
type Turn struct {
	ResponseID   string
	RequestBlob  []byte
	ResponseBlob []byte
}

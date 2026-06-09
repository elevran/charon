package storage

import "errors"

var (
	ErrNotFound       = errors.New("not found")
	ErrChainCorrupted = errors.New("chain corrupted: missing payload")
	ErrAlreadyExists  = errors.New("already exists")
	ErrStoreFull      = errors.New("store full: configured capacity exceeded")
)

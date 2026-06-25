package storage

import "errors"

var (
	ErrNotFound        = errors.New("not found")
	ErrChainCorrupted  = errors.New("chain corrupted: missing payload")
	ErrAlreadyExists   = errors.New("already exists")
	ErrStoreFull       = errors.New("store full: configured capacity exceeded")
	ErrChainTooDeep    = errors.New("chain_too_deep: chain exceeds maximum depth")
	ErrContextTooLarge = errors.New("context_too_large: assembled context exceeds size limit")
)

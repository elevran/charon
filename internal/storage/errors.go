package storage

import "errors"

var (
	ErrNotFound       = errors.New("not found")
	ErrChainCorrupted = errors.New("chain corrupted: missing payload")
	ErrAlreadyExists  = errors.New("already exists")
)

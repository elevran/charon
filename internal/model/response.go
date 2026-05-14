package model

import (
	"encoding/json"

	"github.com/openai/openai-go/responses"
)

// ResponseStatus is the lifecycle state of a stored response.
type ResponseStatus string

const (
	StatusCompleted ResponseStatus = "completed"
	StatusFailed    ResponseStatus = "failed"
	StatusDeleted   ResponseStatus = "deleted"
)

// ResponseMeta is the lightweight index record stored in IndexStore.
type ResponseMeta struct {
	ID                 string
	PreviousResponseID *string
	ChainRootID        string
	Position           int     // 0-based ordinal in chain, denormalised at write time
	PayloadKey         string  // key into PayloadStore for this turn's delta
	CheckpointKey      *string // non-nil when this position is a checkpoint
	OwnerPrincipal     string
	Model              string
	Status             ResponseStatus
	CreatedAt          int64  // Unix epoch seconds
	ExpiresAt          *int64 // Unix epoch seconds; nil means no expiry
}

// ResponsePayload is the content stored per turn.
type ResponsePayload struct {
	ID                 string            `json:"id"`
	PreviousResponseID *string           `json:"previous_response_id,omitempty"`
	InputItems         []json.RawMessage `json:"input_items"`
	OutputItems        []json.RawMessage `json:"output_items"`
	Usage              json.RawMessage   `json:"usage,omitempty"`
}

// StoreRequest is the body of POST /responses/{id}.
type StoreRequest struct {
	PreviousResponseID *string                          `json:"previous_response_id,omitempty"`
	Input              responses.ResponseInputParam     `json:"input"`
	Output             []json.RawMessage                `json:"output"`
	Usage              *responses.ResponseUsage         `json:"usage,omitempty"`
	Status             responses.ResponseStatus         `json:"status"`
	Model              string                           `json:"model,omitempty"`
}

// ResolveResponse is the body returned by GET /responses/{id}/context.
type ResolveResponse struct {
	ResponseID  string            `json:"response_id"`
	FlatContext []json.RawMessage `json:"flat_context"`
}

// RetrieveResponse is the body returned by GET /responses/{id}.
type RetrieveResponse struct {
	ID                 string                       `json:"id"`
	PreviousResponseID *string                      `json:"previous_response_id,omitempty"`
	Status             responses.ResponseStatus     `json:"status"`
	Model              string                       `json:"model,omitempty"`
	CreatedAt          int64                        `json:"created_at"`
	ExpiresAt          *int64                       `json:"expires_at,omitempty"`
	Input              responses.ResponseInputParam `json:"input"`
	Output             []json.RawMessage            `json:"output"`
	Usage              *responses.ResponseUsage     `json:"usage,omitempty"`
}

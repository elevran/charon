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
	ReservationID      string                       `json:"reservation_id,omitempty"` // from preceding resolve; omitted for new chains
	PreviousResponseID *string                      `json:"previous_response_id,omitempty"`
	Input              responses.ResponseInputParam `json:"input"`
	Output             []json.RawMessage            `json:"output"`
	Usage              json.RawMessage              `json:"usage,omitempty"` // raw JSON; avoids SDK type coupling
	Status             responses.ResponseStatus     `json:"status"`
	Model              string                       `json:"model,omitempty"`
}

// ChunkRequest is the body of PATCH /responses/{id}.
// Type "chunk" appends output items to the in-progress stream stage.
// Type "commit" finalises the stream, writing the full payload and committing the index.
//
// Seq is a 0-based sequence number assigned by the proxy before the PATCH is
// sent. Charon sorts received chunks by Seq before assembling the output,
// which allows the proxy to dispatch multiple PATCH requests concurrently
// without requiring them to arrive in order. The commit Seq must equal the
// number of preceding chunk batches (i.e. the next unused seq value).
type ChunkRequest struct {
	Type  string            `json:"type"` // "chunk" | "commit"
	Seq   int               `json:"seq"`  // 0-based; enables out-of-order reassembly
	Items []json.RawMessage `json:"items,omitempty"`
	// Commit-only fields (same semantics as StoreRequest):
	ReservationID      string            `json:"reservation_id,omitempty"`
	PreviousResponseID *string           `json:"previous_response_id,omitempty"`
	Input              []json.RawMessage `json:"input,omitempty"`
	Usage              json.RawMessage   `json:"usage,omitempty"`
	Status             string            `json:"status,omitempty"`
	Model              string            `json:"model,omitempty"`
}

// ResolveResponse is the body returned by GET /responses/{id}/context.
type ResolveResponse struct {
	ReservationID string            `json:"reservation_id"`
	FlatContext   []json.RawMessage `json:"flat_context"`
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

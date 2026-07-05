package proxy

import (
	"encoding/json"

	"github.com/elevran/charon/internal/charon"
)

// storedRequest is the format written as the request blob by Resolve.
// Contains the input items so flat context can be reconstructed from turns.
type storedRequest struct {
	Input []json.RawMessage `json:"input"`
}

// storedResponse is the format written as the response blob by Store.
// Contains the fields needed to reconstruct a ResponseResource on Retrieve.
type storedResponse struct {
	ID                 string            `json:"id"`
	Model              string            `json:"model"`
	Status             string            `json:"status"`
	Background         bool              `json:"background,omitempty"`
	Instructions       *string           `json:"instructions,omitempty"`
	PreviousResponseID *string           `json:"previous_response_id,omitempty"`
	Output             []json.RawMessage `json:"output"`
	Usage              json.RawMessage   `json:"usage,omitempty"`
}

// turnsToFlatCtx assembles the flat context from resolved turns (root-first).
// Input items come from the request blob (staged by Resolve); output items come
// from the response blob. All turns, including first turns, have a request blob
// because the proxy always calls Resolve before storing.
func turnsToFlatCtx(turns []charon.ResolveTurn) []json.RawMessage {
	var flat []json.RawMessage
	for _, t := range turns {
		if len(t.RequestBlob) > 0 {
			var req storedRequest
			if err := json.Unmarshal(t.RequestBlob, &req); err == nil {
				flat = append(flat, req.Input...)
			}
		}
		if len(t.ResponseBlob) > 0 {
			var resp storedResponse
			if err := json.Unmarshal(t.ResponseBlob, &resp); err == nil {
				flat = append(flat, resp.Output...)
			}
		}
	}
	return flat
}

// ResponseResource is the full OpenAI Responses API response returned to clients.
type ResponseResource struct {
	ID                 string            `json:"id"`
	Object             string            `json:"object"` // always "response"
	CreatedAt          int64             `json:"created_at"`
	CompletedAt        *int64            `json:"completed_at,omitempty"`
	Status             string            `json:"status"`
	Model              string            `json:"model"`
	PreviousResponseID *string           `json:"previous_response_id,omitempty"`
	Instructions       *string           `json:"instructions,omitempty"`
	Output             []json.RawMessage `json:"output"`
	Error              *ResponseError    `json:"error,omitempty"`
	Usage              *UsageResource    `json:"usage,omitempty"`
	Store              bool              `json:"store"`
	Tools              []json.RawMessage `json:"tools"`
	ToolChoice         string            `json:"tool_choice"`
	Truncation         string            `json:"truncation"`
	ParallelToolCalls  bool              `json:"parallel_tool_calls"`
	Temperature        float64           `json:"temperature"`
	TopP               float64           `json:"top_p"`
	PresencePenalty    float64           `json:"presence_penalty"`
	FrequencyPenalty   float64           `json:"frequency_penalty"`
	MaxOutputTokens    *int              `json:"max_output_tokens,omitempty"`
	Metadata           map[string]string `json:"metadata"`
	Background         bool              `json:"background"`
	ServiceTier        string            `json:"service_tier"`
}

// ResponseError is the error object embedded in a ResponseResource.
type ResponseError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// UsageResource holds token usage counters.
type UsageResource struct {
	InputTokens         int                 `json:"input_tokens"`
	OutputTokens        int                 `json:"output_tokens"`
	TotalTokens         int                 `json:"total_tokens"`
	InputTokensDetails  inputTokensDetails  `json:"input_tokens_details"`
	OutputTokensDetails outputTokensDetails `json:"output_tokens_details"`
}

type inputTokensDetails struct {
	CachedTokens int `json:"cached_tokens"`
}

type outputTokensDetails struct {
	ReasoningTokens int `json:"reasoning_tokens"`
}

// CreateRequest is the body of POST /responses from the client.
type CreateRequest struct {
	Model              string            `json:"model"`
	Input              json.RawMessage   `json:"input"` // string or []item
	PreviousResponseID *string           `json:"previous_response_id,omitempty"`
	Instructions       *string           `json:"instructions,omitempty"`
	Stream             bool              `json:"stream"`
	Store              *bool             `json:"store,omitempty"` // nil == true
	Background         bool              `json:"background,omitempty"`
	Tools              []json.RawMessage `json:"tools,omitempty"`
	ToolChoice         json.RawMessage   `json:"tool_choice,omitempty"`
}

// ShouldStore returns true when the response must be persisted in Charon.
func (r *CreateRequest) ShouldStore() bool {
	return r.Store == nil || *r.Store
}

package proxy

import "encoding/json"

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

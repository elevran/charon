package inference

import "encoding/json"

// Request is the body sent to POST {base_url}/responses.
// Always stateless: no previous_response_id; store is always false
// (the proxy, not the inference backend, owns persistence).
type Request struct {
	Model        string            `json:"model"`
	Input        []json.RawMessage `json:"input"`
	Instructions *string           `json:"instructions,omitempty"`
	Tools        []json.RawMessage `json:"tools,omitempty"`
	ToolChoice   json.RawMessage   `json:"tool_choice,omitempty"`
	Stream       bool              `json:"stream"`
	Store        bool              `json:"store"` // always false
}

// Response is the subset of ResponseResource that the inference client reads.
type Response struct {
	ID     string            `json:"id"`
	Status string            `json:"status"`
	Model  string            `json:"model"`
	Output []json.RawMessage `json:"output"`
	Error  *ResponseError    `json:"error,omitempty"`
	Usage  *UsageInfo        `json:"usage,omitempty"`
}

type ResponseError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type UsageInfo struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

// SSEEvent is a single event from the inference server's streaming response.
type SSEEvent struct {
	Type           string          `json:"type"`
	SequenceNumber int             `json:"sequence_number"`
	Response       *Response       `json:"response,omitempty"`
	OutputIndex    *int            `json:"output_index,omitempty"`
	Item           json.RawMessage `json:"item,omitempty"`
}

package inference

import "encoding/json"

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
	Raw            json.RawMessage `json:"-"`
}

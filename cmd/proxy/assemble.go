package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/elevran/charon/internal/inference"
)

// marshalStoredResponse serialises an inference response and request metadata
// into the response blob format used by Store and decoded by HandleRetrieve.
func marshalStoredResponse(infResp *inference.Response, prevID *string, instructions *string, background bool) []byte {
	var usage json.RawMessage
	if infResp.Usage != nil {
		usage, _ = json.Marshal(infResp.Usage)
	}
	sr := storedResponse{
		ID:                 infResp.ID,
		Model:              infResp.Model,
		Status:             infResp.Status,
		Background:         background,
		Instructions:       instructions,
		PreviousResponseID: prevID,
		Output:             infResp.Output,
		Usage:              usage,
	}
	b, _ := json.Marshal(sr)
	return b
}

// buildInferenceMap constructs the inference request map from the raw client
// request. It strips gateway-consumed fields, forces store:false, forces
// stream:false (caller overrides to true for streaming paths), and replaces
// input with the assembled flat context + new input items.
func buildInferenceMap(rawReq map[string]json.RawMessage, flatCtx, inputItems []json.RawMessage) map[string]json.RawMessage {
	// Shallow-copy to avoid mutating the caller's map.
	out := make(map[string]json.RawMessage, len(rawReq))
	for k, v := range rawReq {
		out[k] = v
	}

	// Strip gateway-consumed fields.
	delete(out, "previous_response_id")
	delete(out, "background")
	delete(out, "conversation")

	// Inference backend must be stateless.
	out["store"] = json.RawMessage("false")

	// Non-streaming by default; streaming callers override after this call.
	out["stream"] = json.RawMessage("false")

	// Replace input with assembled flat context + new input items.
	combined := make([]json.RawMessage, 0, len(flatCtx)+len(inputItems))
	combined = append(combined, flatCtx...)
	combined = append(combined, inputItems...)
	inputJSON, _ := json.Marshal(combined)
	out["input"] = inputJSON

	return out
}

// inputToItems normalises CreateRequest.Input into []json.RawMessage items.
// If input is a plain JSON string, wraps it as a user message item.
// If input is a JSON array, returns elements unchanged.
func inputToItems(raw json.RawMessage) ([]json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	// Determine kind by first non-whitespace byte.
	trimmed := json.RawMessage(raw)
	for i, b := range trimmed {
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		switch b {
		case '"':
			// Plain string — wrap as user message.
			var text string
			if err := json.Unmarshal(raw, &text); err != nil {
				return nil, fmt.Errorf("decode string input: %w", err)
			}
			item, _ := json.Marshal(map[string]interface{}{
				"type":    "message",
				"role":    "user",
				"content": text,
			})
			return []json.RawMessage{item}, nil
		case '[':
			// Array of items.
			var items []json.RawMessage
			if err := json.Unmarshal(raw[i:], &items); err != nil {
				return nil, fmt.Errorf("decode array input: %w", err)
			}
			return items, nil
		default:
			return nil, fmt.Errorf("unexpected input type (byte 0x%02x)", b)
		}
	}
	return nil, nil
}

// buildResponseResource constructs the ResponseResource returned to the client.
// When completedAt is nil, CompletedAt is left as nil in the returned resource.
func buildResponseResource(
	infResp *inference.Response,
	previousID *string,
	shouldStore bool,
	background bool,
	createdAt time.Time,
	completedAt *time.Time,
) *ResponseResource {
	ts := createdAt.Unix()
	var completedTs *int64
	if completedAt != nil {
		v := completedAt.Unix()
		completedTs = &v
	}
	r := &ResponseResource{
		ID:                 infResp.ID,
		Object:             "response",
		CreatedAt:          ts,
		CompletedAt:        completedTs,
		Status:             infResp.Status,
		Model:              infResp.Model,
		PreviousResponseID: previousID,
		Output:             infResp.Output,
		Store:              shouldStore,
		Background:         background,
		Tools:              []json.RawMessage{},
		ToolChoice:         "auto",
		Truncation:         "disabled",
		Temperature:        1.0,
		TopP:               1.0,
		Metadata:           map[string]string{},
		ServiceTier:        "default",
	}
	if infResp.Error != nil {
		r.Error = &ResponseError{
			Code:    infResp.Error.Code,
			Message: infResp.Error.Message,
		}
	}
	if infResp.Usage != nil {
		r.Usage = &UsageResource{
			InputTokens:  infResp.Usage.InputTokens,
			OutputTokens: infResp.Usage.OutputTokens,
			TotalTokens:  infResp.Usage.TotalTokens,
		}
	}
	if infResp.Output == nil {
		r.Output = []json.RawMessage{}
	}
	return r
}

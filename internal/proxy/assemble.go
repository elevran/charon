package proxy

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/elevran/charon/internal/inference"
)

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

// buildInferenceRequest assembles the inference.Request:
// flatCtx + inputItems concatenated as input, store always false.
func buildInferenceRequest(req CreateRequest, flatCtx, inputItems []json.RawMessage) inference.Request {
	combined := make([]json.RawMessage, 0, len(flatCtx)+len(inputItems))
	combined = append(combined, flatCtx...)
	combined = append(combined, inputItems...)

	return inference.Request{
		Model:        req.Model,
		Input:        combined,
		Instructions: req.Instructions,
		Tools:        req.Tools,
		ToolChoice:   req.ToolChoice,
		Stream:       req.Stream,
		Store:        false,
	}
}

// buildResponseResource constructs the ResponseResource returned to the client.
func buildResponseResource(
	infResp *inference.Response,
	previousID *string,
	shouldStore bool,
	now time.Time,
) *ResponseResource {
	ts := now.Unix()
	r := &ResponseResource{
		ID:                 infResp.ID,
		Object:             "response",
		CreatedAt:          ts,
		CompletedAt:        &ts,
		Status:             infResp.Status,
		Model:              infResp.Model,
		PreviousResponseID: previousID,
		Output:             infResp.Output,
		Store:              shouldStore,
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

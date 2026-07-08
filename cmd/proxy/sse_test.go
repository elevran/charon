package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type parsedSSE struct {
	events        []map[string]json.RawMessage
	finalResponse *ResponseResource
}

func parseSSE(t *testing.T, resp *http.Response) parsedSSE {
	t.Helper()
	defer resp.Body.Close()
	var result parsedSSE
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		var evt map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(data), &evt))
		result.events = append(result.events, evt)

		var typeStr string
		_ = json.Unmarshal(evt["type"], &typeStr)
		if typeStr == "response.completed" {
			var full struct {
				Response ResponseResource `json:"response"`
			}
			_ = json.Unmarshal([]byte(data), &full)
			r := full.Response
			result.finalResponse = &r
		}
	}
	return result
}

func TestStreamBasicResponse(t *testing.T) {
	s := newTestStack(t)

	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxyURL+"/responses", strings.NewReader(`{"model":"test","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	sse := parseSSE(t, resp)
	require.NotEmpty(t, sse.events, "expected SSE events")

	// Extract event types.
	types := make([]string, 0, len(sse.events))
	for _, e := range sse.events {
		var t2 string
		_ = json.Unmarshal(e["type"], &t2)
		types = append(types, t2)
	}
	assert.Contains(t, types, "response.created")
	assert.Contains(t, types, "response.completed")

	require.NotNil(t, sse.finalResponse, "expected response.completed event")
	assert.Equal(t, "completed", sse.finalResponse.Status)
	assert.NotEmpty(t, sse.finalResponse.Output)
}

func TestStreamEventSequenceNumbers(t *testing.T) {
	s := newTestStack(t)

	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxyURL+"/responses", strings.NewReader(`{"model":"test","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	sse := parseSSE(t, resp)

	for i, evt := range sse.events {
		var seq int
		_ = json.Unmarshal(evt["sequence_number"], &seq)
		assert.Equal(t, i, seq, "sequence_number must be monotonically 0,1,2,...")
	}
}

func TestStreamCreatedIDMatchesCompleted(t *testing.T) {
	s := newTestStack(t)

	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxyURL+"/responses", strings.NewReader(`{"model":"test","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	sse := parseSSE(t, resp)
	require.NotNil(t, sse.finalResponse)

	var createdID string
	for _, evt := range sse.events {
		var typeStr string
		_ = json.Unmarshal(evt["type"], &typeStr)
		if typeStr == "response.created" {
			var r struct {
				Response struct {
					ID string `json:"id"`
				} `json:"response"`
			}
			b, _ := json.Marshal(evt)
			_ = json.Unmarshal(b, &r)
			createdID = r.Response.ID
			break
		}
	}
	assert.Equal(t, createdID, sse.finalResponse.ID, "response.created ID must match response.completed ID")
}

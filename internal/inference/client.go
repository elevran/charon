package inference

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Client calls a stateless Responses API inference endpoint.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

// New creates a Client targeting baseURL.
func New(baseURL, apiKey string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		http:    &http.Client{Timeout: timeout},
	}
}

// Complete performs a non-streaming inference call.
func (c *Client) Complete(ctx context.Context, req map[string]json.RawMessage) (*Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/responses", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	c.setHeaders(hreq)

	resp, err := c.http.Do(hreq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("inference %d", resp.StatusCode)
	}
	var r Response
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("decode inference response: %w", err)
	}
	return &r, nil
}

// Stream performs a streaming inference call.
// Returns a channel of SSEEvents. Closed when the stream ends or ctx is cancelled.
// The caller must drain the channel.
func (c *Client) Stream(ctx context.Context, req map[string]json.RawMessage) (<-chan SSEEvent, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/responses", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	c.setHeaders(hreq)

	resp, err := c.http.Do(hreq) //nolint:bodyclose // body is closed in the reader goroutine below
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("inference stream %d", resp.StatusCode)
	}

	ch := make(chan SSEEvent, 16)
	go func() {
		defer close(ch)
		defer func() { _ = resp.Body.Close() }()
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := strings.TrimPrefix(line, "data: ")
			if data == "[DONE]" {
				return
			}
			var evt SSEEvent
			if err := json.Unmarshal([]byte(data), &evt); err != nil {
				continue
			}
			evt.Raw = json.RawMessage(data)
			select {
			case ch <- evt:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func (c *Client) setHeaders(r *http.Request) {
	r.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		r.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
}

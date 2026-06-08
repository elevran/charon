package charon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// StoreRequest is the body of POST /responses/{id} on Charon's internal API.
// Uses []json.RawMessage for Input/Output so the proxy does not need the
// OpenAI SDK types.
type StoreRequest struct {
	ReservationID      string            `json:"reservation_id,omitempty"`
	PreviousResponseID *string           `json:"previous_response_id,omitempty"`
	Input              []json.RawMessage `json:"input"`
	Output             []json.RawMessage `json:"output"`
	Status             string            `json:"status"`
	Model              string            `json:"model,omitempty"`
}

// RetrieveResponse is the body returned by GET /responses/{id}.
type RetrieveResponse struct {
	ID                 string            `json:"id"`
	PreviousResponseID *string           `json:"previous_response_id,omitempty"`
	Status             string            `json:"status"`
	Model              string            `json:"model,omitempty"`
	CreatedAt          int64             `json:"created_at"`
	ExpiresAt          *int64            `json:"expires_at,omitempty"`
	Input              []json.RawMessage `json:"input"`
	Output             []json.RawMessage `json:"output"`
}

// Sentinel errors mirroring storage package errors so the proxy does not
// need to import internal/storage.
var (
	ErrNotFound       = errors.New("not found")
	ErrChainCorrupted = errors.New("chain corrupted")
)

// Client calls Charon's internal HTTP API.
type Client struct {
	baseURL string
	http    *http.Client
}

// New creates a Client targeting baseURL (e.g. "http://127.0.0.1:8081").
func New(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: timeout},
	}
}

// resolveResponse is the body returned by GET /responses/{id}/context.
type resolveResponse struct {
	ReservationID string            `json:"reservation_id"`
	FlatContext   []json.RawMessage `json:"flat_context"`
}

// Resolve calls GET /responses/{id}/context.
// Returns (reservationID, flatContext) on success.
func (c *Client) Resolve(ctx context.Context, previousID string) (string, []json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/responses/"+previousID+"/context", nil)
	if err != nil {
		return "", nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	if err := c.checkStatus(resp); err != nil {
		return "", nil, err
	}
	var r resolveResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", nil, fmt.Errorf("decode resolve response: %w", err)
	}
	return r.ReservationID, r.FlatContext, nil
}

// Store calls POST /responses/{id}.
func (c *Client) Store(ctx context.Context, id string, req StoreRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/responses/"+id, bytes.NewReader(body))
	if err != nil {
		return err
	}
	hreq.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(hreq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return c.checkStatus(resp)
}

// Retrieve calls GET /responses/{id}.
func (c *Client) Retrieve(ctx context.Context, id string) (*RetrieveResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/responses/"+id, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := c.checkStatus(resp); err != nil {
		return nil, err
	}
	var r RetrieveResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("decode retrieve response: %w", err)
	}
	return &r, nil
}

// Delete calls DELETE /responses/{id}.
func (c *Client) Delete(ctx context.Context, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		c.baseURL+"/responses/"+id, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return c.checkStatus(resp)
}

// checkStatus maps HTTP error responses to sentinel errors.
func (c *Client) checkStatus(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	var errBody struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	_ = json.Unmarshal(body, &errBody)

	msg := errBody.Error
	if msg == "" {
		msg = errBody.Code
	}

	switch resp.StatusCode {
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusConflict:
		if strings.Contains(msg, "chain") {
			return ErrChainCorrupted
		}
		return fmt.Errorf("conflict: %s", msg)
	default:
		return fmt.Errorf("charon %d: %s", resp.StatusCode, msg)
	}
}

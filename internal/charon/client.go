package charon

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

// StoreRequest is the body of POST /responses/{id} on Charon's internal API.
// Uses []json.RawMessage for Input/Output so the proxy does not need the
// OpenAI SDK types.
type StoreRequest struct {
	ReservationID      string            `json:"reservation_id,omitempty"`
	PreviousResponseID *string           `json:"previous_response_id,omitempty"`
	Input              []json.RawMessage `json:"input"`
	Output             []json.RawMessage `json:"output"`
	Usage              json.RawMessage   `json:"usage,omitempty"`
	Status             string            `json:"status"`
	Model              string            `json:"model,omitempty"`
}

// CommitRequest is passed to StreamWriter.Commit to finalise a streaming store.
type CommitRequest struct {
	ReservationID      string
	PreviousResponseID *string
	Input              []json.RawMessage
	FinalItems         []json.RawMessage // merged with staged items by Charon
	Usage              json.RawMessage
	Status             string
	Model              string
}

// StreamWriter sends output item batches to Charon incrementally via PATCH,
// then commits with all metadata via a final PATCH.
// Obtain via Client.NewStreamWriter; call Append for each batch; call Commit once.
//
// Each Append call automatically assigns the next sequence number, enabling
// the proxy to dispatch multiple concurrent Append goroutines: assign seqs
// before spawning, then call AppendAt with the pre-assigned seq.
type StreamWriter struct {
	client  *Client
	id      string
	ctx     context.Context //nolint:containedctx
	nextSeq int
	mu      sync.Mutex
}

// NewStreamWriter creates a StreamWriter for the given response ID.
func (c *Client) NewStreamWriter(ctx context.Context, id string) *StreamWriter {
	return &StreamWriter{client: c, id: id, ctx: ctx}
}

// Append sends a batch of output items to Charon, auto-assigning the next
// sequence number. Safe for sequential use; for concurrent use, prefer
// AppendAt with pre-assigned sequence numbers.
func (w *StreamWriter) Append(items []json.RawMessage) error {
	if len(items) == 0 {
		return nil
	}
	w.mu.Lock()
	seq := w.nextSeq
	w.nextSeq++
	w.mu.Unlock()
	return w.appendAt(seq, items)
}

// AppendAt sends a batch with a caller-assigned sequence number.
// Use this when dispatching concurrent goroutines: assign seqs 0,1,2,...
// before spawning, call AppendAt from each goroutine, then Commit.
func (w *StreamWriter) AppendAt(seq int, items []json.RawMessage) error {
	if len(items) == 0 {
		return nil
	}
	w.mu.Lock()
	if seq >= w.nextSeq {
		w.nextSeq = seq + 1
	}
	w.mu.Unlock()
	return w.appendAt(seq, items)
}

func (w *StreamWriter) appendAt(seq int, items []json.RawMessage) error {
	return w.patch(map[string]interface{}{"type": "chunk", "seq": seq, "items": items})
}

// Commit finalises the streaming store.
// Sends PATCH /responses/{id} with {"type":"commit","seq":N,...} where N
// is the commit's sequence number (= number of preceding Append batches).
func (w *StreamWriter) Commit(req CommitRequest) error {
	w.mu.Lock()
	seq := w.nextSeq
	w.mu.Unlock()

	body := map[string]interface{}{
		"type":   "commit",
		"seq":    seq,
		"items":  req.FinalItems,
		"input":  req.Input,
		"status": req.Status,
		"model":  req.Model,
	}
	if req.ReservationID != "" {
		body["reservation_id"] = req.ReservationID
	}
	if req.PreviousResponseID != nil {
		body["previous_response_id"] = *req.PreviousResponseID
	}
	if len(req.Usage) > 0 {
		body["usage"] = req.Usage
	}
	return w.patch(body)
}

func (w *StreamWriter) patch(body interface{}) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(w.ctx, http.MethodPatch,
		w.client.baseURL+"/responses/"+w.id, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return w.client.checkStatus(resp)
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
// The client uses an HTTP/2 cleartext (H2c) transport so that concurrent
// PATCH chunk requests multiplex over a single TCP connection without
// head-of-line blocking. Falls back to HTTP/1.1 automatically if the server
// does not support H2c.
func New(baseURL string, timeout time.Duration) *Client {
	h2transport := &http2.Transport{
		AllowHTTP: true, // allow H2c (cleartext, no TLS)
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, addr)
		},
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Transport: h2transport, Timeout: timeout},
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

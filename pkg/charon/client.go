package charon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ResolveTurn is one turn returned by the resolve endpoint (root-first order).
type ResolveTurn struct {
	RequestBlob  json.RawMessage `json:"request_blob"`
	ResponseBlob json.RawMessage `json:"response_blob"`
}

// RetrieveResponse is the parsed response from GET /responses/{id}.
type RetrieveResponse struct {
	ResponseBlob []byte
	CreatedAt    int64
	ExpiresAt    int64
	Depth        uint32
	Status       uint8
	Version      uint8
}

// Sentinel errors mirroring storage package errors so the proxy does not
// need to import internal/storage.
var (
	ErrNotFound       = errors.New("not found")
	ErrChainCorrupted = errors.New("chain corrupted")
	ErrStoreFull      = errors.New("store full")
)

// Client calls Charon's internal HTTP API.
type Client struct {
	baseURL string
	http    *http.Client
}

// New creates a Client targeting baseURL (e.g. "http://127.0.0.1:8081").
//
// The transport is tuned for the proxy→Charon pattern: N concurrent streaming
// clients each make sequential requests (resolve, store/chunk, commit), so up
// to N connections may be active simultaneously. Go's default
// MaxIdleConnsPerHost of 2 closes and re-opens connections on every burst;
// 64 retains enough idle connections to serve typical concurrency without
// reconnection overhead. Raise MaxIdleConnsPerHost if sustained concurrency
// exceeds this value.
//
// H2c note: if concurrent streaming clients exceed ~500 on a separate-host
// deployment, switching to an http2.Transport{AllowHTTP:true} collapses N
// connections into 1 multiplexed connection. See StreamWriter comment.
func New(baseURL string, timeout time.Duration) *Client {
	transport := &http.Transport{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 64, // retain idle connections across concurrent streaming clients
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true, // internal traffic; no value in gzip
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Transport: transport, Timeout: timeout},
	}
}

// resolveResponse is the JSON body returned by POST /staging?prev={id}.
type resolveResponse struct {
	StagingID string        `json:"staging_id"`
	Turns     []ResolveTurn `json:"turns"`
}

// Resolve calls POST /staging?prev={previousID}.
// Sends the raw request blob as the body, returns (stagingID, turns) on success.
// When previousID is empty the "prev" query param is omitted (first-turn staging).
// tenantKey is forwarded as X-Tenant-Key (empty string sends an empty header, treated as no tenant).
func (c *Client) Resolve(ctx context.Context, previousID, tenantKey string, requestBlob []byte) (string, []ResolveTurn, error) {
	u := c.baseURL + "/staging"
	if previousID != "" {
		u += "?" + url.Values{"prev": {previousID}}.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(requestBlob))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("X-Tenant-Key", tenantKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if err := c.checkStatus(resp); err != nil {
		return "", nil, err
	}
	var r resolveResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", nil, fmt.Errorf("decode resolve response: %w", err)
	}
	return r.StagingID, r.Turns, nil
}

// Store commits a completed response turn.
//
// When stagingID is non-empty (the proxy has an open staging record from a
// prior Resolve call), responseBlob is written as a single chunk and then
// completed: two round trips to PUT /staging/{sid}/chunks/0 and
// PUT /staging/{sid}/complete.
//
// When stagingID is empty (no prior staging, e.g. direct root write in
// tests), the call falls through to POST /responses (buffered path) with
// just the response blob.
func (c *Client) Store(ctx context.Context, id, stagingID, tenantKey string, responseBlob []byte) error {
	if stagingID != "" {
		if err := c.AppendChunk(ctx, stagingID, 0, id, responseBlob); err != nil {
			return err
		}
		_, err := c.Complete(ctx, stagingID, id, tenantKey, uint32(len(responseBlob)))
		return err
	}
	// No staging record: buffered single-call path.
	return c.storeBuffered(ctx, "", id, tenantKey, nil, responseBlob)
}

// storeBuffered calls POST /responses with both blobs in a single JSON body.
// prevID may be empty for a root response; responseID may be empty to let the
// server assign a UUID.
func (c *Client) storeBuffered(ctx context.Context, prevID, responseID, tenantKey string, requestBlob, responseBlob []byte) error {
	payload, err := json.Marshal(struct {
		PrevID       string          `json:"prev_id,omitempty"`
		ResponseID   string          `json:"response_id,omitempty"`
		RequestBlob  json.RawMessage `json:"request_blob,omitempty"`
		ResponseBlob json.RawMessage `json:"response_blob,omitempty"`
	}{
		PrevID:       prevID,
		ResponseID:   responseID,
		RequestBlob:  json.RawMessage(requestBlob),
		ResponseBlob: json.RawMessage(responseBlob),
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/responses", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-Key", tenantKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return c.checkStatus(resp)
}

// AppendChunk calls PUT /staging/{stagingID}/chunks/{k}.
// Sends chunkBody as the raw body. responseID optionally binds the staging
// record on first call; subsequent calls with a different responseID return an error.
func (c *Client) AppendChunk(ctx context.Context, stagingID string, k uint32, responseID string, chunkBody []byte) error {
	u := fmt.Sprintf("%s/staging/%s/chunks/%d", c.baseURL, stagingID, k)
	if responseID != "" {
		u += "?" + url.Values{"response_id": {responseID}}.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(chunkBody))
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return c.checkStatus(resp)
}

// Complete calls PUT /staging/{stagingID}/complete?response_id=...&total=...
// to finalize a streaming ingest. Returns the final responseID.
func (c *Client) Complete(ctx context.Context, stagingID, responseID, tenantKey string, total uint32) (string, error) {
	u := fmt.Sprintf("%s/staging/%s/complete?%s", c.baseURL, stagingID,
		url.Values{"response_id": {responseID}, "total": {strconv.FormatUint(uint64(total), 10)}}.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Tenant-Key", tenantKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()

	if err := c.checkStatus(resp); err != nil {
		return "", err
	}
	var r struct {
		ResponseID string `json:"response_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", fmt.Errorf("decode complete response: %w", err)
	}
	return r.ResponseID, nil
}

// Abort calls PUT /staging/{stagingID}/abort to terminate a staging record.
func (c *Client) Abort(ctx context.Context, stagingID string) error {
	u := fmt.Sprintf("%s/staging/%s/abort", c.baseURL, stagingID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return c.checkStatus(resp)
}

// GetChain calls GET /chain/{id}.
//
// Read-only: returns the chain rooted at id (root-first turns) without
// opening a staging record or otherwise committing a new turn. The wire
// shape mirrors the turns[] portion of POST /staging; the difference is
// purely that no staging side-effect occurs. Used by the proxy for
// store:false turns, or when the proxy intends to commit the request blob
// later via the atomic POST /responses endpoint rather than via the
// streaming staging pipeline.
//
// tenantKey is forwarded as X-Tenant-Key (empty string sends an empty
// header, treated as no tenant).
func (c *Client) GetChain(ctx context.Context, id, tenantKey string) ([]ResolveTurn, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/chain/"+id, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Tenant-Key", tenantKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if err := c.checkStatus(resp); err != nil {
		return nil, err
	}
	var r struct {
		Turns []ResolveTurn `json:"turns"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("decode get-chain response: %w", err)
	}
	return r.Turns, nil
}

// Retrieve calls GET /responses/{id}.
// Returns the raw response blob and public node metadata from response headers.
// tenantKey is forwarded as X-Tenant-Key (empty string sends an empty header, treated as no tenant).
func (c *Client) Retrieve(ctx context.Context, id, tenantKey string) (*RetrieveResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/responses/"+id, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Tenant-Key", tenantKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if err := c.checkStatus(resp); err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read retrieve body: %w", err)
	}
	r := &RetrieveResponse{ResponseBlob: body}
	r.CreatedAt, _ = strconv.ParseInt(resp.Header.Get("X-Created-At"), 10, 64)
	r.ExpiresAt, _ = strconv.ParseInt(resp.Header.Get("X-Expires-At"), 10, 64)
	depth, _ := strconv.ParseUint(resp.Header.Get("X-Depth"), 10, 32)
	r.Depth = uint32(depth)
	status, _ := strconv.ParseUint(resp.Header.Get("X-Status"), 10, 8)
	r.Status = uint8(status)
	version, _ := strconv.ParseUint(resp.Header.Get("X-Version"), 10, 8)
	r.Version = uint8(version)
	return r, nil
}

// Delete calls DELETE /responses/{id}.
// tenantKey is forwarded as X-Tenant-Key (empty string sends an empty header, treated as no tenant).
func (c *Client) Delete(ctx context.Context, id, tenantKey string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		c.baseURL+"/responses/"+id, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Tenant-Key", tenantKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
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
	case http.StatusInsufficientStorage:
		return ErrStoreFull
	default:
		return fmt.Errorf("charon %d: %s", resp.StatusCode, msg)
	}
}

// Backend is the interface proxy.Handler requires from the Charon client.
type Backend interface {
	Resolve(ctx context.Context, previousID, tenantKey string, requestBlob []byte) (string, []ResolveTurn, error)
	GetChain(ctx context.Context, id, tenantKey string) ([]ResolveTurn, error)
	Store(ctx context.Context, id, stagingID, tenantKey string, responseBlob []byte) error
	AppendChunk(ctx context.Context, stagingID string, k uint32, responseID string, body []byte) error
	Complete(ctx context.Context, stagingID, responseID, tenantKey string, total uint32) (string, error)
	Abort(ctx context.Context, stagingID string) error
	Retrieve(ctx context.Context, id, tenantKey string) (*RetrieveResponse, error)
	Delete(ctx context.Context, id, tenantKey string) error
}

var _ Backend = (*Client)(nil)

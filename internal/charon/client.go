package charon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ResolveTurn is one turn returned by the resolve endpoint (root-first order).
type ResolveTurn struct {
	RequestBlob  []byte `json:"request_blob"`
	ResponseBlob []byte `json:"response_blob"`
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

// resolveResponse is the JSON body returned by POST /responses/{id}/resolve.
type resolveResponse struct {
	StagingID string        `json:"staging_id"`
	Turns     []ResolveTurn `json:"turns"`
}

// Resolve calls POST /responses?prev={previousID}.
// Sends the raw request blob as the body, returns (stagingID, turns) on success.
// When previousID is empty the "prev" query param is omitted (first-turn staging).
// tenantKey is forwarded as X-Tenant-Key if non-empty.
func (c *Client) Resolve(ctx context.Context, previousID, tenantKey string, requestBlob []byte) (string, []ResolveTurn, error) {
	url := c.baseURL + "/responses"
	if previousID != "" {
		url += "?prev=" + previousID
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(requestBlob))
	if err != nil {
		return "", nil, err
	}
	if tenantKey != "" {
		req.Header.Set("X-Tenant-Key", tenantKey)
	}
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

// Store calls POST /responses/{id}?req={stagingID}.
// Sends responseBlob as the raw body; stagingID is passed as the "req" query param.
func (c *Client) Store(ctx context.Context, id, stagingID, tenantKey string, responseBlob []byte) error {
	url := c.baseURL + "/responses/" + id
	if stagingID != "" {
		url += "?req=" + stagingID
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(responseBlob))
	if err != nil {
		return err
	}
	if tenantKey != "" {
		req.Header.Set("X-Tenant-Key", tenantKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return c.checkStatus(resp)
}

// Retrieve calls GET /responses/{id}.
// Returns the raw response blob and public node metadata from response headers.
// tenantKey is forwarded as X-Tenant-Key if non-empty.
func (c *Client) Retrieve(ctx context.Context, id, tenantKey string) (*RetrieveResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/responses/"+id, nil)
	if err != nil {
		return nil, err
	}
	if tenantKey != "" {
		req.Header.Set("X-Tenant-Key", tenantKey)
	}
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
// tenantKey is forwarded as X-Tenant-Key if non-empty.
func (c *Client) Delete(ctx context.Context, id, tenantKey string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		c.baseURL+"/responses/"+id, nil)
	if err != nil {
		return err
	}
	if tenantKey != "" {
		req.Header.Set("X-Tenant-Key", tenantKey)
	}
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

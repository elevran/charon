package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	crdbpebble "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/chainstore"
	pebblebe "github.com/elevran/charon/internal/chainstore/pebble"
	"github.com/elevran/charon/internal/inference"
	"github.com/elevran/charon/internal/server"
	"github.com/elevran/charon/pkg/charon"
)

// ---------------------------------------------------------------------------
// Test stack
// ---------------------------------------------------------------------------

// testStack holds the three servers that make up the test infrastructure.
type testStack struct {
	charonSrv *httptest.Server
	mockInf   *inference.MockServer
	proxySrv  *httptest.Server
	// proxyURL is the base URL for the proxy. For httptest stacks it equals
	// proxySrv.URL; for real-listener stacks it is the http://host:port string.
	proxyURL string
}

type stackConfig struct {
	charonMiddleware func(http.Handler) http.Handler
	infURL           string // non-empty → use this URL instead of a fresh MockServer
	realListeners    bool
	timeout          time.Duration
}

// stackOption is a functional option for newTestStack.
type stackOption func(*stackConfig)

// withCharonMiddleware wraps the Charon mux before handing it to httptest.
// Used to inject faults (e.g. failing store).
func withCharonMiddleware(mw func(http.Handler) http.Handler) stackOption {
	return func(c *stackConfig) { c.charonMiddleware = mw }
}

// withInferenceURL directs the proxy at an already-running inference server
// instead of starting a fresh MockServer.
func withInferenceURL(u string) stackOption {
	return func(c *stackConfig) { c.infURL = u }
}

// withRealListeners uses real OS-assigned TCP ports instead of httptest servers.
// Required when an out-of-process client (e.g. bun) needs to connect.
func withRealListeners() stackOption {
	return func(c *stackConfig) { c.realListeners = true }
}

// withTimeout sets the client timeout used for charon and inference clients.
// Default is 5s; use a larger value for slow out-of-process runners.
func withTimeout(d time.Duration) stackOption {
	return func(c *stackConfig) { c.timeout = d }
}

// newTestStack creates a full Charon + (optional) mock inference + proxy stack
// and registers t.Cleanup for every resource it allocates.
func newTestStack(t testing.TB, opts ...stackOption) *testStack {
	t.Helper()

	cfg := stackConfig{timeout: 5 * time.Second}
	for _, o := range opts {
		o(&cfg)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Charon store
	pebbleOpts := &crdbpebble.Options{FS: vfs.NewMem()}
	svc, err := pebblebe.Open(context.Background(), "", pebbleOpts, chainstore.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	// Charon HTTP handler
	charonH := server.NewHandler(svc, log)
	charonMux := http.NewServeMux()
	server.RegisterHandlers(charonMux, charonH)
	var charonHandler http.Handler = charonMux
	if cfg.charonMiddleware != nil {
		charonHandler = cfg.charonMiddleware(charonMux)
	}

	// Inference server
	var mockInf *inference.MockServer
	infURL := cfg.infURL
	if infURL == "" {
		mockInf = inference.NewMockServer()
		t.Cleanup(mockInf.Close)
		infURL = mockInf.URL
	}

	// Proxy handler
	charonClient := charon.New("", cfg.timeout) // URL filled in below
	infClient := inference.New(infURL, "", cfg.timeout)
	proxyH := NewHandler(charonClient, infClient, log)
	proxyMux := http.NewServeMux()
	RegisterHandlers(proxyMux, proxyH)

	s := &testStack{mockInf: mockInf}

	if cfg.realListeners {
		charonLn := mustListen(t)
		charonSrv := &http.Server{Handler: charonHandler}
		go charonSrv.Serve(charonLn) //nolint:errcheck
		charonURL := fmt.Sprintf("http://127.0.0.1:%d", charonLn.Addr().(*net.TCPAddr).Port)
		t.Cleanup(func() { charonSrv.Close() })

		// Re-create the charon client now that we know the URL.
		proxyH = NewHandler(charon.New(charonURL, cfg.timeout), infClient, log)
		proxyMux = http.NewServeMux()
		RegisterHandlers(proxyMux, proxyH)

		proxyLn := mustListen(t)
		proxySrv := &http.Server{Handler: proxyMux}
		go proxySrv.Serve(proxyLn) //nolint:errcheck
		t.Cleanup(func() { proxySrv.Close() })

		s.proxyURL = fmt.Sprintf("http://127.0.0.1:%d", proxyLn.Addr().(*net.TCPAddr).Port)
	} else {
		charonSrv := httptest.NewServer(charonHandler)
		t.Cleanup(charonSrv.Close)
		s.charonSrv = charonSrv

		// Re-create the charon client with the actual URL.
		proxyH = NewHandler(charon.New(charonSrv.URL, cfg.timeout), infClient, log)
		proxyMux = http.NewServeMux()
		RegisterHandlers(proxyMux, proxyH)

		proxySrv := httptest.NewServer(proxyMux)
		t.Cleanup(proxySrv.Close)
		s.proxySrv = proxySrv
		s.proxyURL = proxySrv.URL
	}

	return s
}

func mustListen(t testing.TB) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	return ln
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

// doRequest sends an HTTP request with an optional JSON body and returns the response.
func doRequest(t testing.TB, baseURL, method, path string, body any) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(context.Background(), method, baseURL+path, r)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// decodeJSON decodes the response body into T.
func decodeJSON[T any](t testing.TB, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var v T
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&v))
	return v
}

// ---------------------------------------------------------------------------
// SSE helpers
// ---------------------------------------------------------------------------

// sseResult holds the events and final response from an SSE stream.
type sseResult struct {
	Events        []map[string]json.RawMessage
	EventTypes    []string
	FinalResponse *ResponseResource
	ErrorCode     string
}

// readSSE consumes an SSE response body and returns structured results.
// It also extracts the first response.created ID (for disruptive tests).
func readSSE(t testing.TB, resp *http.Response) sseResult {
	t.Helper()
	defer resp.Body.Close()
	var r sseResult
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		var evt map[string]json.RawMessage
		if err := json.Unmarshal([]byte(data), &evt); err != nil {
			continue
		}
		r.Events = append(r.Events, evt)

		var typeStr string
		_ = json.Unmarshal(evt["type"], &typeStr)
		if typeStr != "" {
			r.EventTypes = append(r.EventTypes, typeStr)
		}

		switch typeStr {
		case "response.completed", "response.failed":
			var container struct {
				Response ResponseResource `json:"response"`
			}
			if json.Unmarshal([]byte(data), &container) == nil {
				res := container.Response
				r.FinalResponse = &res
			}
		case "error":
			var container struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if json.Unmarshal([]byte(data), &container) == nil {
				r.ErrorCode = container.Error.Code
			}
		}
	}
	return r
}

// createdID returns the response ID from the first response.created event in
// the stream, used to verify disruptive-test non-persistence.
func (r sseResult) createdID() string {
	for _, evt := range r.Events {
		var typeStr string
		_ = json.Unmarshal(evt["type"], &typeStr)
		if typeStr == "response.created" {
			var container struct {
				Response struct {
					ID string `json:"id"`
				} `json:"response"`
			}
			if json.Unmarshal(mustMarshal(evt), &container) == nil {
				return container.Response.ID
			}
		}
	}
	return ""
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// ---------------------------------------------------------------------------
// WebSocket helpers
// ---------------------------------------------------------------------------

// wsSession wraps a gorilla WebSocket connection with test helpers.
type wsSession struct {
	conn *websocket.Conn
	t    testing.TB
}

// dialWS connects to the /responses WebSocket endpoint.
func dialWS(t testing.TB, serverURL string) *wsSession {
	t.Helper()
	u, _ := url.Parse(serverURL)
	u.Scheme = "ws"
	u.Path = "/responses"
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return &wsSession{conn: conn, t: t}
}

func (w *wsSession) send(msg any) {
	b, _ := json.Marshal(msg)
	require.NoError(w.t, w.conn.WriteMessage(websocket.TextMessage, b))
}

func (w *wsSession) readUntil(timeout time.Duration) (resp ResponseResource, errCode string) {
	w.conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		_, msgBytes, err := w.conn.ReadMessage()
		if err != nil {
			return
		}
		var evt struct {
			Type     string            `json:"type"`
			Response *ResponseResource `json:"response,omitempty"`
			Error    *struct {
				Code string `json:"code"`
			} `json:"error,omitempty"`
		}
		if json.Unmarshal(msgBytes, &evt) != nil {
			continue
		}
		switch evt.Type {
		case "response.completed", "response.failed":
			if evt.Response != nil {
				resp = *evt.Response
			}
			return
		case "error":
			if evt.Error != nil {
				errCode = evt.Error.Code
			}
			return
		}
	}
}

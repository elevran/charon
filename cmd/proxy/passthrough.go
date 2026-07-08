package main

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// newPassthroughProxy constructs an httputil.ReverseProxy that forwards
// every request verbatim to targetURL, preserving method, path, query,
// headers, and body, and streams the response back unchanged.
func newPassthroughProxy(targetURL string, log *slog.Logger) http.Handler {
	target, err := url.Parse(targetURL)
	if err != nil {
		// Programmer error: targetURL must be a valid URL.
		panic("passthrough proxy: invalid target URL: " + err.Error())
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	// The default Director set by NewSingleHostReverseProxy rewrites the
	// request's Host header to the target host. That is the correct behaviour
	// for a transparent reverse-proxy.
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Error("passthrough proxy error", "method", r.Method, "path", r.URL.Path, "err", err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	return proxy
}

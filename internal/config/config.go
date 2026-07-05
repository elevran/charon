package config

import (
	"net"
	"time"
)

// StorageConfig holds store-level settings. Used by storage backend packages.
type StorageConfig struct {
	DataDir         string        `json:"data_dir"`          // default "./data"
	TTLDays         int           `json:"ttl_days"`          // default 30
	MaxResponses    int64         `json:"max_responses"`     // max total responses; 0 = unbounded
	MaxPayload      ByteSize      `json:"max_payload"`       // max size of a single response payload blob
	MaxChainDepth   int           `json:"max_chain_depth"`   // max chain walk hops; 0 = default (1000)
	MaxContextBytes ByteSize      `json:"max_context_bytes"` // max assembled context size; 0 = unbounded
	TTLInterval     time.Duration `json:"ttl_interval"`      // how often the TTL expiry worker runs; default 1h
}

// deriveCharonURL returns an HTTP URL for the Charon internal API from its
// listen address. Wildcard hosts (empty, "0.0.0.0", "::") are replaced with
// "127.0.0.1" so the proxy connects to localhost.
func deriveCharonURL(charonListen string) string {
	host, port, err := net.SplitHostPort(charonListen)
	if err != nil {
		return "http://127.0.0.1:8081"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

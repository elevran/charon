package config

import (
	"net"
	"time"
)

// StorageConfig holds store-level settings. Used by storage backend packages.
type StorageConfig struct {
	Backend                   string        `json:"backend"`                      // "memory" (default) | "sqlite" | "postgres" | "postgres+s3"
	IndexBackend              string        `json:"index_backend"`                // "memory" | "sqlite" | "postgres"; overrides Backend when set
	PayloadBackend            string        `json:"payload_backend"`              // "memory" | "filesystem" | "s3"; overrides Backend when set
	DataDir                   string        `json:"data_dir"`                     // default "./data"
	CheckpointInterval        int           `json:"checkpoint_interval"`          // default 10
	TTLDays                   int           `json:"ttl_days"`                     // default 30
	WriteIntentStaleThreshold time.Duration `json:"write_intent_stale_threshold"` // default 5m
	// Caps — 0 means unbounded.
	MaxResponses int64    `json:"max_responses"` // max total responses in the index
	MaxPayload   ByteSize `json:"max_payload"`   // max size of a single response payload blob
	// Backend-specific connection settings.
	Postgres PostgresConfig `json:"postgres"`
	S3       S3Config       `json:"s3"`
}

// PostgresConfig holds PostgreSQL connection settings.
type PostgresConfig struct {
	DSN      string `json:"dsn"`       // e.g. "postgres://user:pass@host:5432/db"
	MaxConns int32  `json:"max_conns"` // default 10
}

// S3Config holds S3-compatible object storage settings.
type S3Config struct {
	Bucket          string `json:"bucket"`            // e.g. "charon-payloads"
	Region          string `json:"region"`            // e.g. "us-east-1"
	EndpointURL     string `json:"endpoint_url"`      // empty = AWS; set for MinIO/COS
	AccessKeyID     string `json:"access_key_id"`     // empty = default credential chain
	SecretAccessKey string `json:"secret_access_key"` // empty = default credential chain
	PathStyle       bool   `json:"path_style"`        // true for MinIO
}

// TelemetryConfig holds OpenTelemetry tracing settings.
// ExporterURL is the OTLP HTTP endpoint (e.g. "http://localhost:4318").
// Empty means tracing is disabled (no-op provider, zero overhead).
type TelemetryConfig struct {
	ExporterURL   string `json:"exporter_url"`   // OTLP HTTP endpoint; empty = disabled
	CharonService string `json:"charon_service"` // default "charon"
	ProxyService  string `json:"proxy_service"`  // default "charon-proxy"
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

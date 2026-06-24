package config

import (
	"fmt"
	"net"
	"os"
	"time"

	"sigs.k8s.io/yaml"
)

// Config is the top-level application configuration.
// All proxy concerns live under Proxy; all Charon concerns under Charon.
type Config struct {
	Proxy  ProxyConfig  `json:"proxy"`
	Charon CharonConfig `json:"charon"`
}

// ProxyConfig holds all proxy-side settings. When Enabled is false (the
// default), the binary starts only the Charon internal API; no inference
// client, no proxy HTTP server, and no Charon HTTP client are created.
type ProxyConfig struct {
	Enabled   bool            `json:"enabled"`    // default false — proxy is off unless explicitly enabled
	Listen    string          `json:"listen"`     // default ":8080"
	CharonURL string          `json:"charon_url"` // default auto-derived from Charon.Listen
	Inference InferenceConfig `json:"inference"`
}

// InferenceConfig is the stateless Responses API inference backend.
// The backend receives a full flat_context as input and returns a
// ResponseResource with its own assigned id.
type InferenceConfig struct {
	BaseURL          string `json:"base_url"` // default "http://localhost:11434"
	APIKey           string `json:"api_key"`
	TimeoutSeconds   int    `json:"timeout_seconds"`    // default 120
	StoreBufferBytes int    `json:"store_buffer_bytes"` // 0 → 65536 (64 KB); -1 → flush every item
}

// CharonConfig holds all Charon-side settings.
type CharonConfig struct {
	Listen  string        `json:"listen"` // default ":8081"
	Storage StorageConfig `json:"storage"`
	Workers WorkerConfig  `json:"workers"`
}

// StorageConfig holds store-level settings.
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

// WorkerConfig holds background worker settings.
type WorkerConfig struct {
	TTLInterval      time.Duration `json:"ttl_interval"`      // default 1h
	RecoveryInterval time.Duration `json:"recovery_interval"` // default 5m
}

// Load reads config from path and applies defaults. If path is empty, returns defaults.
func Load(path string) (Config, error) {
	var cfg Config
	applyDefaults(&cfg)

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("read config: %w", err)
		}
		if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
			return cfg, fmt.Errorf("parse config: %w", err)
		}
		applyDefaults(&cfg)
	}

	// Auto-derive proxy.charon_url from charon.listen if not explicitly set.
	// Done after both applyDefaults passes so any user-supplied charon.listen is resolved first.
	if cfg.Proxy.CharonURL == "" {
		cfg.Proxy.CharonURL = deriveCharonURL(cfg.Charon.Listen)
	}

	return cfg, nil
}

func applyDefaults(cfg *Config) {
	// Proxy defaults — Enabled stays false unless the user sets proxy.enabled: true
	if cfg.Proxy.Listen == "" {
		cfg.Proxy.Listen = ":8080"
	}
	if cfg.Proxy.Inference.BaseURL == "" {
		cfg.Proxy.Inference.BaseURL = "http://localhost:11434"
	}
	if cfg.Proxy.Inference.TimeoutSeconds <= 0 {
		cfg.Proxy.Inference.TimeoutSeconds = 120
	}
	// StoreBufferBytes: 0 → 65536 (64 KB); -1 → immediate flush; N>0 → N bytes
	if cfg.Proxy.Inference.StoreBufferBytes == 0 {
		cfg.Proxy.Inference.StoreBufferBytes = 65536
	}

	// Charon defaults
	if cfg.Charon.Listen == "" {
		cfg.Charon.Listen = ":8081"
	}
	if cfg.Charon.Storage.Backend == "" {
		cfg.Charon.Storage.Backend = "memory"
	}
	if cfg.Charon.Storage.DataDir == "" {
		cfg.Charon.Storage.DataDir = "./data"
	}
	if cfg.Charon.Storage.CheckpointInterval <= 0 {
		cfg.Charon.Storage.CheckpointInterval = 10
	}
	if cfg.Charon.Storage.TTLDays <= 0 {
		cfg.Charon.Storage.TTLDays = 30
	}
	if cfg.Charon.Storage.WriteIntentStaleThreshold <= 0 {
		cfg.Charon.Storage.WriteIntentStaleThreshold = 5 * time.Minute
	}
	if cfg.Charon.Workers.TTLInterval <= 0 {
		cfg.Charon.Workers.TTLInterval = time.Hour
	}
	if cfg.Charon.Workers.RecoveryInterval <= 0 {
		cfg.Charon.Workers.RecoveryInterval = 5 * time.Minute
	}
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

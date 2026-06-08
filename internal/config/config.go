package config

import (
	"fmt"
	"os"
	"time"

	"sigs.k8s.io/yaml"
)

// Config is the top-level application configuration.
type Config struct {
	Server    ServerConfig    `json:"server"`
	Charon    CharonConfig    `json:"charon"`
	Storage   StorageConfig   `json:"storage"`
	Workers   WorkerConfig    `json:"workers"`
	Inference InferenceConfig `json:"inference"`
}

// CharonConfig controls both the port Charon listens on and the URL the
// proxy uses to reach it. In single-binary mode these point to localhost;
// in multi-binary mode they point to the remote Charon instance.
type CharonConfig struct {
	Listen  string `json:"listen"`   // default ":8081"
	BaseURL string `json:"base_url"` // default "http://127.0.0.1:8081"
}

// InferenceConfig is the stateless Responses API inference backend.
// The backend receives a full flat_context as input and returns a
// ResponseResource with its own assigned id.
type InferenceConfig struct {
	BaseURL        string `json:"base_url"`        // default "http://localhost:11434"
	APIKey         string `json:"api_key"`
	TimeoutSeconds int    `json:"timeout_seconds"` // default 120
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Listen string `json:"listen"`
}

// StorageConfig holds store-level settings.
type StorageConfig struct {
	CheckpointInterval        int           `json:"checkpoint_interval"`
	TTLDays                   int           `json:"ttl_days"`
	WriteIntentStaleThreshold time.Duration `json:"write_intent_stale_threshold"`
	Backend                   string        `json:"backend"`  // "memory" (default) | "sqlite"
	DataDir                   string        `json:"data_dir"` // default "./data"
	SQLite                    SQLiteConfig  `json:"sqlite"`
}

// SQLiteConfig holds SQLite-specific tuning knobs.
type SQLiteConfig struct {
	WALMode       bool `json:"wal_mode"`
	BusyTimeoutMs int  `json:"busy_timeout_ms"`
}

// WorkerConfig holds background worker settings.
type WorkerConfig struct {
	TTLInterval      time.Duration `json:"ttl_interval"`
	RecoveryInterval time.Duration `json:"recovery_interval"`
}

// Load reads config from path and applies defaults. If path is empty, returns defaults.
func Load(path string) (Config, error) {
	var cfg Config
	applyDefaults(&cfg)

	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	applyDefaults(&cfg)
	return cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Server.Listen == "" {
		cfg.Server.Listen = ":8080"
	}
	if cfg.Storage.CheckpointInterval <= 0 {
		cfg.Storage.CheckpointInterval = 10
	}
	if cfg.Storage.TTLDays <= 0 {
		cfg.Storage.TTLDays = 30
	}
	if cfg.Storage.WriteIntentStaleThreshold <= 0 {
		cfg.Storage.WriteIntentStaleThreshold = 5 * time.Minute
	}
	if cfg.Storage.Backend == "" {
		cfg.Storage.Backend = "memory"
	}
	if cfg.Storage.DataDir == "" {
		cfg.Storage.DataDir = "./data"
	}
	if cfg.Storage.SQLite.BusyTimeoutMs <= 0 {
		cfg.Storage.SQLite.BusyTimeoutMs = 5000
	}
	// WALMode defaults to false (DELETE journal); set wal_mode: true in config to enable WAL.
	if cfg.Charon.Listen == "" {
		cfg.Charon.Listen = ":8081"
	}
	if cfg.Charon.BaseURL == "" {
		cfg.Charon.BaseURL = "http://127.0.0.1:8081"
	}
	if cfg.Inference.BaseURL == "" {
		cfg.Inference.BaseURL = "http://localhost:11434"
	}
	if cfg.Inference.TimeoutSeconds <= 0 {
		cfg.Inference.TimeoutSeconds = 120
	}
	if cfg.Workers.TTLInterval <= 0 {
		cfg.Workers.TTLInterval = time.Hour
	}
	if cfg.Workers.RecoveryInterval <= 0 {
		cfg.Workers.RecoveryInterval = 5 * time.Minute
	}
}

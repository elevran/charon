package config

import (
	"fmt"
	"os"
	"time"

	"sigs.k8s.io/yaml"
)

// Config is the top-level application configuration.
type Config struct {
	Server  ServerConfig  `json:"server"`
	Storage StorageConfig `json:"storage"`
	Workers WorkerConfig  `json:"workers"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Listen string `json:"listen"`
}

// StorageConfig holds store-level settings.
type StorageConfig struct {
	CheckpointInterval          int           `json:"checkpoint_interval"`
	TTLDays                     int           `json:"ttl_days"`
	WriteIntentStaleThreshold   time.Duration `json:"write_intent_stale_threshold"`
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
	if cfg.Workers.TTLInterval <= 0 {
		cfg.Workers.TTLInterval = time.Hour
	}
	if cfg.Workers.RecoveryInterval <= 0 {
		cfg.Workers.RecoveryInterval = 5 * time.Minute
	}
}

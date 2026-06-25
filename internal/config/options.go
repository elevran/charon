package config

import (
	"flag"
	"fmt"
	"os"
	"time"

	"sigs.k8s.io/yaml"
)

// fileConfig is a private type used only to parse YAML config files.
// It mirrors the YAML structure so existing config files continue to work.
type fileConfig struct {
	Proxy     fileProxyConfig     `json:"proxy"`
	Charon    fileCharonConfig    `json:"charon"`
	Telemetry fileTelemetryConfig `json:"telemetry"`
}

type fileProxyConfig struct {
	Enabled   bool                `json:"enabled"`
	Listen    string              `json:"listen"`
	CharonURL string              `json:"charon_url"`
	Inference fileInferenceConfig `json:"inference"`
}

type fileInferenceConfig struct {
	BaseURL          string `json:"base_url"`
	APIKey           string `json:"api_key"`
	TimeoutSeconds   int    `json:"timeout_seconds"`
	StoreBufferBytes int    `json:"store_buffer_bytes"`
}

type fileCharonConfig struct {
	Listen  string            `json:"listen"`
	Storage fileStorageConfig `json:"storage"`
	Workers fileWorkerConfig  `json:"workers"`
}

type fileStorageConfig struct {
	Backend                   string         `json:"backend"`
	IndexBackend              string         `json:"index_backend"`
	PayloadBackend            string         `json:"payload_backend"`
	DataDir                   string         `json:"data_dir"`
	CheckpointInterval        int            `json:"checkpoint_interval"`
	TTLDays                   int            `json:"ttl_days"`
	WriteIntentStaleThreshold time.Duration  `json:"write_intent_stale_threshold"`
	MaxResponses              int64          `json:"max_responses"`
	MaxPayload                ByteSize       `json:"max_payload"`
	Postgres                  PostgresConfig `json:"postgres"`
	S3                        S3Config       `json:"s3"`
}

type fileWorkerConfig struct {
	TTLInterval      time.Duration `json:"ttl_interval"`
	RecoveryInterval time.Duration `json:"recovery_interval"`
}

type fileTelemetryConfig struct {
	ExporterURL   string `json:"exporter_url"`
	CharonService string `json:"charon_service"`
	ProxyService  string `json:"proxy_service"`
}

// applyFileDefaults fills in zero-valued fields in fc with built-in defaults.
// Called twice: before and after YAML unmarshalling so that missing file fields
// stay at their defaults.
func applyFileDefaults(fc *fileConfig) {
	if fc.Proxy.Listen == "" {
		fc.Proxy.Listen = ":8080"
	}
	if fc.Proxy.Inference.BaseURL == "" {
		fc.Proxy.Inference.BaseURL = "http://localhost:11434"
	}
	if fc.Proxy.Inference.TimeoutSeconds <= 0 {
		fc.Proxy.Inference.TimeoutSeconds = 120
	}
	if fc.Proxy.Inference.StoreBufferBytes == 0 {
		fc.Proxy.Inference.StoreBufferBytes = 65536
	}
	if fc.Charon.Listen == "" {
		fc.Charon.Listen = ":8081"
	}
	if fc.Charon.Storage.Backend == "" {
		fc.Charon.Storage.Backend = "memory"
	}
	if fc.Charon.Storage.DataDir == "" {
		fc.Charon.Storage.DataDir = "./data"
	}
	if fc.Charon.Storage.CheckpointInterval <= 0 {
		fc.Charon.Storage.CheckpointInterval = 10
	}
	if fc.Charon.Storage.TTLDays <= 0 {
		fc.Charon.Storage.TTLDays = 30
	}
	if fc.Charon.Storage.WriteIntentStaleThreshold <= 0 {
		fc.Charon.Storage.WriteIntentStaleThreshold = 5 * time.Minute
	}
	if fc.Charon.Workers.TTLInterval <= 0 {
		fc.Charon.Workers.TTLInterval = time.Hour
	}
	if fc.Charon.Workers.RecoveryInterval <= 0 {
		fc.Charon.Workers.RecoveryInterval = 5 * time.Minute
	}
	if fc.Telemetry.CharonService == "" {
		fc.Telemetry.CharonService = "charon"
	}
	if fc.Telemetry.ProxyService == "" {
		fc.Telemetry.ProxyService = "charon-proxy"
	}
}

// loadFileConfig reads and parses a YAML config file, applying defaults.
func loadFileConfig(path string) (fileConfig, error) {
	var fc fileConfig
	applyFileDefaults(&fc)

	data, err := os.ReadFile(path)
	if err != nil {
		return fc, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.UnmarshalStrict(data, &fc); err != nil {
		return fc, fmt.Errorf("parse config: %w", err)
	}
	// Second pass of defaults to fill any gaps after file parse.
	applyFileDefaults(&fc)
	return fc, nil
}

// ServerOptions holds the full configuration for the charon server command.
// It mirrors the Config struct but exposes AddFlags/Complete/Validate and
// owns the merge between CLI flags and config-file values.
type ServerOptions struct {
	// Config file path — set by --config flag.
	ConfigFile string

	// Proxy settings.
	ProxyEnabled   bool
	ProxyListen    string
	ProxyBackend   string // inference base URL
	ProxyAPIKey    string
	ProxyCharonURL string

	// Inference sub-settings (config-file only — deep tuning knobs).
	InferenceTimeoutSeconds   int
	InferenceStoreBufferBytes int

	// Charon internal API.
	CharonListen string

	// Storage (shared with ReconcileOptions via the same field set).
	Storage StorageOptions

	// Worker settings (config-file only).
	WorkerTTLInterval      time.Duration
	WorkerRecoveryInterval time.Duration

	// Telemetry settings.
	Telemetry TelemetryOptions
}

// StorageOptions holds storage settings shared between ServerOptions and ReconcileOptions.
type StorageOptions struct {
	Backend        string
	IndexBackend   string
	PayloadBackend string
	DataDir        string

	// Config-file-only knobs.
	CheckpointInterval        int
	TTLDays                   int
	WriteIntentStaleThreshold time.Duration
	MaxResponses              int64
	MaxPayload                ByteSize

	Postgres PostgresConfig
	S3       S3Config
}

// TelemetryOptions holds OpenTelemetry settings for the server.
type TelemetryOptions struct {
	ExporterURL   string // OTLP HTTP endpoint; empty = disabled
	CharonService string // default "charon"
	ProxyService  string // default "charon-proxy"
}

// ToTelemetryConfig converts TelemetryOptions to TelemetryConfig.
func (t *TelemetryOptions) ToTelemetryConfig() TelemetryConfig {
	return TelemetryConfig{
		ExporterURL:   t.ExporterURL,
		CharonService: t.CharonService,
		ProxyService:  t.ProxyService,
	}
}

// ReconcileOptions holds configuration for the reconcile subcommand.
type ReconcileOptions struct {
	// Config file path — set by --config flag.
	ConfigFile string

	// Storage is the only piece the reconcile command cares about.
	Storage StorageOptions
}

// NewServerOptions returns a ServerOptions pre-filled with built-in defaults.
func NewServerOptions() *ServerOptions {
	return &ServerOptions{
		ProxyListen:               ":8080",
		ProxyBackend:              "http://localhost:11434",
		InferenceTimeoutSeconds:   120,
		InferenceStoreBufferBytes: 65536,
		CharonListen:              ":8081",
		Storage: StorageOptions{
			Backend:                   "memory",
			DataDir:                   "./data",
			CheckpointInterval:        10,
			TTLDays:                   30,
			WriteIntentStaleThreshold: 5 * time.Minute,
		},
		WorkerTTLInterval:      time.Hour,
		WorkerRecoveryInterval: 5 * time.Minute,
		Telemetry: TelemetryOptions{
			CharonService: "charon",
			ProxyService:  "charon-proxy",
		},
	}
}

// NewReconcileOptions returns a ReconcileOptions pre-filled with built-in defaults.
func NewReconcileOptions() *ReconcileOptions {
	return &ReconcileOptions{
		Storage: StorageOptions{
			Backend:                   "memory",
			DataDir:                   "./data",
			CheckpointInterval:        10,
			TTLDays:                   30,
			WriteIntentStaleThreshold: 5 * time.Minute,
		},
	}
}

// AddFlags registers CLI flags on fs. Only commonly-overridden options are
// exposed as flags; deep tuning knobs remain config-file-only.
func (o *ServerOptions) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&o.CharonListen, "listen", o.CharonListen, "charon internal API listen address")
	fs.StringVar(&o.ProxyListen, "proxy-listen", o.ProxyListen, "proxy server listen address")
	fs.BoolVar(&o.ProxyEnabled, "proxy", o.ProxyEnabled, "enable the proxy layer")
	fs.StringVar(&o.ProxyBackend, "backend", o.ProxyBackend, "inference backend base URL")
	fs.StringVar(&o.Storage.Backend, "storage-backend", o.Storage.Backend, "storage backend (memory|sqlite|postgres|postgres+s3)")
	fs.StringVar(&o.Telemetry.ExporterURL, "telemetry-exporter-url", o.Telemetry.ExporterURL, "OTLP HTTP exporter URL (e.g. http://localhost:4318); empty disables tracing")
}

// AddFlags registers CLI flags on fs for the reconcile subcommand.
func (o *ReconcileOptions) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&o.Storage.Backend, "storage-backend", o.Storage.Backend, "storage backend (memory|sqlite|postgres|postgres+s3)")
}

// Complete loads the config file (if --config was set) and merges file values
// into the options struct. CLI flags take precedence: any flag that was
// explicitly set by the user (detected via fs.Visit) is not overwritten.
// Complete also resolves derived values such as ProxyCharonURL.
func (o *ServerOptions) Complete(fs *flag.FlagSet) error {
	if o.ConfigFile == "" {
		// No config file — derive ProxyCharonURL from built-in defaults and return.
		if o.ProxyCharonURL == "" {
			o.ProxyCharonURL = deriveCharonURL(o.CharonListen)
		}
		return nil
	}

	fc, err := loadFileConfig(o.ConfigFile)
	if err != nil {
		return err
	}

	// Collect which flags were explicitly set on the command line.
	setFlags := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { setFlags[f.Name] = true })

	// Merge file values into options, skipping fields covered by explicit flags.
	if !setFlags["proxy"] {
		o.ProxyEnabled = fc.Proxy.Enabled
	}
	if !setFlags["proxy-listen"] {
		o.ProxyListen = fc.Proxy.Listen
	}
	if !setFlags["backend"] {
		o.ProxyBackend = fc.Proxy.Inference.BaseURL
	}
	// Always take API key from file (not a CLI flag).
	o.ProxyAPIKey = fc.Proxy.Inference.APIKey
	// Deep inference knobs are config-file-only.
	o.InferenceTimeoutSeconds = fc.Proxy.Inference.TimeoutSeconds
	o.InferenceStoreBufferBytes = fc.Proxy.Inference.StoreBufferBytes

	if !setFlags["listen"] {
		o.CharonListen = fc.Charon.Listen
	}
	if !setFlags["storage-backend"] {
		o.Storage.Backend = fc.Charon.Storage.Backend
	}

	// All remaining storage fields are config-file-only.
	o.Storage.IndexBackend = fc.Charon.Storage.IndexBackend
	o.Storage.PayloadBackend = fc.Charon.Storage.PayloadBackend
	o.Storage.DataDir = fc.Charon.Storage.DataDir
	o.Storage.CheckpointInterval = fc.Charon.Storage.CheckpointInterval
	o.Storage.TTLDays = fc.Charon.Storage.TTLDays
	o.Storage.WriteIntentStaleThreshold = fc.Charon.Storage.WriteIntentStaleThreshold
	o.Storage.MaxResponses = fc.Charon.Storage.MaxResponses
	o.Storage.MaxPayload = fc.Charon.Storage.MaxPayload
	o.Storage.Postgres = fc.Charon.Storage.Postgres
	o.Storage.S3 = fc.Charon.Storage.S3

	// Worker settings are config-file-only.
	o.WorkerTTLInterval = fc.Charon.Workers.TTLInterval
	o.WorkerRecoveryInterval = fc.Charon.Workers.RecoveryInterval

	// Derive ProxyCharonURL: file value takes precedence, then auto-derive.
	if !setFlags["proxy-charon-url"] {
		if fc.Proxy.CharonURL != "" {
			o.ProxyCharonURL = fc.Proxy.CharonURL
		} else {
			o.ProxyCharonURL = deriveCharonURL(o.CharonListen)
		}
	}

	// Telemetry settings are config-file-only except exporter URL which has a flag.
	if !setFlags["telemetry-exporter-url"] {
		o.Telemetry.ExporterURL = fc.Telemetry.ExporterURL
	}
	o.Telemetry.CharonService = fc.Telemetry.CharonService
	o.Telemetry.ProxyService = fc.Telemetry.ProxyService

	return nil
}

// Complete loads the config file (if --config was set) and merges file values
// into the ReconcileOptions. CLI flags take precedence.
func (o *ReconcileOptions) Complete(fs *flag.FlagSet) error {
	if o.ConfigFile == "" {
		return nil
	}

	fc, err := loadFileConfig(o.ConfigFile)
	if err != nil {
		return err
	}

	setFlags := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { setFlags[f.Name] = true })

	if !setFlags["storage-backend"] {
		o.Storage.Backend = fc.Charon.Storage.Backend
	}
	o.Storage.IndexBackend = fc.Charon.Storage.IndexBackend
	o.Storage.PayloadBackend = fc.Charon.Storage.PayloadBackend
	o.Storage.DataDir = fc.Charon.Storage.DataDir
	o.Storage.CheckpointInterval = fc.Charon.Storage.CheckpointInterval
	o.Storage.TTLDays = fc.Charon.Storage.TTLDays
	o.Storage.WriteIntentStaleThreshold = fc.Charon.Storage.WriteIntentStaleThreshold
	o.Storage.MaxResponses = fc.Charon.Storage.MaxResponses
	o.Storage.MaxPayload = fc.Charon.Storage.MaxPayload
	o.Storage.Postgres = fc.Charon.Storage.Postgres
	o.Storage.S3 = fc.Charon.Storage.S3

	return nil
}

// Validate checks ServerOptions for invalid combinations. It performs no I/O.
func (o *ServerOptions) Validate() error {
	// Validate storage backend.
	if err := o.Storage.validate(); err != nil {
		return err
	}

	// Proxy-specific validation.
	if o.ProxyEnabled && o.ProxyBackend == "" {
		return fmt.Errorf("proxy enabled but proxy backend (inference base URL) is empty")
	}

	return nil
}

// Validate checks ReconcileOptions for invalid combinations. It performs no I/O.
func (o *ReconcileOptions) Validate() error {
	return o.Storage.validate()
}

// validate checks StorageOptions for invalid combinations.
func (s *StorageOptions) validate() error {
	effectiveIndex, effectivePayload := resolveBackends(s.Backend, s.IndexBackend, s.PayloadBackend)

	if effectiveIndex == "postgres" || effectivePayload == "postgres" {
		if s.Postgres.DSN == "" {
			return fmt.Errorf("storage backend %q requires postgres.dsn to be set", s.Backend)
		}
	}
	if effectivePayload == "s3" {
		if s.S3.Bucket == "" {
			return fmt.Errorf("storage backend %q requires s3.bucket to be set", s.Backend)
		}
	}
	return nil
}

// resolveBackends returns the effective index and payload backend names from
// the legacy Backend field and the explicit IndexBackend/PayloadBackend overrides.
// This mirrors the logic in openStorage in main.go.
func resolveBackends(backend, indexBackend, payloadBackend string) (string, string) {
	if indexBackend == "" || payloadBackend == "" {
		switch backend {
		case "sqlite":
			if indexBackend == "" {
				indexBackend = "sqlite"
			}
			if payloadBackend == "" {
				payloadBackend = "filesystem"
			}
		case "postgres":
			if indexBackend == "" {
				indexBackend = "postgres"
			}
			if payloadBackend == "" {
				payloadBackend = "filesystem"
			}
		case "postgres+s3":
			if indexBackend == "" {
				indexBackend = "postgres"
			}
			if payloadBackend == "" {
				payloadBackend = "s3"
			}
		default: // "memory"
			if indexBackend == "" {
				indexBackend = "memory"
			}
			if payloadBackend == "" {
				payloadBackend = "memory"
			}
		}
	}
	return indexBackend, payloadBackend
}

// ToStorageConfig converts StorageOptions to the StorageConfig used by existing
// storage-opening code.
func (s *StorageOptions) ToStorageConfig() StorageConfig {
	return StorageConfig{
		Backend:                   s.Backend,
		IndexBackend:              s.IndexBackend,
		PayloadBackend:            s.PayloadBackend,
		DataDir:                   s.DataDir,
		CheckpointInterval:        s.CheckpointInterval,
		TTLDays:                   s.TTLDays,
		WriteIntentStaleThreshold: s.WriteIntentStaleThreshold,
		MaxResponses:              s.MaxResponses,
		MaxPayload:                s.MaxPayload,
		Postgres:                  s.Postgres,
		S3:                        s.S3,
	}
}

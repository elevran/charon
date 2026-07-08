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
	Listen    string              `json:"listen"`
	CharonURL string              `json:"charon_url"`
	Inference fileInferenceConfig `json:"inference"`
}

type fileInferenceConfig struct {
	BaseURL        string `json:"base_url"`
	APIKey         string `json:"api_key"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

type fileCharonConfig struct {
	Listen  string            `json:"listen"`
	Storage fileStorageConfig `json:"storage"`
	Workers fileWorkerConfig  `json:"workers"`
}

type fileStorageConfig struct {
	DataDir         string   `json:"data_dir"`
	TTLDays         int      `json:"ttl_days"`
	MaxResponses    int64    `json:"max_responses"`
	MaxPayload      ByteSize `json:"max_payload"`
	MaxChainDepth   int      `json:"max_chain_depth"`
	MaxContextBytes ByteSize `json:"max_context_bytes"`
}

type fileWorkerConfig struct {
	// TTLInterval is how often the background reaper runs, not the TTL itself.
	// Maps to chainstore.Config.TTLInterval.
	TTLInterval time.Duration `json:"ttl_interval"`
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
	if fc.Charon.Listen == "" {
		fc.Charon.Listen = ":8081"
	}
	if fc.Charon.Storage.DataDir == "" {
		fc.Charon.Storage.DataDir = "./data"
	}
	if fc.Charon.Storage.TTLDays <= 0 {
		fc.Charon.Storage.TTLDays = 30
	}
	if fc.Charon.Workers.TTLInterval <= 0 {
		fc.Charon.Workers.TTLInterval = time.Hour
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

// ---------------------------------------------------------------------------
// TelemetryOptions (shared)
// ---------------------------------------------------------------------------

// TelemetryOptions holds OpenTelemetry settings.
type TelemetryOptions struct {
	ExporterURL string // OTLP HTTP endpoint; empty = disabled
	ServiceName string // identifies this binary in traces
}

// AddFlags registers the --telemetry-exporter-url flag on fs.
func (o *TelemetryOptions) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.ExporterURL, "telemetry-exporter-url", o.ExporterURL, "OTLP HTTP exporter URL (e.g. http://localhost:4318); empty disables tracing")
}

// visitedFlags returns a set of flag names that were explicitly set on fs.
func visitedFlags(fs *flag.FlagSet) map[string]bool {
	m := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { m[f.Name] = true })
	return m
}

// ---------------------------------------------------------------------------
// CharonOptions
// ---------------------------------------------------------------------------

// CharonOptions holds configuration for the Charon response-storage server.
type CharonOptions struct {
	// Config file path — set by --config flag.
	ConfigFile string

	// Listen address for the Charon internal API.
	Listen string

	// Storage settings.
	DataDir string
	// TTLDays is the maximum age of a stored response. Maps to chainstore.Config.TTL.
	TTLDays         int
	MaxResponses    int64
	MaxPayload      ByteSize
	MaxChainDepth   int
	MaxContextBytes ByteSize

	// WorkerTTLInterval is how often the background TTL reaper runs (not the TTL itself).
	WorkerTTLInterval time.Duration

	Telemetry TelemetryOptions
}

// NewCharonOptions returns a CharonOptions pre-filled with built-in defaults.
func NewCharonOptions() *CharonOptions {
	return &CharonOptions{
		Listen:            ":8081",
		DataDir:           "./data",
		TTLDays:           30,
		WorkerTTLInterval: time.Hour,
		Telemetry:         TelemetryOptions{ServiceName: "charon"},
	}
}

// AddFlags registers CLI flags on fs for the Charon server.
func (o *CharonOptions) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&o.Listen, "listen", o.Listen, "charon internal API listen address")
	fs.StringVar(&o.DataDir, "storage-data-dir", o.DataDir, "data directory for Pebble storage")
	o.Telemetry.AddFlags(fs)
}

// Complete loads the config file (if --config was set) and merges file values
// into the options struct. CLI flags take precedence over config-file values.
func (o *CharonOptions) Complete(fs *flag.FlagSet) error {
	if o.ConfigFile == "" {
		return nil
	}

	fc, err := loadFileConfig(o.ConfigFile)
	if err != nil {
		return err
	}

	setFlags := visitedFlags(fs)

	if !setFlags["listen"] {
		o.Listen = fc.Charon.Listen
	}
	if !setFlags["storage-data-dir"] {
		o.DataDir = fc.Charon.Storage.DataDir
	}

	// Storage fields are config-file-only.
	o.TTLDays = fc.Charon.Storage.TTLDays
	o.MaxResponses = fc.Charon.Storage.MaxResponses
	o.MaxPayload = fc.Charon.Storage.MaxPayload
	o.MaxChainDepth = fc.Charon.Storage.MaxChainDepth
	o.MaxContextBytes = fc.Charon.Storage.MaxContextBytes

	// Worker settings are config-file-only.
	o.WorkerTTLInterval = fc.Charon.Workers.TTLInterval

	if !setFlags["telemetry-exporter-url"] {
		o.Telemetry.ExporterURL = fc.Telemetry.ExporterURL
	}
	o.Telemetry.ServiceName = fc.Telemetry.CharonService

	return nil
}

// Validate checks CharonOptions for invalid combinations. It performs no I/O.
func (o *CharonOptions) Validate() error {
	if o.DataDir == "" {
		return fmt.Errorf("charon storage data-dir is empty")
	}
	return nil
}

// ---------------------------------------------------------------------------
// ProxyOptions
// ---------------------------------------------------------------------------

// ProxyOptions holds configuration for the proxy server.
type ProxyOptions struct {
	// Config file path — set by --config flag.
	ConfigFile string

	// Listen address for the proxy HTTP server.
	Listen string

	// Inference backend settings.
	Backend        string // base URL
	APIKey         string
	TimeoutSeconds int

	// CharonURL is the Charon internal API endpoint the proxy calls.
	// Auto-derived from config file proxy.charon_url or Charon's listen address.
	CharonURL string

	Telemetry TelemetryOptions
}

// NewProxyOptions returns a ProxyOptions pre-filled with built-in defaults.
func NewProxyOptions() *ProxyOptions {
	return &ProxyOptions{
		Listen:         ":8080",
		Backend:        "http://localhost:11434",
		TimeoutSeconds: 120,
		CharonURL:      "http://127.0.0.1:8081",
		Telemetry:      TelemetryOptions{ServiceName: "charon-proxy"},
	}
}

// AddFlags registers CLI flags on fs for the proxy server.
func (o *ProxyOptions) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&o.Listen, "listen", o.Listen, "proxy server listen address")
	fs.StringVar(&o.Backend, "backend", o.Backend, "inference backend base URL")
	fs.StringVar(&o.CharonURL, "charon-url", o.CharonURL, "charon internal API base URL")
	o.Telemetry.AddFlags(fs)
}

// Complete loads the config file (if --config was set) and merges file values
// into the options struct. CLI flags take precedence over config-file values.
func (o *ProxyOptions) Complete(fs *flag.FlagSet) error {
	if o.ConfigFile == "" {
		return nil
	}

	fc, err := loadFileConfig(o.ConfigFile)
	if err != nil {
		return err
	}

	setFlags := visitedFlags(fs)

	if !setFlags["listen"] {
		o.Listen = fc.Proxy.Listen
	}
	if !setFlags["backend"] {
		o.Backend = fc.Proxy.Inference.BaseURL
	}
	// API key and timeout are config-file-only.
	o.APIKey = fc.Proxy.Inference.APIKey
	o.TimeoutSeconds = fc.Proxy.Inference.TimeoutSeconds

	if !setFlags["charon-url"] {
		if fc.Proxy.CharonURL != "" {
			o.CharonURL = fc.Proxy.CharonURL
		} else {
			o.CharonURL = deriveCharonURL(fc.Charon.Listen)
		}
	}

	if !setFlags["telemetry-exporter-url"] {
		o.Telemetry.ExporterURL = fc.Telemetry.ExporterURL
	}
	o.Telemetry.ServiceName = fc.Telemetry.ProxyService

	return nil
}

// Validate checks ProxyOptions for invalid combinations. It performs no I/O.
func (o *ProxyOptions) Validate() error {
	if o.Backend == "" {
		return fmt.Errorf("proxy backend (inference base URL) is empty")
	}
	return nil
}

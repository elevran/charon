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

// ServerOptions holds the full configuration for the charon server command.
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
	InferenceTimeoutSeconds int

	// Charon internal API.
	CharonListen string

	// Storage.
	DataDir string
	// TTLDays is the maximum age of a stored response. Maps to chainstore.Config.TTL.
	TTLDays         int
	MaxResponses    int64
	MaxPayload      ByteSize
	MaxChainDepth   int
	MaxContextBytes ByteSize

	// WorkerTTLInterval is how often the background TTL reaper runs (not the TTL itself).
	// Maps to chainstore.Config.TTLInterval.
	WorkerTTLInterval time.Duration

	// Telemetry settings.
	Telemetry TelemetryOptions
}

// TelemetryOptions holds OpenTelemetry settings for the server.
type TelemetryOptions struct {
	ExporterURL   string // OTLP HTTP endpoint; empty = disabled
	CharonService string // default "charon"
	ProxyService  string // default "charon-proxy"
}

// NewServerOptions returns a ServerOptions pre-filled with built-in defaults.
func NewServerOptions() *ServerOptions {
	return &ServerOptions{
		ProxyListen:             ":8080",
		ProxyBackend:            "http://localhost:11434",
		InferenceTimeoutSeconds: 120,
		CharonListen:            ":8081",
		DataDir:                 "./data",
		TTLDays:                 30,
		WorkerTTLInterval:       time.Hour,
		Telemetry: TelemetryOptions{
			CharonService: "charon",
			ProxyService:  "charon-proxy",
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
	fs.StringVar(&o.DataDir, "storage-data-dir", o.DataDir, "data directory for Pebble storage")
	fs.StringVar(&o.Telemetry.ExporterURL, "telemetry-exporter-url", o.Telemetry.ExporterURL, "OTLP HTTP exporter URL (e.g. http://localhost:4318); empty disables tracing")
}

// Complete loads the config file (if --config was set) and merges file values
// into the options struct. CLI flags take precedence over config-file values.
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

	if !setFlags["listen"] {
		o.CharonListen = fc.Charon.Listen
	}
	if !setFlags["storage-data-dir"] {
		o.DataDir = fc.Charon.Storage.DataDir
	}

	// All remaining storage fields are config-file-only.
	o.TTLDays = fc.Charon.Storage.TTLDays
	o.MaxResponses = fc.Charon.Storage.MaxResponses
	o.MaxPayload = fc.Charon.Storage.MaxPayload
	o.MaxChainDepth = fc.Charon.Storage.MaxChainDepth
	o.MaxContextBytes = fc.Charon.Storage.MaxContextBytes

	// Worker settings are config-file-only.
	o.WorkerTTLInterval = fc.Charon.Workers.TTLInterval

	// Derive ProxyCharonURL: file value takes precedence, then auto-derive.
	if fc.Proxy.CharonURL != "" {
		o.ProxyCharonURL = fc.Proxy.CharonURL
	} else {
		o.ProxyCharonURL = deriveCharonURL(o.CharonListen)
	}

	// Telemetry settings are config-file-only except exporter URL which has a flag.
	if !setFlags["telemetry-exporter-url"] {
		o.Telemetry.ExporterURL = fc.Telemetry.ExporterURL
	}
	o.Telemetry.CharonService = fc.Telemetry.CharonService
	o.Telemetry.ProxyService = fc.Telemetry.ProxyService

	return nil
}

// Validate checks ServerOptions for invalid combinations. It performs no I/O.
func (o *ServerOptions) Validate() error {
	if o.ProxyEnabled && o.ProxyBackend == "" {
		return fmt.Errorf("proxy enabled but proxy backend (inference base URL) is empty")
	}
	return nil
}

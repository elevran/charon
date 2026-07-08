package proxyconfig

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"

	"sigs.k8s.io/yaml"
)

// TelemetryOptions holds OpenTelemetry settings.
type TelemetryOptions struct {
	ExporterURL string // OTLP HTTP endpoint; empty = disabled
	ServiceName string // identifies this binary in traces
}

// AddFlags registers the --telemetry-exporter-url flag on fs.
func (o *TelemetryOptions) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.ExporterURL, "telemetry-exporter-url", o.ExporterURL, "OTLP HTTP exporter URL (e.g. http://localhost:4318); empty disables tracing")
}

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
			o.CharonURL = deriveCharonURL(fc.CharonListen)
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

// ---------------------------------------------------------------------------
// private YAML loader
// ---------------------------------------------------------------------------

type fileConfig struct {
	Proxy        fileProxyConfig     `json:"proxy"`
	CharonListen string              `json:"_charon_listen"` // populated from charon.listen after parse
	Telemetry    fileTelemetryConfig `json:"telemetry"`
}

// fileConfigRaw is the raw YAML shape; we flatten charon.listen into fileConfig.
type fileConfigRaw struct {
	Proxy     fileProxyConfig     `json:"proxy"`
	Charon    fileCharonSection   `json:"charon"`
	Telemetry fileTelemetryConfig `json:"telemetry"`
}

type fileCharonSection struct {
	Listen  string          `json:"listen"`
	Storage json.RawMessage `json:"storage"`
	Workers json.RawMessage `json:"workers"`
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

type fileTelemetryConfig struct {
	ExporterURL   string `json:"exporter_url"`
	ProxyService  string `json:"proxy_service"`
	CharonService string `json:"charon_service"` // accepted but unused — avoids strict-parse rejection of shared config files
}

func applyFileDefaults(raw *fileConfigRaw) {
	if raw.Proxy.Listen == "" {
		raw.Proxy.Listen = ":8080"
	}
	if raw.Proxy.Inference.BaseURL == "" {
		raw.Proxy.Inference.BaseURL = "http://localhost:11434"
	}
	if raw.Proxy.Inference.TimeoutSeconds <= 0 {
		raw.Proxy.Inference.TimeoutSeconds = 120
	}
	if raw.Charon.Listen == "" {
		raw.Charon.Listen = ":8081"
	}
	if raw.Telemetry.ProxyService == "" {
		raw.Telemetry.ProxyService = "charon-proxy"
	}
}

func loadFileConfig(path string) (fileConfig, error) {
	var raw fileConfigRaw
	applyFileDefaults(&raw)

	data, err := os.ReadFile(path)
	if err != nil {
		return fileConfig{}, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.UnmarshalStrict(data, &raw); err != nil {
		return fileConfig{}, fmt.Errorf("parse config: %w", err)
	}
	applyFileDefaults(&raw)

	return fileConfig{
		Proxy:        raw.Proxy,
		CharonListen: raw.Charon.Listen,
		Telemetry:    raw.Telemetry,
	}, nil
}

func visitedFlags(fs *flag.FlagSet) map[string]bool {
	m := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { m[f.Name] = true })
	return m
}

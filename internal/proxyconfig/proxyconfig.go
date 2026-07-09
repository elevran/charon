package proxyconfig

import (
	"flag"
	"fmt"
	"net"
	"os"

	"sigs.k8s.io/yaml"

	"github.com/elevran/charon/internal/telemetry"
)

// defaultMaxChunkBytes is the default cap for chunkedResponseWriter (1 MiB).
const defaultMaxChunkBytes int64 = 1 << 20

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

	// MaxChunkBytes caps the in-memory response buffer before flushing to
	// Charon as a chunk. 0 or negative applies the default (1 MiB).
	MaxChunkBytes int64

	Telemetry telemetry.Options
}

// NewOptions returns a ProxyOptions pre-filled with built-in defaults.
func NewOptions() *ProxyOptions {
	return &ProxyOptions{
		Listen:         ":8080",
		Backend:        "http://localhost:11434",
		TimeoutSeconds: 120,
		CharonURL:      "http://127.0.0.1:8081",
		Telemetry:      telemetry.Options{ServiceName: "charon-proxy"},
	}
}

// AddFlags registers CLI flags on fs for the proxy server.
func (o *ProxyOptions) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&o.Listen, "listen", o.Listen, "proxy server listen address")
	fs.StringVar(&o.Backend, "backend", o.Backend, "inference backend base URL")
	fs.StringVar(&o.CharonURL, "charon-url", o.CharonURL, "charon internal API base URL")
	fs.Int64Var(&o.MaxChunkBytes, "max-chunk-bytes", 0,
		"max response bytes buffered before flushing to Charon as a chunk (0 = default 1 MiB)")
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
	o.Telemetry.ServiceName = fc.Telemetry.ServiceName

	if o.MaxChunkBytes <= 0 {
		o.MaxChunkBytes = defaultMaxChunkBytes
	}

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
	Proxy     fileProxyConfig     `json:"proxy"`
	Charon    fileCharonSection   `json:"charon"`
	Telemetry fileTelemetryConfig `json:"telemetry"`
}

// fileCharonSection holds only the Charon fields the proxy needs: the listen
// address (used to derive CharonURL). Charon-specific storage and worker
// config belongs in the Charon config file, never here.
type fileCharonSection struct {
	Listen string `json:"listen"`
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
	ExporterURL string `json:"exporter_url"`
	ServiceName string `json:"service_name"`
}

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
	if fc.Telemetry.ServiceName == "" {
		fc.Telemetry.ServiceName = "charon-proxy"
	}
}

func loadFileConfig(path string) (fileConfig, error) {
	var fc fileConfig
	data, err := os.ReadFile(path)
	if err != nil {
		return fc, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.UnmarshalStrict(data, &fc); err != nil {
		return fc, fmt.Errorf("parse config: %w", err)
	}
	applyFileDefaults(&fc)
	return fc, nil
}

func visitedFlags(fs *flag.FlagSet) map[string]bool {
	m := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { m[f.Name] = true })
	return m
}

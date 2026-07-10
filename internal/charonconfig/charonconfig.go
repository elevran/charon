package charonconfig

import (
	"flag"
	"fmt"
	"os"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/elevran/charon/internal/bytesize"
	"github.com/elevran/charon/internal/telemetry"
)

// CharonOptions holds configuration for the Charon response-storage server.
type CharonOptions struct {
	// Config file path — set by --config flag.
	ConfigFile string

	// Listen address for the Charon internal API.
	Listen string

	// Storage settings.
	DataDir string
	// TTLDays is the maximum age of a stored response.
	TTLDays         int
	MaxResponses    int64
	MaxPayload      bytesize.ByteSize
	MaxChainDepth   int
	MaxContextBytes bytesize.ByteSize

	// WorkerTTLInterval is how often the background TTL reaper runs (not the TTL itself).
	WorkerTTLInterval time.Duration

	Telemetry telemetry.Options
}

// NewOptions returns a CharonOptions pre-filled with built-in defaults.
func NewOptions() *CharonOptions {
	return &CharonOptions{
		Listen:            ":8081",
		DataDir:           "./data",
		TTLDays:           30,
		WorkerTTLInterval: time.Hour,
		Telemetry:         telemetry.Options{ServiceName: "charon"},
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
	o.Telemetry.ServiceName = fc.Telemetry.ServiceName

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
// private YAML loader
// ---------------------------------------------------------------------------

type fileConfig struct {
	Charon    fileCharonConfig    `json:"charon"`
	Telemetry fileTelemetryConfig `json:"telemetry"`
}

type fileCharonConfig struct {
	Listen  string            `json:"listen"`
	Storage fileStorageConfig `json:"storage"`
	Workers fileWorkerConfig  `json:"workers"`
}

type fileStorageConfig struct {
	DataDir         string            `json:"data_dir"`
	TTLDays         int               `json:"ttl_days"`
	MaxResponses    int64             `json:"max_responses"`
	MaxPayload      bytesize.ByteSize `json:"max_payload"`
	MaxChainDepth   int               `json:"max_chain_depth"`
	MaxContextBytes bytesize.ByteSize `json:"max_context_bytes"`
}

type fileWorkerConfig struct {
	TTLInterval time.Duration `json:"ttl_interval"`
}

type fileTelemetryConfig struct {
	ExporterURL string `json:"exporter_url"`
	ServiceName string `json:"service_name"`
}

func applyFileDefaults(fc *fileConfig) {
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
	if fc.Telemetry.ServiceName == "" {
		fc.Telemetry.ServiceName = "charon"
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

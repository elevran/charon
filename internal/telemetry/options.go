package telemetry

import "flag"

// Options holds OpenTelemetry configuration for a single service.
type Options struct {
	ExporterURL string // OTLP HTTP endpoint; empty = disabled
	ServiceName string // identifies this binary in traces
}

// AddFlags registers the --telemetry-exporter-url flag on fs.
func (o *Options) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.ExporterURL, "telemetry-exporter-url", o.ExporterURL, "OTLP HTTP exporter URL (e.g. http://localhost:4318); empty disables tracing")
}

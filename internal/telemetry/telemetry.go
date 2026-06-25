// Package telemetry wires up OpenTelemetry tracing for Charon and its proxy.
// When exporterURL is empty the call is a no-op and the default (no-op)
// global TracerProvider is left in place — zero overhead when disabled.
package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const (
	maxQueueSize      = 512 // bounded export queue — prevents unbounded memory growth
	maxExportBatch    = 256
	maxAttributeCount = 64 // per-span attribute cap
	maxEventCount     = 16 // per-span event cap
	exportTimeout     = 5 * time.Second
)

// Init creates and returns a TracerProvider for serviceName that exports to
// exporterURL via OTLP HTTP. It does NOT register the provider as global;
// callers pass it explicitly to otelhttp and store.Config so that two
// providers (one for Charon, one for the proxy) can coexist in the same
// process without interfering.
//
// Returns nil when exporterURL is empty — callers should guard and skip
// shutdown.
func Init(ctx context.Context, serviceName, exporterURL string) (*sdktrace.TracerProvider, error) {
	if exporterURL == "" {
		return nil, nil
	}

	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(exporterURL),
		otlptracehttp.WithTimeout(exportTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("create OTLP HTTP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("create OTel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		// AlwaysSample is the default; making it explicit so it's visible here.
		// For production, configure tail-based or probabilistic sampling at the
		// collector rather than here — the bounded queue (maxQueueSize) is the
		// primary guard against memory growth when sampling is not configured.
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exp,
			sdktrace.WithMaxQueueSize(maxQueueSize),
			sdktrace.WithMaxExportBatchSize(maxExportBatch),
		),
		sdktrace.WithResource(res),
		sdktrace.WithRawSpanLimits(sdktrace.SpanLimits{
			AttributeCountLimit:         maxAttributeCount,
			EventCountLimit:             maxEventCount,
			LinkCountLimit:              sdktrace.DefaultLinkCountLimit,
			AttributePerEventCountLimit: sdktrace.DefaultAttributePerEventCountLimit,
			AttributePerLinkCountLimit:  sdktrace.DefaultAttributePerLinkCountLimit,
		}),
	)
	return tp, nil
}

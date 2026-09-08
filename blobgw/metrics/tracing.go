// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package metrics

// tracing.go stands up blobgw's OpenTelemetry TracerProvider — the trace side of
// the same telemetry the MeterProvider in metrics.go exports. The two share an
// endpoint (the OTLP collector), a resource (service.name=blobgw + this node's
// service.instance.id), and the same gating: traces are OFF unless an OTLP
// endpoint is configured. There is deliberately no Prometheus equivalent — a
// trace needs a collector to land in, so the OTLP endpoint is the only switch.
//
// Once NewTracing registers its provider globally (otel.SetTracerProvider), the
// casstore backing-store spans (casstore.backing.*, opened via the GLOBAL tracer
// provider in blobstore/obs) and blobgw's own request spans are exported and the
// data-path latency histograms — already recorded within request context — get
// trace exemplars for free. When tracing is disabled the global provider stays
// the default no-op, so every span site is a cheap no-op (zero overhead).
// See docs/OBSERVABILITY.md.

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// buildResource builds the OTEL resource shared by the meter and tracer
// providers: service.name=blobgw plus this node's service.instance.id (so a
// central collector tells replicas apart). It is the single source of the
// resource identity so metrics and traces correlate on the same target.
func buildResource(cfg config) (*resource.Resource, error) {
	name := cfg.serviceName
	if name == "" {
		name = "blobgw"
	}
	idAttrs := []attribute.KeyValue{attribute.String("service.name", name)}
	if cfg.instanceID != "" {
		idAttrs = append(idAttrs, attribute.String("service.instance.id", cfg.instanceID))
	}
	return resource.Merge(resource.Default(), resource.NewSchemaless(idAttrs...))
}

// Tracing holds blobgw's TracerProvider. Shutdown flushes and stops it; the
// caller wires Shutdown into daemon shutdown. A nil *Tracing is a no-op (tracing
// disabled), so call sites never branch on whether tracing is on.
type Tracing struct {
	provider *sdktrace.TracerProvider
}

// NewTracing builds a TracerProvider with an OTLP/gRPC trace exporter to the
// configured collector, registers it as the GLOBAL provider (so blobstore/obs
// and blobgw spans are exported), and installs a W3C TraceContext propagator so
// an upstream caller's trace continues across the layer boundary. It mirrors the
// MeterProvider's endpoint/insecure/resource and the same gating: it returns a
// nil *Tracing (no provider, no global registration) when no OTLP endpoint is
// set, leaving the global provider as the default no-op.
//
// Sampling is parent-based (a sampled upstream trace is always continued) over a
// trace-id-ratio root sampler. Since tracing is opt-in behind an explicit
// endpoint, the default ratio (WithTraceSampler unset or ≤ 0) is 1.0 — sample
// everything — matching the metrics posture of "off until you ask, then full".
func NewTracing(ctx context.Context, opts ...Option) (*Tracing, error) {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.otlpEndpoint == "" {
		// No collector → no trace provider. Spans stay global no-ops (zero overhead).
		return nil, nil
	}

	res, err := buildResource(cfg)
	if err != nil {
		return nil, fmt.Errorf("tracing: resource: %w", err)
	}

	eopts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.otlpEndpoint)}
	if cfg.otlpInsecure {
		eopts = append(eopts, otlptracegrpc.WithInsecure())
	}
	exp, err := otlptracegrpc.New(ctx, eopts...)
	if err != nil {
		return nil, fmt.Errorf("tracing: otlp exporter: %w", err)
	}

	ratio := cfg.traceSampler
	if ratio <= 0 {
		ratio = 1.0
	}
	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return &Tracing{provider: tp}, nil
}

// Shutdown flushes and stops the TracerProvider (a nil *Tracing is a no-op).
func (t *Tracing) Shutdown(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	return t.provider.Shutdown(ctx)
}

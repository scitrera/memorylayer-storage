// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// tracing.go stands up the daemon's OpenTelemetry TracerProvider — the trace
// half of the observability story (metrics.go is the metric half). The storage
// architecture is a fan-out (a cold mlfs read decomposes into a cache miss → a
// manifest read on PostgreSQL → a pack GET on S3), so tail latency lives in that
// fan-out and a request span that crosses the layer boundary is where it shows
// up (docs/OBSERVABILITY.md, "Traces"). The casstore backing wrapper
// (casstore/blobstore/obs) already opens leaf client spans on the GLOBAL tracer
// provider; the FUSE bridge opens the parent op spans on it too. This file is
// what makes those spans actually export: it builds an SDK TracerProvider over
// an OTLP/gRPC trace exporter and installs it as the global provider.
//
// Tracing is built ONLY when an OTLP endpoint is configured — unlike metrics
// (which always have the local Prometheus surfaces), a trace is useless without
// a collector to receive it. When no endpoint is set NewTracing returns a nil
// *Tracing whose every method is a no-op and the global provider stays the OTEL
// default no-op, so casstore/bridge spans cost nothing and behavior is unchanged.
package metrics

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

// Tracing owns the daemon's SDK TracerProvider and its shutdown. A nil *Tracing
// is the disabled case (no OTLP endpoint): every method is a no-op, mirroring the
// nil-safety of *Registry, so callers never branch on tracing being off.
type Tracing struct {
	provider *sdktrace.TracerProvider
}

// NewTracing builds the daemon's TracerProvider and installs it as the global
// OTEL provider (so casstore/blobstore/obs and the FUSE bridge, which both use
// otel.Tracer(...), export through it), along with a W3C trace-context
// propagator. It honors the SAME Options as metrics.New — WithOTLP supplies the
// collector endpoint (REQUIRED: with no endpoint NewTracing returns (nil, nil)
// and leaves the global no-op provider in place) and WithIdentity supplies the
// resource identity, so a trace carries the same service.name/instance/domain a
// metric stream does and a collector correlates them.
//
// sampleRatio is the head-sampling probability for a ROOT span (a span with no
// remote parent); a span whose parent is already sampled is always recorded
// (ParentBased), so a request never decomposes into a half-sampled trace. Pass
// ratio ≥ 1 to record everything, ≤ 0 for the SDK default (AlwaysSample). The
// exemplar bridge on the metric side links a sampled trace to its latency
// bucket, so the sampler also governs how many exemplars land.
func NewTracing(ctx context.Context, sampleRatio float64, opts ...Option) (*Tracing, error) {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	// No collector → no trace provider. Leave the global OTEL no-op in place so
	// casstore/bridge spans are cheap no-ops and nothing exports.
	if cfg.otlpEndpoint == "" {
		return nil, nil
	}

	// Same resource identity as the meter provider (service.name=mlfs +
	// instance/domain/region), so a collector groups a daemon's traces and metrics
	// under one producer. Kept in sync with metrics.New's idAttrs.
	idAttrs := []attribute.KeyValue{attribute.String("service.name", "mlfs")}
	if cfg.hasIdentity {
		if cfg.instanceID != "" {
			idAttrs = append(idAttrs, attribute.String("service.instance.id", cfg.instanceID))
		}
		if cfg.domain != "" {
			idAttrs = append(idAttrs, attribute.String("mlfs.domain", cfg.domain))
		}
		idAttrs = append(idAttrs, attribute.Int("mlfs.region", cfg.region))
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(idAttrs...))
	if err != nil {
		return nil, fmt.Errorf("tracing: resource: %w", err)
	}

	eopts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.otlpEndpoint)}
	if cfg.otlpInsecure {
		eopts = append(eopts, otlptracegrpc.WithInsecure())
	}
	exp, err := otlptracegrpc.New(ctx, eopts...)
	if err != nil {
		return nil, fmt.Errorf("tracing: otlp trace exporter: %w", err)
	}

	// ParentBased(TraceIDRatio): honor an upstream sampling decision when one
	// exists (so a cross-layer request stays whole), else sample a ROOT span by
	// ratio. ratio ≤ 0 (SDK default) and ratio ≥ 1 both record every root span;
	// an in-between ratio head-samples roots.
	root := sdktrace.AlwaysSample()
	if sampleRatio > 0 && sampleRatio < 1 {
		root = sdktrace.TraceIDRatioBased(sampleRatio)
	}
	sampler := sdktrace.ParentBased(root)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	// W3C trace-context propagation so a span crossing a process/RPC boundary
	// (or arriving in an incoming header) keeps one trace id.
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return &Tracing{provider: tp}, nil
}

// Shutdown flushes buffered spans and stops the provider. Nil-safe (the disabled
// case). Mirrors Registry.Shutdown so the daemon drains both on exit.
func (t *Tracing) Shutdown(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	return t.provider.Shutdown(ctx)
}

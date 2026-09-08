// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package edge

import (
	"context"
	"time"

	obsmetrics "github.com/scitrera/memorylayer-storage/blobgw/metrics"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics holds blobgw-edge's request-driven (RED) instruments for the external
// trust boundary — the data path (get/put/head/delete) and the edge's defining
// action, capability minting. It mirrors blobgw's metrics.DataPath: the same
// FINE IO bucket set (docs/OBSERVABILITY.md), the same bounded {operation,
// outcome}/{error.kind} attribute vocabulary, and the same nil-safety so call
// sites never branch on whether metrics are on.
//
// A Metrics is built over any metric.Meter — the shared "blobgw-edge" meter from
// a Registry (Registry.Meter()) when metrics are enabled, or a no-op meter when
// they are disabled — so edge.New works identically with or without metrics.
// All Record* methods are nil-safe: a nil *Metrics is a no-op.
//
// tenant (from the verified capability / asserted identity) is attached as a
// BOUNDED attribute where safe. ref / jti / hash are NEVER metric labels
// (unbounded — they are span attributes only, docs/OBSERVABILITY.md).
type Metrics struct {
	requestDuration metric.Float64Histogram // edge.request.duration {operation, outcome, tenant}
	mintDuration    metric.Float64Histogram // edge.mint.duration    {op, outcome, tenant}
	mintRejected    metric.Int64Counter     // edge.mint.rejected    {reason}
	verifyFailures  metric.Int64Counter     // edge.capability.verify_failures
	bytesPut        metric.Int64Counter     // edge.bytes_put (By) {tenant}
	bytesGet        metric.Int64Counter     // edge.bytes_get (By) {tenant}
	integrityErrors metric.Int64Counter     // edge.integrity.errors {error.kind}
}

// fineDurationBuckets (seconds) are the backing-store / DB / request IO set from
// docs/OBSERVABILITY.md: sub-millisecond local/cached ops up to a slow multi-second
// round trip. The edge data-path + mint histograms use these (not the coarse
// GC/compaction set) since edge latency is what operators tune against.
var fineDurationBuckets = []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// NewMetrics builds the edge instrument set over meter. meter must be non-nil
// (use Registry.Meter(), which returns a no-op meter when disabled). It returns
// an error only if instrument construction fails.
//
// Instruments (RED for the request-driven surfaces; integrity is the read-path canary):
//   - edge.request.duration            histogram (s)  {operation, outcome, tenant}
//   - edge.mint.duration               histogram (s)  {op, outcome, tenant}
//   - edge.mint.rejected               counter        {reason}
//   - edge.capability.verify_failures  counter
//   - edge.bytes_put                   counter (By)   {tenant}
//   - edge.bytes_get                   counter (By)   {tenant}
//   - edge.integrity.errors            counter        {error.kind}
func NewMetrics(meter metric.Meter) (*Metrics, error) {
	var ierr error
	hist := func(name, desc string) metric.Float64Histogram {
		h, err := meter.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(fineDurationBuckets...))
		if err != nil && ierr == nil {
			ierr = err
		}
		return h
	}
	ctr := func(name, desc string, opts ...metric.Int64CounterOption) metric.Int64Counter {
		c, err := meter.Int64Counter(name, append([]metric.Int64CounterOption{metric.WithDescription(desc)}, opts...)...)
		if err != nil && ierr == nil {
			ierr = err
		}
		return c
	}

	m := &Metrics{
		requestDuration: hist("edge.request.duration", "External data-path request latency, by operation + outcome + tenant"),
		mintDuration:    hist("edge.mint.duration", "Capability-mint latency, by op + outcome + tenant"),
		mintRejected:    ctr("edge.mint.rejected", "Capability-mint requests rejected before signing, by reason"),
		verifyFailures:  ctr("edge.capability.verify_failures", "Data-path capability token verify failures (the auth canary: a spike is an attack or a key-rotation issue)"),
		bytesPut:        ctr("edge.bytes_put", "Object bytes accepted via PUT, by tenant", metric.WithUnit("By")),
		bytesGet:        ctr("edge.bytes_get", "Object bytes streamed via GET, by tenant", metric.WithUnit("By")),
		integrityErrors: ctr("edge.integrity.errors", "Data-integrity faults on the read path (the corruption / GC over-deletion canary), by error kind"),
	}
	if ierr != nil {
		return nil, ierr
	}
	return m, nil
}

// tenantAttrs builds the bounded attribute set for a data-path/mint measurement:
// operation + outcome + tenant. The key is passed (data path uses "operation",
// mint uses "op") so both call sites share the taxonomy. An empty tenant is
// omitted rather than recorded as "" (keeps the label absent, not blank).
func recordAttrs(opKey, op, outcome, tenant string) metric.MeasurementOption {
	attrs := []attribute.KeyValue{attribute.String(opKey, op), attribute.String("outcome", outcome)}
	if tenant != "" {
		attrs = append(attrs, attribute.String("tenant", tenant))
	}
	return metric.WithAttributes(attrs...)
}

// RecordRequest records one data-path request: the request-latency histogram
// {operation, outcome, tenant}. operation is a LOW-CARDINALITY label
// (get/put/head/delete), NEVER the raw ref. ok is the request outcome
// (status < 400). Recorded within ctx so a sampled span yields an exemplar on
// the bucket.
func (m *Metrics) RecordRequest(ctx context.Context, op, tenant string, start time.Time, ok bool) {
	if m == nil {
		return
	}
	outcome := "ok"
	if !ok {
		outcome = "error"
	}
	m.requestDuration.Record(ctx, time.Since(start).Seconds(), recordAttrs("operation", op, outcome, tenant))
}

// RecordMint records one capability mint: the duration histogram {op, outcome,
// tenant}. op is the requested capability op (get/put/head/delete or "" when the
// request was malformed before op parsed); ok is whether a token was signed.
// Recorded within ctx so the mint span links the bucket as an exemplar.
func (m *Metrics) RecordMint(ctx context.Context, op, tenant string, start time.Time, ok bool) {
	if m == nil {
		return
	}
	outcome := "ok"
	if !ok {
		outcome = "error"
	}
	m.mintDuration.Record(ctx, time.Since(start).Seconds(), recordAttrs("op", op, outcome, tenant))
}

// MintRejected bumps the mint-rejection counter for reason (a bounded,
// low-cardinality cause: unauthenticated/rate_limited/bad_request/invalid_op/
// forbidden/mint_error). An empty reason is ignored so call sites can pass a
// classifier's output without branching.
func (m *Metrics) MintRejected(ctx context.Context, reason string) {
	if m == nil || reason == "" {
		return
	}
	m.mintRejected.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// VerifyFailure bumps the data-path token verify-failure counter — the auth
// canary (docs/OBSERVABILITY.md): a spike means an attack or a key-rotation
// issue. No attributes: the failure is deliberately opaque (the edge never
// reveals which check failed, mirroring the 401 it returns), and there is no
// verified tenant to attribute it to.
func (m *Metrics) VerifyFailure(ctx context.Context) {
	if m == nil {
		return
	}
	m.verifyFailures.Add(ctx, 1)
}

// AddBytesPut counts object bytes accepted via PUT, by tenant.
func (m *Metrics) AddBytesPut(ctx context.Context, tenant string, n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.bytesPut.Add(ctx, n, tenantAttr(tenant))
}

// AddBytesGet counts object bytes streamed via GET, by tenant.
func (m *Metrics) AddBytesGet(ctx context.Context, tenant string, n int64) {
	if m == nil || n <= 0 {
		return
	}
	m.bytesGet.Add(ctx, n, tenantAttr(tenant))
}

// IntegrityError bumps the read-path data-integrity canary for kind
// (corrupt/not_found — the values snapshot.IntegrityKind returns). An empty kind
// is ignored, so call sites can pass IntegrityKind(err) directly. Same smoke
// alarm as blobgw.integrity.errors: it should be ~0 forever
// (docs/OBSERVABILITY.md operational canaries).
func (m *Metrics) IntegrityError(ctx context.Context, kind string) {
	if m == nil || kind == "" {
		return
	}
	m.integrityErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("error.kind", kind)))
}

// tenantAttr builds the bounded tenant attribute set for a counter, omitting it
// when tenant is empty (label absent, not blank).
func tenantAttr(tenant string) metric.MeasurementOption {
	if tenant == "" {
		return metric.WithAttributes()
	}
	return metric.WithAttributes(attribute.String("tenant", tenant))
}

// ErrorKind re-exports blobgw's bounded error taxonomy so the edge classifies
// error.kind identically to the layers behind it (one dashboard vocabulary).
func ErrorKind(err error) string { return obsmetrics.ErrorKind(err) }

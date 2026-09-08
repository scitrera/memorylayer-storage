// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// DataPath holds blobgw's request-driven (RED) and integrity instruments for the
// hot path — PostgreSQL index ops, control-plane RPCs, HTTP requests, and the
// data-integrity canary. Unlike the GC/compaction Registry (one process sweeps
// many tenants on a leader), these are recorded inline on every request, so they
// use the FINE IO bucket set (docs/OBSERVABILITY.md) and derive rate/errors from
// the duration histogram's {operation, outcome} attributes plus a separate errors
// counter carrying {operation, error.kind}.
//
// A DataPath is built over any metric.Meter — the shared "blobgw" meter from a
// Registry (Registry.Meter()) when metrics are enabled, or a no-op meter when they
// are disabled — so call sites never branch on whether metrics are on. All Record*
// methods are nil-safe: a nil *DataPath is a no-op.
type DataPath struct {
	pgDuration metric.Float64Histogram
	pgErrors   metric.Int64Counter

	cpDuration metric.Float64Histogram
	cpErrors   metric.Int64Counter

	httpDuration metric.Float64Histogram
	bytesPut     metric.Int64Counter
	bytesGet     metric.Int64Counter

	integrityErrors metric.Int64Counter

	pool *poolGauges // observable DB-pool gauges; nil unless BindPool was called
}

// NewDataPath builds the data-path instruments over meter. meter must be non-nil
// (use Registry.Meter(), which returns a no-op meter when disabled). It returns an
// error only if instrument construction fails.
//
// Instruments (RED for the request-driven surfaces; the integrity canary is USE):
//   - blobgw.pg.op.duration         histogram (s)  {operation, outcome}
//   - blobgw.pg.errors              counter        {operation, error.kind}
//   - blobgw.cp.op.duration         histogram (s)  {operation, outcome}
//   - blobgw.cp.errors              counter        {operation, error.kind}
//   - blobgw.http.request.duration  histogram (s)  {operation, outcome}
//   - blobgw.http.bytes_put         counter (By)
//   - blobgw.http.bytes_get         counter (By)
//   - blobgw.integrity.errors       counter        {error.kind}
//
// DB connection-pool gauges (blobgw.pg.pool.in_use/idle/wait_count) are registered
// separately via BindPool, since they observe a live *sql.DB.
func NewDataPath(meter metric.Meter) (*DataPath, error) {
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

	d := &DataPath{
		pgDuration:      hist("blobgw.pg.op.duration", "PostgreSQL index operation latency"),
		pgErrors:        ctr("blobgw.pg.errors", "PostgreSQL index operations that failed, by error kind"),
		cpDuration:      hist("blobgw.cp.op.duration", "Control-plane RPC handler latency"),
		cpErrors:        ctr("blobgw.cp.errors", "Control-plane RPCs that failed, by error kind"),
		httpDuration:    hist("blobgw.http.request.duration", "HTTP data-path request latency, by route + outcome"),
		bytesPut:        ctr("blobgw.http.bytes_put", "Object bytes stored via PUT", metric.WithUnit("By")),
		bytesGet:        ctr("blobgw.http.bytes_get", "Object bytes streamed via GET", metric.WithUnit("By")),
		integrityErrors: ctr("blobgw.integrity.errors", "Data-integrity faults on the read path (the corruption / GC over-deletion canary), by error kind"),
	}
	if ierr != nil {
		return nil, ierr
	}
	return d, nil
}

// RecordPG records one PostgreSQL index op: the duration histogram (always,
// {operation, outcome}) and the error counter on failure ({operation, error.kind}).
// It returns err unchanged so call sites stay one-liners.
func (d *DataPath) RecordPG(ctx context.Context, op string, start time.Time, err error) error {
	if d == nil {
		return err
	}
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	d.pgDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(attribute.String("operation", op), attribute.String("outcome", outcome)))
	if err != nil {
		d.pgErrors.Add(ctx, 1,
			metric.WithAttributes(attribute.String("operation", op), attribute.String("error.kind", ErrorKind(err))))
	}
	return err
}

// RecordCP records one control-plane RPC handler: the duration histogram (always,
// {operation, outcome}) and the error counter on failure ({operation, error.kind}).
// handlerErr is the operation's effective outcome (a decode/validate/store failure
// the handler surfaces to the client), not the NATS publish error.
func (d *DataPath) RecordCP(ctx context.Context, op string, start time.Time, handlerErr error) {
	if d == nil {
		return
	}
	outcome := "ok"
	if handlerErr != nil {
		outcome = "error"
	}
	d.cpDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(attribute.String("operation", op), attribute.String("outcome", outcome)))
	if handlerErr != nil {
		d.cpErrors.Add(ctx, 1,
			metric.WithAttributes(attribute.String("operation", op), attribute.String("error.kind", ErrorKind(handlerErr))))
	}
}

// RecordHTTP records one HTTP data-path request: the request-latency histogram
// {operation, outcome}. operation is a LOW-CARDINALITY route label (put/get/head/
// delete/list/mint/finalize), NEVER the raw path. ok is the request outcome
// (status < 400).
func (d *DataPath) RecordHTTP(ctx context.Context, op string, start time.Time, ok bool) {
	if d == nil {
		return
	}
	outcome := "ok"
	if !ok {
		outcome = "error"
	}
	d.httpDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(attribute.String("operation", op), attribute.String("outcome", outcome)))
}

// AddBytesPut counts object bytes stored via PUT.
func (d *DataPath) AddBytesPut(ctx context.Context, n int64) {
	if d == nil || n <= 0 {
		return
	}
	d.bytesPut.Add(ctx, n)
}

// AddBytesGet counts object bytes streamed via GET.
func (d *DataPath) AddBytesGet(ctx context.Context, n int64) {
	if d == nil || n <= 0 {
		return
	}
	d.bytesGet.Add(ctx, n)
}

// IntegrityError bumps the data-integrity canary for kind (corrupt/not_found/
// range — the values snapshot.IntegrityKind returns). An empty kind is ignored,
// so call sites can pass IntegrityKind(err) directly without branching. This is
// the smoke alarm for chunk-hash mismatch, pack-not-found, and ranged-EOF; it
// should be ~0 forever (docs/OBSERVABILITY.md operational canaries).
func (d *DataPath) IntegrityError(ctx context.Context, kind string) {
	if d == nil || kind == "" {
		return
	}
	d.integrityErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("error.kind", kind)))
}

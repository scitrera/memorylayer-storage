// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package controlplane is the blobgw-side NATS control-plane server: it serves
// the ctlproto request-reply RPCs (index lookup/record, GC purge, presign) that
// mlfs nodes (the RemoteDedupStore client) call. It is the server half of
// ADR-001 §2.5's internal node ↔ blobgw control plane.
//
// # Scale-out
//
// All handlers subscribe under the ctlproto.QueueGroup queue group, so NATS
// load-balances each request across blobgw replicas — running more replicas is
// the only scale-out step (ADR §2.5: "queue groups are the scale-out
// mechanism", no external LB).
//
// # Tenant routing & the account model
//
// ctlproto subjects are per-tenant (blobgw.<tenant>.index.lookup, …). A single
// server cannot enumerate tenants up front, so it subscribes with NATS
// wildcards (blobgw.*.index.lookup, …) and recovers the concrete tenant from
// each delivered subject via ctlproto.TenantFromSubject, then re-validates it
// with ctlproto.ValidateTenant.
//
// Account model (ADR-001 §7.7 — account-per-domain on the converged control
// plane). This server is account-agnostic by design: it always subscribes to
// the blobgw.*.> wildcards in WHATEVER account its *nats.Conn lives in, and the
// account boundary is configured OUTSIDE this code, on the NATS server. Two
// deployment shapes are supported, both unchanged in this file:
//
//   - Legacy single account: one account holds blobgw + every mlfs node; tenants
//     are isolated only by subject prefix (blobgw.<tenant>.*) plus this server's
//     app-level TenantFromSubject/ValidateTenant. Correct for a trusted single
//     instance.
//
//   - Account-per-domain (the §7.7 target): blobgw connects in a SHARED service
//     account that EXPORTS a service on blobgw.*.> ; each tenant gets its own
//     NATS account that IMPORTS that service remapped to its own prefix only
//     (tenant A imports blobgw.A.> ). A node in tenant A's account can then reach
//     blobgw for tenant A — and ONLY tenant A: publishing blobgw.B.* from
//     account A has no matching import, so NATS returns no-responder. Isolation
//     is enforced by the ACCOUNT boundary (the export/import mapping), not merely
//     by this server's subject validation, even if a node forges the subject.
//     blobgw's connection account is selected by cmd/blobgw's -nats-creds /
//     -nats-nkey. The required server-side export/import mapping operators must
//     apply is documented in docs/blobgw-control-plane-accounts.md and proven by
//     TestServiceAccountIsolation in this package.
//
// Because the service runs in one account and tenant accounts import a restricted
// view, the existing wildcard subscribe + tenant-from-subject logic is unchanged:
// the cross-account service export/import is the only new isolation surface, and
// it lives in NATS config / cmd wiring, not here.
package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/scitrera/memorylayer-storage/blobgw/metrics"
	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/ctlproto"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation scope for the control-plane RPC spans.
const tracerName = "github.com/scitrera/memorylayer-storage/blobgw/controlplane"

// Wildcard subscription subjects: one per operation, matching every tenant.
// TenantFromSubject recovers the concrete tenant from the delivered subject.
const (
	subjIndexLookup = "blobgw.*.index.lookup"
	subjIndexRecord = "blobgw.*.index.record"
	subjPresign     = "blobgw.*.presign"
	subjGCPurge     = "blobgw.*.gc.purge"
	subjAssocRecord = "blobgw.*.assoc.record"
)

// Default option values.
const (
	defaultRequestTimeout = 30 * time.Second
	defaultPresignMaxTTL  = 15 * time.Minute
)

// Server serves the ctlproto control-plane RPCs over NATS request-reply.
//
// The index RPCs (lookup/record/purge) are handled against a
// snapshot.DedupStore; the presign RPC resolves per-tenant credentials via a
// tenantbind.Provider and mints presigned S3 URLs. A Server is safe for
// concurrent use once Start has returned — NATS delivers requests on its own
// goroutines and the backing stores are concurrency-safe.
type Server struct {
	nc       *nats.Conn
	index    snapshot.DedupStore
	provider tenantbind.Provider

	codec          ctlproto.Codec
	requestTimeout time.Duration
	presignMaxTTL  time.Duration
	logger         *slog.Logger
	// dp records per-RPC RED metrics (blobgw.cp.op.duration / errors). nil-safe:
	// a nil *metrics.DataPath makes every record call a no-op.
	dp *metrics.DataPath
	// tracer + propagator open the per-RPC server span and continue a caller's
	// trace from the NATS message headers. Both come from the GLOBAL OTEL provider,
	// so they are cheap no-ops until a TracerProvider is registered.
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator

	subs []*nats.Subscription
}

// Option configures a Server.
type Option func(*Server)

// WithCodec sets the wire codec used to decode requests and encode responses.
// Defaults to ctlproto.DefaultCodec.
func WithCodec(c ctlproto.Codec) Option {
	return func(s *Server) {
		if c != nil {
			s.codec = c
		}
	}
}

// WithRequestTimeout bounds how long a single handler may spend in the backing
// store / presign call before its context is cancelled. Defaults to 30s.
func WithRequestTimeout(d time.Duration) Option {
	return func(s *Server) {
		if d > 0 {
			s.requestTimeout = d
		}
	}
}

// WithPresignMaxTTL caps the validity of minted presigned URLs: the effective
// TTL is min(requested, cap). Defaults to 15m (well under the AWS STS session
// floor so the future STS impl can honor the same cap — ADR §2.9).
func WithPresignMaxTTL(d time.Duration) Option {
	return func(s *Server) {
		if d > 0 {
			s.presignMaxTTL = d
		}
	}
}

// WithLogger sets the structured logger. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) {
		if l != nil {
			s.logger = l
		}
	}
}

// WithMetrics wires the data-path RED recorder so each RPC handler records
// blobgw.cp.op.duration {operation, outcome} + blobgw.cp.errors {operation,
// error.kind}. Omit (or pass nil) to leave control-plane metrics dark.
func WithMetrics(dp *metrics.DataPath) Option {
	return func(s *Server) { s.dp = dp }
}

// NewServer builds a control-plane Server. nc is a live NATS connection; index
// backs the dedup-index RPCs (inject a snapshot.MemoryDedupStore in tests, a
// pgindex.Store in production); provider resolves per-tenant bindings for the
// presign RPC. All three are required.
func NewServer(nc *nats.Conn, index snapshot.DedupStore, provider tenantbind.Provider, opts ...Option) (*Server, error) {
	if nc == nil {
		return nil, fmt.Errorf("controlplane: nil nats connection")
	}
	if index == nil {
		return nil, fmt.Errorf("controlplane: nil index store")
	}
	if provider == nil {
		return nil, fmt.Errorf("controlplane: nil tenant provider")
	}
	s := &Server{
		nc:             nc,
		index:          index,
		provider:       provider,
		codec:          ctlproto.DefaultCodec,
		requestTimeout: defaultRequestTimeout,
		presignMaxTTL:  defaultPresignMaxTTL,
		logger:         slog.Default(),
		tracer:         otel.Tracer(tracerName),
		propagator:     otel.GetTextMapPropagator(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Start installs the queue-group subscriptions. It returns the first
// subscription error (after tearing down any partial subscriptions). The
// supplied ctx bounds the lifetime of in-flight handler work: each handler
// derives its per-request context from ctx (so Close/cancellation interrupts
// long backing-store calls). Start returns immediately; requests are served on
// NATS callback goroutines until Close.
func (s *Server) Start(ctx context.Context) error {
	subscriptions := []struct {
		subject string
		op      string // the bounded operation label for the cp.* RED instruments
		handler func(context.Context, *nats.Msg) error
	}{
		{subjIndexLookup, "lookup", s.handleLookup},
		{subjIndexRecord, "record", s.handleRecord},
		{subjGCPurge, "gc.purge", s.handlePurge},
		{subjPresign, "presign", s.handlePresign},
		{subjAssocRecord, "assoc.record", s.handleAssociate},
	}
	for _, sub := range subscriptions {
		h, op := sub.handler, sub.op
		natsSub, err := s.nc.QueueSubscribe(sub.subject, ctlproto.QueueGroup, func(msg *nats.Msg) {
			// Open the per-RPC server span (blobgw.cp.<op>, SpanKind=Server),
			// best-effort continuing a caller's trace from the NATS message headers,
			// so the casstore backing / PG work the handler triggers nests under it
			// and the cp.* histogram (recorded within this ctx) gets a trace exemplar.
			// A global no-op when no TracerProvider is registered (tracing off).
			rctx := s.extractTrace(ctx, msg)
			rctx, span := s.tracer.Start(rctx, "blobgw.cp."+op, trace.WithSpanKind(trace.SpanKindServer))
			// RED for the control-plane RPC: time the handler and record
			// blobgw.cp.op.duration {operation, outcome} + blobgw.cp.errors
			// {operation, error.kind} from the handler's effective outcome (the error
			// it surfaced to the client via the response Error field). A dropped
			// invalid-subject request returns nil — it never reached the operation.
			start := time.Now()
			herr := h(rctx, msg)
			s.dp.RecordCP(rctx, op, start, herr)
			if herr != nil {
				span.SetStatus(codes.Error, metrics.ErrorKind(herr))
			}
			span.End()
		})
		if err != nil {
			_ = s.Close()
			return fmt.Errorf("controlplane: subscribe %q: %w", sub.subject, err)
		}
		s.subs = append(s.subs, natsSub)
	}
	return nil
}

// Close drains and unsubscribes all subscriptions. It is safe to call more than
// once. It does NOT close the NATS connection — the caller owns the conn's
// lifecycle (mirroring pgindex, which does not own its *sql.DB).
func (s *Server) Close() error {
	var firstErr error
	for _, sub := range s.subs {
		if sub == nil {
			continue
		}
		if err := sub.Drain(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("controlplane: drain subscription: %w", err)
		}
	}
	s.subs = nil
	return firstErr
}

// extractTrace continues a caller's distributed trace from the NATS message
// headers when present (best-effort): mlfs's RemoteDedupStore client may inject
// W3C trace-context headers on the request, so the cp span chains under the
// caller's span. A message with no headers (the common path today) just returns
// ctx unchanged, so the cp span is a fresh root. nats.Header is a
// map[string][]string, the same shape propagation.MapCarrier expects via a thin
// single-value view.
func (s *Server) extractTrace(ctx context.Context, msg *nats.Msg) context.Context {
	if len(msg.Header) == 0 {
		return ctx
	}
	return s.propagator.Extract(ctx, natsHeaderCarrier(msg.Header))
}

// natsHeaderCarrier adapts nats.Header (map[string][]string) to the
// propagation.TextMapCarrier interface for trace-context extraction. Set is
// unused on the server (extract-only) but required by the interface.
type natsHeaderCarrier nats.Header

func (c natsHeaderCarrier) Get(key string) string {
	v := nats.Header(c).Get(key)
	return v
}
func (c natsHeaderCarrier) Set(key, value string) { nats.Header(c).Set(key, value) }
func (c natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// tenantFromMsg extracts and validates the tenant token from msg.Subject. On an
// invalid subject it returns ok=false WITHOUT replying: a malformed subject can
// only arrive from a misconfigured/hostile client, the response type is
// ambiguous, and there is no tenant to scope a reply to — so we log and drop.
func (s *Server) tenantFromMsg(msg *nats.Msg) (string, bool) {
	tenant, ok := ctlproto.TenantFromSubject(msg.Subject)
	if !ok {
		s.logger.Warn("controlplane: dropping request on invalid subject", "subject", msg.Subject)
		return "", false
	}
	return tenant, true
}

// reply encodes resp with the server codec and publishes it to msg.Reply. A
// handled error is conveyed via the response's own Error field (set by the
// caller before reply), so the client never times out on a handled failure; a
// missing Reply subject (a fire-and-forget request) or an encode/publish
// failure is logged, as there is nowhere left to surface it.
func (s *Server) reply(msg *nats.Msg, resp any) {
	if msg.Reply == "" {
		// No reply subject: the client did not use request-reply. Nothing to do.
		return
	}
	data, err := s.codec.Marshal(resp)
	if err != nil {
		s.logger.Error("controlplane: marshal response", "subject", msg.Subject, "err", err)
		return
	}
	if err := msg.Respond(data); err != nil {
		s.logger.Error("controlplane: respond", "subject", msg.Subject, "err", err)
	}
}

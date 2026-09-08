// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package server exposes the blobgw Gateway over HTTP.
//
// Routes:
//
//	PUT    /v1/objects/{ref...}       store an object (body = content; Content-Type honored)
//	GET    /v1/objects/{ref...}       stream an object's bytes
//	HEAD   /v1/objects/{ref...}       object metadata (headers only)
//	DELETE /v1/objects/{ref...}       delete an object
//	GET    /v1/objects?prefix=&limit=&after= list objects by ref prefix (bare JSON array; X-Blobgw-Next-Cursor header pages)
//	DELETE /v1/objects?prefix=        bulk-delete every object under a (required, non-empty) prefix → {"deleted":N}
//	POST   /v1/staged                 mint a stage-then-finalize upload (JSON in/out)
//	POST   /v1/finalize               finalize a staged upload (JSON in/out)
//	GET    /healthz                   liveness  (UNVERSIONED, operational)
//	GET    /readyz                    readiness (UNVERSIONED, operational)
//	GET    /metrics                   Prometheus exposition (UNVERSIONED, operational)
//
// API versioning policy (A7): the DATA plane lives under a /v1 prefix so the
// object/staging contract can evolve (a future /v2) without breaking existing
// callers. OPERATIONAL endpoints (healthz/readyz/metrics) are deliberately
// UNVERSIONED at the root: they describe the process, not the data API, and
// liveness/readiness probes and scrapers should not have to track API versions.
//
// This is an internal, trusted service (the L1.3 edge owns the external trust
// boundary); it does no auth of its own.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/httpx"
	obsmetrics "github.com/scitrera/memorylayer-storage/blobgw/metrics"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation scope for blobgw's server spans.
const tracerName = "github.com/scitrera/memorylayer-storage/blobgw/server"

// apiV1 is the data-plane route prefix (see the versioning policy above).
const apiV1 = "/v1"

// Option configures the server constructed by New.
type Option func(*server)

// WithReadiness wires the dependency probe backing GET /readyz. ready should
// return nil only when every backing dependency the gateway needs (its index
// and chunk backend) is reachable; a non-nil error makes /readyz return 503.
// When unset, /readyz reports ready unconditionally (it degrades to a second
// liveness check), so callers that can't cheaply probe dependencies still get a
// working endpoint.
func WithReadiness(ready func(ctx context.Context) error) Option {
	return func(s *server) { s.ready = ready }
}

// WithLogger sets the slog.Logger used by the access-log middleware. Defaults
// to slog.Default when unset.
func WithLogger(l *slog.Logger) Option {
	return func(s *server) {
		if l != nil {
			s.logger = l
		}
	}
}

// DomainUsage is the per-domain storage rollup the usage endpoint reports. It
// mirrors pgindex.UsageStats but is redeclared here so the server package does
// not import pgindex (the server is wired against small interfaces, not concrete
// stores). physical is the effective (dedup × compression) on-disk footprint;
// logicalDeduped is the unique uncompressed content size; ObjectApparentBytes
// is the pre-dedup size of blobgw objects only (mlfs slice refs are combined
// downstream by data-connectors).
type DomainUsage struct {
	PhysicalBytes       int64
	LogicalDedupedBytes int64
	ObjectApparentBytes int64
}

// DomainUsager reports a domain's storage rollup. pgindex.Store satisfies it
// (its DomainUsage returns pgindex.UsageStats, adapted at the wiring site). The
// server holds only this narrow capability so it never depends on the concrete
// index store.
type DomainUsager interface {
	DomainUsage(ctx context.Context, domain string) (DomainUsage, error)
}

// WithUsage wires the per-domain storage-usage source backing GET /admin/usage.
// When unset the endpoint returns 501 (the daemon's index doesn't expose usage
// accounting, e.g. the in-memory index in dev/tests).
func WithUsage(u DomainUsager) Option {
	return func(s *server) { s.usage = u }
}

// PackBackfiller runs a one-time pack-size backfill over a domain (or all
// domains when domain==""), recording each pack's on-disk size into the index's
// pack-size accounting. It backs POST /admin/backfill-packs. Kept narrow so the
// server package does not import pgindex/snapshot — the concrete backfill (which
// lists pack blobs and calls snapshot.BackfillPackSizes) is wired at main.go.
type PackBackfiller interface {
	BackfillPacks(ctx context.Context, domain string) (recorded int, err error)
}

// WithBackfill wires the pack-size backfill source backing
// POST /admin/backfill-packs. When unset the endpoint returns 501 (the daemon's
// index doesn't record per-pack sizes, e.g. the in-memory index in dev/tests).
func WithBackfill(b PackBackfiller) Option {
	return func(s *server) { s.backfill = b }
}

// GCSummary is the outcome of a single on-demand per-tenant GC pass the
// TenantGCer runs. It is a narrow, server-owned mirror of the reclaim-relevant
// fields of snapshot.GCResult (mapped at the wiring site) so the server package
// stays free of a casstore-GC dependency.
type GCSummary struct {
	LiveChunks      int
	ChunksReclaimed int
	BytesReclaimed  int64
}

// TenantGCer runs a real, on-demand garbage-collection pass over ONE tenant's
// pack store (its own backend under the per-tenant bucket binding, ADR §2.6),
// backfilling pack sizes and reclaiming orphaned packs older than the safety
// window. It backs POST /admin/gc. Kept narrow so the server package does not
// import the gateway TenantRouter / casstore GC — the concrete adapter (which
// resolves the tenant's ChunkedGC and runs RunOnceForDomain) is wired at
// main.go.
type TenantGCer interface {
	RunGCForDomain(ctx context.Context, domain string) (GCSummary, error)
}

// WithTenantGC wires the on-demand per-tenant GC source backing POST /admin/gc.
// When unset the endpoint returns 501 (no per-tenant router is wired, e.g. the
// single-tenant HTTP-only posture or dev/tests).
func WithTenantGC(g TenantGCer) Option {
	return func(s *server) { s.tenantGC = g }
}

// WithMetrics wires the OTEL data-path recorder so the server emits
// blobgw.http.request.duration {operation, outcome} plus the bytes_put/get
// counters onto the shared meter (alongside the existing hand-rolled /metrics text
// endpoint, which is kept for back-compat). Also feeds the integrity canary on the
// GET read path. Omit (or pass nil) to leave OTEL HTTP metrics dark — the
// hand-rolled /metrics endpoint still works.
func WithMetrics(dp *obsmetrics.DataPath) Option {
	return func(s *server) { s.dp = dp }
}

// New returns an http.Handler serving the object API over gw. The data routes
// are mounted under /v1; operational routes (healthz/readyz/metrics) stay at
// the root. The returned handler is wrapped in metrics + access-log middleware.
func New(gw *gateway.Gateway, opts ...Option) http.Handler {
	s := &server{
		gw:         gw,
		logger:     slog.Default(),
		metrics:    &metrics{},
		tracer:     otel.Tracer(tracerName),
		propagator: otel.GetTextMapPropagator(),
	}
	for _, opt := range opts {
		opt(s)
	}

	mux := http.NewServeMux()

	// Operational endpoints — UNVERSIONED at the root (see versioning policy).
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /metrics", s.metrics.handler)

	// Data plane — versioned under /v1. Each handler is registered with a
	// LOW-CARDINALITY operation label (put/get/head/delete/list/mint/finalize) for
	// the OTEL request histogram — never the raw path (which is unbounded by ref).
	// Admin usage rollup — UNVERSIONED under /admin (an operational/admin query
	// over the pack/ref tables, not part of the /v1 object data contract).
	mux.HandleFunc("GET /admin/usage", s.adminUsage)
	// Admin pack-size backfill — UNVERSIONED under /admin (a one-time maintenance
	// op that refreshes the pack accounting table, not part of the /v1 data contract).
	mux.HandleFunc("POST /admin/backfill-packs", s.adminBackfillPacks)
	// Admin on-demand per-tenant GC — UNVERSIONED under /admin (an ops maintenance
	// action that reclaims orphaned packs for one tenant, not part of the /v1 data
	// contract).
	mux.HandleFunc("POST /admin/gc", s.adminGC)

	mux.HandleFunc("GET "+apiV1+"/objects", s.withOp("list", s.list))
	// Bulk prefix delete: the no-segment DELETE pattern. Go 1.22's ServeMux
	// disambiguates it from DELETE /v1/objects/{ref...} (single delete) by the
	// presence of a trailing path segment, so the two coexist on one mux.
	mux.HandleFunc("DELETE "+apiV1+"/objects", s.withOp("delete", s.deletePrefix))
	mux.HandleFunc("PUT "+apiV1+"/objects/{ref...}", s.withOp("put", s.put))
	mux.HandleFunc("GET "+apiV1+"/objects/{ref...}", s.withOp("get", s.get))
	mux.HandleFunc("HEAD "+apiV1+"/objects/{ref...}", s.withOp("head", s.head))
	mux.HandleFunc("DELETE "+apiV1+"/objects/{ref...}", s.withOp("delete", s.delete))
	mux.HandleFunc("POST "+apiV1+"/staged", s.withOp("mint", s.mint))
	mux.HandleFunc("POST "+apiV1+"/finalize", s.withOp("finalize", s.finalize))

	// Middleware order (outermost first): access logging wraps metrics so the
	// logged status/duration include any work the metrics layer does, and both
	// see the same response-writer wrapper that captures the status + byte count.
	return s.accessLog(s.middleware(s.metrics.middleware(mux)))
}

// opContextKey carries a per-request *opHolder (placed by the OTEL middleware
// BEFORE routing) that withOp fills once the mux selects a handler. A holder is
// used rather than a plain context value because withOp runs INSIDE the mux
// (after the outer middleware has already entered ServeHTTP): a context.WithValue
// set there would not be visible to the outer middleware. The holder passes the
// matched op AND the per-request server span back out so the outer middleware
// can finish them (record the histogram outcome, set span status, end the span)
// once the handler — and the casstore spans nested under it — have returned.
type opContextKey struct{}

// opHolder is the shared cell withOp fills and the OTEL middleware reads back
// after routing: the bounded operation label and the server span opened around
// the handler (nil span = tracing disabled, still a valid no-op end).
type opHolder struct {
	op   string
	span trace.Span
	// ctx is the handler context carrying span (set by withOp). The histogram is
	// recorded within it so a sampled span yields an exemplar on the bucket.
	ctx context.Context
}

// withOp resolves the matched route's low-cardinality operation label and opens
// the per-request server span (blobgw.http.<op>, SpanKind=Server) around handler,
// so the casstore backing spans (casstore.backing.*) opened during the handler
// nest under it and the data-path histogram — recorded within this span's ctx —
// gets a trace exemplar. It writes the op + span into the opHolder the outer
// middleware placed in the context so that middleware can finish them. Object ref
// is a fine SPAN attribute (per-request, never aggregated) even though it must
// never be a metric label (docs/OBSERVABILITY.md). Operational routes (healthz/
// readyz/metrics) are not wrapped, so they carry no op and are absent from both
// the histogram and the trace. nil-safe: with no holder (server built without
// the middleware) it is a thin passthrough; with no TracerProvider the span is a
// global no-op (zero overhead when tracing is off).
func (s *server) withOp(op string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h, ok := r.Context().Value(opContextKey{}).(*opHolder)
		if !ok {
			handler(w, r)
			return
		}
		ctx, span := s.tracer.Start(r.Context(), "blobgw.http."+op,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("operation", op)))
		if ref := r.PathValue("ref"); ref != "" {
			span.SetAttributes(attribute.String("object.ref", ref))
		}
		h.op, h.span, h.ctx = op, span, ctx
		handler(w, r.WithContext(ctx))
	}
}

// middleware records the OTEL data-path request histogram
// (blobgw.http.request.duration {operation, outcome}) and finishes the server
// span that withOp opens for routes carrying an operation label. It runs OUTSIDE
// the mux, so it cannot know the matched route up front: it seeds the opHolder +
// continues an upstream trace BEFORE routing, then reads the op/span withOp filled
// back AFTER the handler returns. outcome is derived from the status code
// (<400 = ok). nil-safe on both axes: with no DataPath the metric record is a
// no-op; with no TracerProvider the span is a global no-op (so this stays a thin
// passthrough when observability is off).
func (s *server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Continue an upstream caller's trace (W3C trace-context headers) before
		// routing, and seed the holder withOp fills once the route is matched.
		ctx := s.propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		holder := &opHolder{}
		ctx = context.WithValue(ctx, opContextKey{}, holder)

		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r.WithContext(ctx))

		// An operational route (no withOp) leaves the holder empty: nothing to record.
		if holder.op == "" {
			return
		}
		// Record within the span ctx (holder.ctx) so a sampled trace links the
		// latency bucket to the span as an exemplar (docs/OBSERVABILITY.md).
		s.dp.RecordHTTP(holder.ctx, holder.op, start, sw.status < 400)
		if holder.span != nil {
			holder.span.SetAttributes(attribute.Int("http.status_code", sw.status))
			if sw.status >= 400 {
				holder.span.SetStatus(codes.Error, http.StatusText(sw.status))
			}
			holder.span.End()
		}
	})
}

type server struct {
	gw      *gateway.Gateway
	logger  *slog.Logger
	ready   func(ctx context.Context) error
	metrics *metrics
	// usage backs GET /admin/usage. nil = usage accounting unavailable (the
	// endpoint returns 501); wired via WithUsage from the pgindex Store.
	usage DomainUsager
	// backfill backs POST /admin/backfill-packs. nil = pack-size recording
	// unavailable (the endpoint returns 501); wired via WithBackfill in the
	// postgres index branch.
	backfill PackBackfiller
	// tenantGC backs POST /admin/gc. nil = no per-tenant router wired (the
	// endpoint returns 501); wired via WithTenantGC in the control-plane posture.
	tenantGC TenantGCer
	// dp is the OTEL data-path recorder (nil = OTEL HTTP metrics off; the
	// hand-rolled /metrics text endpoint is unaffected). All its methods are
	// nil-safe.
	dp *obsmetrics.DataPath
	// tracer + propagator open the per-request server span and continue an
	// upstream caller's trace. Both come from the GLOBAL OTEL provider, so they
	// are cheap no-ops until a TracerProvider is registered (tracing disabled).
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

// --- operational handlers ---

func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

// readyz reports whether the gateway's backing dependencies are usable. Unlike
// healthz (pure liveness), it actively probes via the readiness function wired
// at construction (DB ping + a cheap backend reachability check). A nil probe
// means "always ready".
func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.ready == nil {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ready")
		return
	}
	// Bound the probe so a hung dependency can't pin the readiness handler.
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.ready(ctx); err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "not ready: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ready")
}

// usageResponse is the GET /admin/usage JSON body.
// object_apparent_bytes is the pre-dedup size of blobgw objects only
// (SUM(blob_ref.size_b)); mlfs slice refs are combined downstream by
// data-connectors and are not visible to blobgw.
type usageResponse struct {
	Domain              string  `json:"domain"`
	PhysicalBytes       int64   `json:"physical_bytes"`
	LogicalDedupedBytes int64   `json:"logical_deduped_bytes"`
	CompressionRatio    float64 `json:"compression_ratio"`
	ObjectApparentBytes int64   `json:"object_apparent_bytes"`
	ComputedAt          string  `json:"computed_at"`
}

// adminUsage serves GET /admin/usage?domain=<d>: the per-domain storage rollup
// (physical/effective bytes, logical deduped bytes, their ratio). It always
// computes fresh at the source of truth (the pack/ref tables) — any caching is
// the caller's job. A missing/empty domain is a 400; no usage source wired is a
// 501.
func (s *server) adminUsage(w http.ResponseWriter, r *http.Request) {
	domain := r.URL.Query().Get("domain")
	if domain == "" {
		httpx.WriteError(w, http.StatusBadRequest, "domain query param is required")
		return
	}
	if s.usage == nil {
		httpx.WriteError(w, http.StatusNotImplemented, "usage accounting not available on this index")
		return
	}
	u, err := s.usage.DomainUsage(r.Context(), domain)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// compression_ratio = logical / physical (0.0 when physical is 0 — avoids a
	// divide-by-zero and reports a sane 0 for an empty domain).
	ratio := 0.0
	if u.PhysicalBytes > 0 {
		ratio = float64(u.LogicalDedupedBytes) / float64(u.PhysicalBytes)
	}
	writeJSON(w, http.StatusOK, usageResponse{
		Domain:              domain,
		PhysicalBytes:       u.PhysicalBytes,
		LogicalDedupedBytes: u.LogicalDedupedBytes,
		CompressionRatio:    ratio,
		ObjectApparentBytes: u.ObjectApparentBytes,
		ComputedAt:          time.Now().UTC().Format(time.RFC3339),
	})
}

// backfillPacksResponse is the POST /admin/backfill-packs JSON body: the domain
// swept (empty when all domains) and the count of pack sizes recorded.
type backfillPacksResponse struct {
	Domain   string `json:"domain"`
	Recorded int    `json:"recorded"`
}

// adminBackfillPacks serves POST /admin/backfill-packs?domain=<d>: a one-time
// maintenance sweep that lists every pack blob (a domain's when ?domain= is set,
// or ALL domains when it is empty/omitted) and records each pack's on-disk size
// into the index's pack accounting, filling rows for packs written before storage
// accounting was enabled. Read-only w.r.t. blobs — it reclaims nothing. No
// backfill source wired is a 501. Idempotent, so it is safe to re-run.
func (s *server) adminBackfillPacks(w http.ResponseWriter, r *http.Request) {
	domain := r.URL.Query().Get("domain") // optional: empty means all domains
	if s.backfill == nil {
		httpx.WriteError(w, http.StatusNotImplemented, "pack-size backfill not available on this index")
		return
	}
	recorded, err := s.backfill.BackfillPacks(r.Context(), domain)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, backfillPacksResponse{Domain: domain, Recorded: recorded})
}

// gcResponse is the POST /admin/gc JSON body: the tenant swept plus the pass's
// live-chunk count and the packs/bytes reclaimed.
type gcResponse struct {
	Domain          string `json:"domain"`
	LiveChunks      int    `json:"live_chunks"`
	ChunksReclaimed int    `json:"chunks_reclaimed"`
	BytesReclaimed  int64  `json:"bytes_reclaimed"`
}

// adminGC serves POST /admin/gc?domain=<d>: a real, on-demand garbage-collection
// pass over ONE tenant's pack store — it backfills that tenant's pack sizes and
// reclaims orphaned packs older than the configured safety window (young packs
// are deferred to guard the GC↔in-flight-write race). This is an ops maintenance
// action, distinct from the leader-elected scheduled sweeper. Because GC is
// scoped to a single tenant's own backend (ADR §2.6), ?domain= is REQUIRED (a
// missing/empty domain is a 400, unlike backfill's all-domains sweep). No
// per-tenant GC source wired is a 501.
func (s *server) adminGC(w http.ResponseWriter, r *http.Request) {
	domain := r.URL.Query().Get("domain")
	if domain == "" {
		httpx.WriteError(w, http.StatusBadRequest, "domain query param is required")
		return
	}
	if s.tenantGC == nil {
		httpx.WriteError(w, http.StatusNotImplemented, "per-tenant GC not available on this daemon")
		return
	}
	sum, err := s.tenantGC.RunGCForDomain(r.Context(), domain)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, gcResponse{
		Domain:          domain,
		LiveChunks:      sum.LiveChunks,
		ChunksReclaimed: sum.ChunksReclaimed,
		BytesReclaimed:  sum.BytesReclaimed,
	})
}

// --- data handlers ---

func (s *server) put(w http.ResponseWriter, r *http.Request) {
	ref := r.PathValue("ref")
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	info, err := s.gw.Put(r.Context(), ref, ct, r.Body)
	if err != nil {
		writeErr(w, err)
		return
	}
	s.metrics.bytesPut.Add(info.Size)
	s.metrics.putCount.Add(1)
	s.dp.AddBytesPut(r.Context(), info.Size) // OTEL twin of the hand-rolled counter
	writeJSON(w, http.StatusCreated, info)
}

func (s *server) get(w http.ResponseWriter, r *http.Request) {
	rc, info, err := s.gw.Get(r.Context(), r.PathValue("ref"))
	if err != nil {
		// A data-integrity fault can surface from the manifest resolve before any
		// body streams (e.g. a referenced pack missing). Classify + count it as the
		// corruption / GC-over-deletion canary (docs/OBSERVABILITY.md).
		s.recordIntegrity(r, err)
		writeErr(w, err)
		return
	}
	defer rc.Close()
	setObjectHeaders(w, info)
	w.WriteHeader(http.StatusOK)
	n, copyErr := io.Copy(w, rc)
	s.metrics.bytesGet.Add(n)
	s.dp.AddBytesGet(r.Context(), n) // OTEL twin of the hand-rolled counter
	// The body is read lazily from casstore as io.Copy pulls it, so a chunk-hash
	// mismatch / missing-pack / ranged-EOF surfaces HERE, mid-stream (after the 200
	// header is already sent). This is the integrity canary's primary site.
	if copyErr != nil {
		s.recordIntegrity(r, copyErr)
	}
}

// recordIntegrity classifies err with snapshot.IntegrityKind and, when it is a
// data-integrity fault (corrupt/not_found/range), bumps blobgw.integrity.errors
// {error.kind} and logs at ERROR. It should fire ~0 times forever — it is the
// smoke alarm for corruption AND GC over-deletion (TECH_DEBT #50 class). A
// non-integrity error (a normal not-found, a client disconnect) is ignored.
func (s *server) recordIntegrity(r *http.Request, err error) {
	kind := snapshot.IntegrityKind(err)
	if kind == "" {
		return
	}
	s.dp.IntegrityError(r.Context(), kind)
	s.logger.ErrorContext(r.Context(), "blobgw: data-integrity fault on read path",
		"path", r.URL.Path, "error.kind", kind, "err", err)
}

func (s *server) head(w http.ResponseWriter, r *http.Request) {
	info, err := s.gw.Head(r.Context(), r.PathValue("ref"))
	if err != nil {
		writeErr(w, err)
		return
	}
	setObjectHeaders(w, info)
	setHeadMetaHeaders(w, info)
	w.WriteHeader(http.StatusOK)
}

func (s *server) delete(w http.ResponseWriter, r *http.Request) {
	if err := s.gw.Delete(r.Context(), r.PathValue("ref")); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// list serves GET /v1/objects. The response BODY is a bare JSON array (kept
// backward-compatible — NOT wrapped in an envelope). Keyset pagination is
// opaque and out-of-band: pass ?after=<cursor> to resume, and when more rows
// remain the response carries an X-Blobgw-Next-Cursor header (omitted when the
// listing is exhausted). With limit>0 the page is capped at that many rows; with
// limit<=0 the server applies an internal default page and surfaces the cursor
// so a client can transparently follow it to completion.
func (s *server) list(w http.ResponseWriter, r *http.Request) {
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = n
	}
	objs, next, err := s.gw.ListPage(r.Context(), r.URL.Query().Get("prefix"), r.URL.Query().Get("after"), limit)
	if err != nil {
		writeErr(w, err)
		return
	}
	if objs == nil {
		objs = []gateway.ObjectInfo{}
	}
	if next != "" {
		w.Header().Set("X-Blobgw-Next-Cursor", next)
	}
	writeJSON(w, http.StatusOK, objs)
}

// deletePrefix serves DELETE /v1/objects?prefix=... (the no-segment route,
// distinct from DELETE /v1/objects/{ref...}). It removes every object whose ref
// starts with prefix using the same per-ref delete logic as single delete (no
// orphaned manifests) and replies {"deleted": N}. A missing/empty prefix is
// rejected with 400 to avoid a "delete the whole domain" footgun.
func (s *server) deletePrefix(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	if prefix == "" {
		httpx.WriteError(w, http.StatusBadRequest, "prefix query param is required for bulk delete")
		return
	}
	deleted, err := s.gw.DeletePrefix(r.Context(), prefix)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, deletePrefixResponse{Deleted: deleted})
}

type deletePrefixResponse struct {
	Deleted int `json:"deleted"`
}

type mintRequest struct {
	Ref         string `json:"ref,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	MaxSize     int64  `json:"max_size,omitempty"`
	TTLSeconds  int    `json:"ttl_seconds,omitempty"`
	// ContentHash/Size arm the OPTIONAL finalize integrity gate (A1): the staged
	// upload's computed sha256/size must match these or Finalize returns 409.
	// Omit (or 0/"") to accept whatever is staged.
	ContentHash string `json:"content_hash,omitempty"`
	Size        int64  `json:"size,omitempty"`
}

func (s *server) mint(w http.ResponseWriter, r *http.Request) {
	var req mintRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
		httpx.WriteError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	ttl := time.Duration(req.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	staged, err := s.gw.MintRefWithOptions(r.Context(), req.Ref, gateway.MintOptions{
		ContentType:  req.ContentType,
		MaxSize:      req.MaxSize,
		TTL:          ttl,
		ExpectedHash: req.ContentHash,
		ExpectedSize: req.Size,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, staged)
}

type finalizeRequest struct {
	Ref string `json:"ref"`
}

func (s *server) finalize(w http.ResponseWriter, r *http.Request) {
	var req finalizeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	info, err := s.gw.Finalize(r.Context(), req.Ref)
	if err != nil {
		writeErr(w, err)
		return
	}
	s.metrics.finalizeCount.Add(1)
	writeJSON(w, http.StatusOK, info)
}

// setObjectHeaders delegates to the shared httpx writer (Content-Type when set,
// Content-Length, quoted ETag when a content hash is present) so blobgw/server
// and blobgw-edge emit the identical object header set from one place.
func setObjectHeaders(w http.ResponseWriter, info gateway.ObjectInfo) {
	httpx.SetObjectHeaders(w, info)
}

// setHeadMetaHeaders emits the full ObjectInfo over HEAD response headers (A4),
// so a HEAD has the SAME metadata fidelity as GET/list — the Python client's
// head() reconstructs a complete ObjectInfo from them. It augments (does not
// replace) the Content-Type/Content-Length/ETag set by setObjectHeaders.
//
// X-Blobgw-* header contract (HEAD only):
//
//	X-Blobgw-Domain      ObjectInfo.Domain
//	X-Blobgw-Created-At  ObjectInfo.CreatedAt, RFC3339 (UTC)
//	X-Blobgw-Version     ObjectInfo.Version (casstore snapshot version)
//	X-Blobgw-Pending     "true"/"false" (always emitted)
//	X-Blobgw-User-Meta   JSON-encoded map[string]string of user metadata,
//	                     OMITTED entirely when there is none
//
// All but Pending are omitted when their value is empty, so an older server (or
// an object without that field) simply doesn't send the header; clients must
// default missing headers to ""/{}/false.
func setHeadMetaHeaders(w http.ResponseWriter, info gateway.ObjectInfo) {
	h := w.Header()
	if info.Domain != "" {
		h.Set("X-Blobgw-Domain", info.Domain)
	}
	if !info.CreatedAt.IsZero() {
		h.Set("X-Blobgw-Created-At", info.CreatedAt.UTC().Format(time.RFC3339))
	}
	if info.Version != "" {
		h.Set("X-Blobgw-Version", info.Version)
	}
	h.Set("X-Blobgw-Pending", strconv.FormatBool(info.Pending))
	if len(info.UserMeta) > 0 {
		if b, err := json.Marshal(info.UserMeta); err == nil {
			h.Set("X-Blobgw-User-Meta", string(b))
		}
	}
}

// writeErr maps a gateway error to its HTTP status and writes the {"error":...}
// body. The sentinel→category decision is centralized in gateway.Classify; this
// server keeps its OWN status mapping (ErrNotStaged and ErrIntegrity both →
// 409) and its OWN body (the raw err.Error() string).
func writeErr(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch gateway.Classify(err) {
	case gateway.KindNotFound:
		status = http.StatusNotFound
	case gateway.KindInvalidRef:
		status = http.StatusBadRequest
	case gateway.KindNotStaged:
		status = http.StatusConflict
	case gateway.KindIntegrity:
		status = http.StatusConflict
	}
	httpx.WriteError(w, status, err.Error())
}

// writeJSON delegates to the shared httpx writer (Content-Type: application/json
// then the encoded value). Kept as a thin local alias so the many call sites
// read unchanged.
func writeJSON(w http.ResponseWriter, status int, v any) {
	httpx.WriteJSON(w, status, v)
}

// ---------------------------------------------------------------------------
// metrics + middleware
// ---------------------------------------------------------------------------

// metrics holds the process-wide counters exposed at /metrics. They are plain
// atomic counters so /metrics can be hand-rolled in Prometheus text exposition
// format WITHOUT pulling in a client-library dependency (A6). requestsByClass
// is keyed by "<method> <statusClass>" (e.g. "GET 2xx") to keep cardinality
// bounded while still distinguishing success from error.
type metrics struct {
	inFlight      atomic.Int64
	bytesPut      atomic.Int64
	bytesGet      atomic.Int64
	putCount      atomic.Int64
	finalizeCount atomic.Int64

	requests sync.Map // map["<method> <class>"]*atomic.Int64
}

func (m *metrics) countRequest(method string, status int) {
	key := method + " " + statusClass(status)
	v, ok := m.requests.Load(key)
	if !ok {
		v, _ = m.requests.LoadOrStore(key, new(atomic.Int64))
	}
	v.(*atomic.Int64).Add(1)
}

// middleware records per-request metrics: in-flight gauge and a request counter
// keyed by method + status class. It wraps the response writer to observe the
// status code the handler wrote.
func (m *metrics) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.inFlight.Add(1)
		defer m.inFlight.Add(-1)
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		m.countRequest(r.Method, sw.status)
	})
}

// handler serves the Prometheus text exposition. Hand-rolled (no client lib):
// each metric prints its HELP/TYPE lines then its samples.
func (m *metrics) handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	var b strings.Builder

	b.WriteString("# HELP blobgw_requests_total Total HTTP requests by method and status class.\n")
	b.WriteString("# TYPE blobgw_requests_total counter\n")
	m.requests.Range(func(key, val any) bool {
		k := key.(string)
		method, class, _ := strings.Cut(k, " ")
		fmt.Fprintf(&b, "blobgw_requests_total{method=%q,status=%q} %d\n",
			method, class, val.(*atomic.Int64).Load())
		return true
	})

	b.WriteString("# HELP blobgw_in_flight_requests In-flight HTTP requests.\n")
	b.WriteString("# TYPE blobgw_in_flight_requests gauge\n")
	fmt.Fprintf(&b, "blobgw_in_flight_requests %d\n", m.inFlight.Load())

	b.WriteString("# HELP blobgw_bytes_put_total Total object bytes stored via PUT.\n")
	b.WriteString("# TYPE blobgw_bytes_put_total counter\n")
	fmt.Fprintf(&b, "blobgw_bytes_put_total %d\n", m.bytesPut.Load())

	b.WriteString("# HELP blobgw_bytes_get_total Total object bytes streamed via GET.\n")
	b.WriteString("# TYPE blobgw_bytes_get_total counter\n")
	fmt.Fprintf(&b, "blobgw_bytes_get_total %d\n", m.bytesGet.Load())

	b.WriteString("# HELP blobgw_put_total Total successful object PUTs.\n")
	b.WriteString("# TYPE blobgw_put_total counter\n")
	fmt.Fprintf(&b, "blobgw_put_total %d\n", m.putCount.Load())

	b.WriteString("# HELP blobgw_finalize_total Total successful staged-upload finalizes.\n")
	b.WriteString("# TYPE blobgw_finalize_total counter\n")
	fmt.Fprintf(&b, "blobgw_finalize_total %d\n", m.finalizeCount.Load())

	_, _ = io.WriteString(w, b.String())
}

// accessLog logs one structured line per request (method, path, status,
// duration, bytes) at INFO via slog.
func (s *server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		s.logger.InfoContext(r.Context(), "blobgw: request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"bytes", sw.written,
		)
	})
}

// statusWriter wraps http.ResponseWriter to capture the status code and the
// number of body bytes written, for metrics and access logging. It mirrors the
// default ResponseWriter's implicit-200 behavior: if WriteHeader is never
// called, status stays at its initialized 200.
type statusWriter struct {
	http.ResponseWriter
	status      int
	written     int64
	wroteHeader bool
}

func (sw *statusWriter) WriteHeader(code int) {
	if !sw.wroteHeader {
		sw.status = code
		sw.wroteHeader = true
	}
	sw.ResponseWriter.WriteHeader(code)
}

func (sw *statusWriter) Write(p []byte) (int, error) {
	if !sw.wroteHeader {
		sw.wroteHeader = true
	}
	n, err := sw.ResponseWriter.Write(p)
	sw.written += int64(n)
	return n, err
}

// statusClass collapses a status code into a Prometheus-friendly class label so
// the request counter's cardinality stays bounded.
func statusClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	case code >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}

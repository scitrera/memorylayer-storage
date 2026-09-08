// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package edge is blobgw-edge: the externally-facing capability layer in front
// of internal blobgw.
//
// It owns the external trust boundary — capability minting + stateless
// verification, tenant isolation, quotas, rate limiting, revocation, and audit
// — while internal blobgw stays policy-light behind it. Per the L1.3 design,
// the edge is on the data path: it terminates external upload/download bytes
// (data-path shape B) and streams them to/from a per-tenant blobgw Gateway via
// a TenantRouter, so identical content within a tenant dedups to one physical
// copy and distinct tenants never share chunks.
//
// Authorization is on the logical ref (tenant + subject + capability); dedup is
// on the physical chunk (tenant domain). The two never cross: the tenant comes
// only from the asserted identity (mint) or the verified capability (data
// path), so a caller can never reach another tenant's namespace.
package edge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw-edge/capability"
	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/httpx"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation scope for blobgw-edge's server spans.
const tracerName = "github.com/scitrera/memorylayer-storage/blobgw-edge/edge"

// Existence modes for the cross-user existence-oracle knob.
const (
	// ModeTrusted treats a tenant as one trust domain: HEAD/existence checks
	// are honored.
	ModeTrusted = "trusted"
	// ModeConfidential refuses existence checks (HEAD), so no cross-user
	// existence signal is observable. Uploads always transfer in full and
	// dedup server-side after receipt (this edge never does skip-upload
	// existence negotiation), which already closes the upload-side oracle.
	ModeConfidential = "confidential"
)

// Config tunes edge policy.
type Config struct {
	Audience            string        // this edge's identity (token audience)
	MaxObjectSize       int64         // global upload ceiling in bytes (0 = unlimited)
	AllowedContentTypes []string      // PUT content-type allowlist (empty = any)
	DefaultTTL          time.Duration // capability TTL when the request omits one
	ExistenceMode       string        // ModeTrusted | ModeConfidential

	// DefaultRequireAuth is the auth-binding default when a mint request does not
	// specify require_auth (tri-state nil). THIS PHASE: false — shareable-by-default
	// (non-breaking). The minter always stamps the resolved value onto the token
	// explicitly, so the verifier never infers a default.
	DefaultRequireAuth bool
	// ShareableMaxTTL clamps the TTL of shareable (ra=false) capabilities so a
	// leaked bearer URL is short-lived by construction. 0 disables the clamp.
	ShareableMaxTTL time.Duration
	// AuthBoundMaxTTL is the ceiling for auth-bound (ra=true) capabilities, which
	// can safely carry longer TTLs since a leaked URL is inert without the session.
	// 0 disables the clamp.
	AuthBoundMaxTTL time.Duration
}

// Deps bundles the pluggable policy backends.
type Deps struct {
	Identity   IdentityProvider
	Quota      QuotaStore
	Rate       RateLimiter
	Revocation Revocation
	Audit      AuditSink
}

// Server is the edge HTTP service.
type Server struct {
	router   *gateway.TenantRouter
	minter   *capability.Minter
	verifier *capability.Verifier
	deps     Deps
	cfg      Config
	ctAllow  map[string]struct{} // nil = allow any

	// metrics is the OTEL edge instrument set (nil = metrics off). All its
	// methods are nil-safe, so call sites never branch on whether metrics are on.
	metrics *Metrics
	// tracer + propagator open the per-request server span and continue an
	// upstream caller's trace. Both come from the GLOBAL OTEL provider, so they
	// are cheap no-ops until a TracerProvider is registered (tracing disabled).
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

// Option configures the Server built by New. Options are the seam for wiring
// OTEL: without any, the server is telemetry-dark (noop meter + global no-op
// tracer → zero overhead, no behavior change), mirroring blobgw server's
// WithMetrics.
type Option func(*Server)

// WithMetrics wires the OTEL edge instrument set so the server emits
// edge.request.duration / edge.mint.* / edge.capability.verify_failures /
// edge.bytes_* / edge.integrity.errors onto the shared meter. Omit (or pass nil)
// to leave edge metrics dark. nil-safe: every Record* call is a no-op.
func WithMetrics(m *Metrics) Option {
	return func(s *Server) { s.metrics = m }
}

// WithTracing overrides the tracer + propagator used for the edge's server
// spans. By default New captures the GLOBAL OTEL tracer/propagator (a no-op
// until a TracerProvider is registered), which is what production wants — so
// this is mainly a test seam. A nil tracer or propagator is ignored.
func WithTracing(tracer trace.Tracer, propagator propagation.TextMapPropagator) Option {
	return func(s *Server) {
		if tracer != nil {
			s.tracer = tracer
		}
		if propagator != nil {
			s.propagator = propagator
		}
	}
}

// New constructs an edge server. Missing Deps default to permissive/no-op
// implementations so a minimal dev setup works. Optional Options wire OTEL
// metrics/tracing; with none the server is telemetry-dark.
func New(router *gateway.TenantRouter, minter *capability.Minter, verifier *capability.Verifier, deps Deps, cfg Config, opts ...Option) *Server {
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = 15 * time.Minute
	}
	if cfg.ShareableMaxTTL <= 0 {
		cfg.ShareableMaxTTL = 5 * time.Minute
	}
	if cfg.AuthBoundMaxTTL <= 0 {
		cfg.AuthBoundMaxTTL = 60 * time.Minute
	}
	if cfg.ExistenceMode == "" {
		cfg.ExistenceMode = ModeTrusted
	}
	if deps.Quota == nil {
		deps.Quota = NewMemoryQuotaStore(0, 0)
	}
	if deps.Rate == nil {
		deps.Rate = NewTokenBucketLimiter(0, 0) // disabled
	}
	if deps.Revocation == nil {
		deps.Revocation = NewMemoryRevocation()
	}
	if deps.Audit == nil {
		deps.Audit = SlogAuditSink{Logger: slog.Default()}
	}
	var ctAllow map[string]struct{}
	if len(cfg.AllowedContentTypes) > 0 {
		ctAllow = make(map[string]struct{}, len(cfg.AllowedContentTypes))
		for _, ct := range cfg.AllowedContentTypes {
			ctAllow[ct] = struct{}{}
		}
	}
	s := &Server{
		router: router, minter: minter, verifier: verifier, deps: deps, cfg: cfg, ctAllow: ctAllow,
		tracer:     otel.Tracer(tracerName),
		propagator: otel.GetTextMapPropagator(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Handler returns the edge's HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	mux.HandleFunc("POST /capabilities", s.mint)
	mux.HandleFunc("POST /revoke", s.revoke)
	mux.HandleFunc("PUT /blob/{ref...}", s.dataPath)
	mux.HandleFunc("GET /blob/{ref...}", s.dataPath)
	mux.HandleFunc("HEAD /blob/{ref...}", s.dataPath)
	mux.HandleFunc("DELETE /blob/{ref...}", s.dataPath)
	// Stage-then-finalize (op=STAGE / op=FINALIZE). Both are POST, so the
	// route's required op — not r.Method — is what the capability must carry;
	// each handler passes it to the shared verification path.
	mux.HandleFunc("POST /staged/{ref...}", s.stage)
	mux.HandleFunc("POST /finalize/{ref...}", s.finalize)
	return mux
}

type mintRequest struct {
	Op          string `json:"op"`
	Ref         string `json:"ref"`
	MaxSize     int64  `json:"max_size,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	ContentHash string `json:"content_hash,omitempty"`
	TTLSeconds  int    `json:"ttl_seconds,omitempty"`
	// RequireAuth is TRI-STATE: nil ⇒ use cfg.DefaultRequireAuth; non-nil ⇒ honor
	// it verbatim. A pointer so an absent field is distinct from an explicit false.
	RequireAuth *bool `json:"require_auth,omitempty"`
	// MatchMode selects how the live principal is matched on the data path when the
	// token is auth-bound: "exact" | "same-tenant". "group" is RESERVED (rejected
	// 400); any other non-empty value is rejected 400. Ignored when the resolved
	// require-auth is false.
	MatchMode string `json:"match_mode,omitempty"`
}

type mintResponse struct {
	Token         string    `json:"token"`
	CapabilityURL string    `json:"capability_url"`
	Op            string    `json:"op"`
	Ref           string    `json:"ref"`
	Tenant        string    `json:"tenant"`
	ExpiresAt     time.Time `json:"expires_at"`
}

func (s *Server) mint(w http.ResponseWriter, r *http.Request) {
	// Server span around the whole mint (the edge's defining action). Continue an
	// upstream W3C trace so a caller's trace threads into the edge. The span is a
	// global no-op when tracing is disabled.
	ctx := s.propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := s.tracer.Start(ctx, "edge.mint", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	r = r.WithContext(ctx)
	start := time.Now()

	// reject records the mint RED (rejection counter + error histogram) and the
	// span error status, then writes the HTTP error. op/tenant are best-effort:
	// "" until each is known. Recorded within ctx so a sampled trace links the
	// bucket as an exemplar.
	reject := func(status int, msg, reason, op, tenant string) {
		s.metrics.MintRejected(ctx, reason)
		s.metrics.RecordMint(ctx, op, tenant, start, false)
		span.SetStatus(codes.Error, reason)
		writeErr(w, status, msg)
	}

	tenant, subject, err := s.deps.Identity.Identify(r)
	if err != nil {
		reject(http.StatusUnauthorized, "unauthenticated", "unauthenticated", "", "")
		return
	}
	span.SetAttributes(attribute.String("tenant", tenant))
	if !s.deps.Rate.Allow(ctx, tenant) {
		reject(http.StatusTooManyRequests, "rate limited", "rate_limited", "", tenant)
		return
	}
	var req mintRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		reject(http.StatusBadRequest, "invalid JSON body", "bad_request", "", tenant)
		return
	}
	op := capability.Op(req.Op)
	switch op {
	case capability.OpPut, capability.OpGet, capability.OpHead, capability.OpDelete,
		capability.OpStage, capability.OpFinalize:
	default:
		reject(http.StatusBadRequest, "invalid op", "invalid_op", "", tenant)
		return
	}
	// op is bounded (validated above); attribute it as the LOW-CARDINALITY method
	// on the span + histogram (ref stays a span-only attribute below).
	opLabel := strings.ToLower(req.Op)
	span.SetAttributes(attribute.String("op", opLabel), attribute.String("ref", req.Ref))
	if req.Ref == "" {
		reject(http.StatusBadRequest, "ref required", "bad_request", opLabel, tenant)
		return
	}
	if op == capability.OpHead && s.cfg.ExistenceMode == ModeConfidential {
		reject(http.StatusForbidden, "existence checks disabled (confidential tenant)", "forbidden", opLabel, tenant)
		return
	}
	// PUT and STAGE both authorize an upload (STAGE via a presigned staging PUT),
	// so both apply the upload guards at mint: the content-type allowlist and the
	// edge max-size ceiling. FINALIZE carries no body through the edge, so it is
	// not gated here (its ContentHash claim is enforced at finalize time).
	if op == capability.OpPut || op == capability.OpStage {
		if req.ContentType != "" && !s.contentTypeAllowed(req.ContentType) {
			reject(http.StatusForbidden, "content type not allowed", "forbidden", opLabel, tenant)
			return
		}
		if s.cfg.MaxObjectSize > 0 && req.MaxSize > s.cfg.MaxObjectSize {
			reject(http.StatusForbidden, "max_size exceeds edge limit", "forbidden", opLabel, tenant)
			return
		}
	}
	// Resolve auth-binding (tri-state) and match-mode. The token ALWAYS stamps ra
	// explicitly (never inferred by the verifier). For an auth-bound token the
	// match-mode defaults to "exact" and must be one of the supported modes;
	// "group" is reserved (rejected, not silently accepted) and any other value is
	// invalid. For a shareable token match-mode is irrelevant and forced empty.
	ra := s.cfg.DefaultRequireAuth
	if req.RequireAuth != nil {
		ra = *req.RequireAuth
	}
	mm := ""
	if ra {
		mm = req.MatchMode
		if mm == "" {
			mm = "exact"
		}
		switch mm {
		case "exact", "same-tenant":
		case "group":
			reject(http.StatusBadRequest, "match_mode 'group' not yet supported", "bad_request", opLabel, tenant)
			return
		default:
			reject(http.StatusBadRequest, "invalid match_mode", "bad_request", opLabel, tenant)
			return
		}
	}

	ttl := s.cfg.DefaultTTL
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
	}
	// TTL clamp keyed off the auth binding: shareable (bearer) URLs are forced very
	// short, auth-bound URLs get a longer ceiling.
	if !ra && s.cfg.ShareableMaxTTL > 0 && ttl > s.cfg.ShareableMaxTTL {
		ttl = s.cfg.ShareableMaxTTL
	}
	if ra && s.cfg.AuthBoundMaxTTL > 0 && ttl > s.cfg.AuthBoundMaxTTL {
		ttl = s.cfg.AuthBoundMaxTTL
	}
	claims := capability.Claims{
		Tenant:      tenant,
		Subject:     subject,
		Op:          op,
		Ref:         req.Ref,
		MaxSize:     req.MaxSize,
		ContentType: req.ContentType,
		ContentHash: req.ContentHash,
		RequireAuth: ra,
		MatchMode:   mm,
	}
	token, finalized, err := s.minter.Mint(claims, ttl)
	if err != nil {
		reject(http.StatusInternalServerError, "mint failed", "mint_error", opLabel, tenant)
		return
	}
	s.deps.Audit.Record(ctx, AuditEvent{
		Kind: "mint", Tenant: tenant, Subject: subject, Op: req.Op, Ref: req.Ref, JTI: finalized.JTI, Allowed: true,
	})
	s.metrics.RecordMint(ctx, opLabel, tenant, start, true)
	writeJSON(w, http.StatusOK, mintResponse{
		Token:         token,
		CapabilityURL: capabilityURL(op, req.Ref, token),
		Op:            req.Op,
		Ref:           req.Ref,
		Tenant:        tenant,
		ExpiresAt:     time.Unix(finalized.Expiry, 0).UTC(),
	})
}

func (s *Server) dataPath(w http.ResponseWriter, r *http.Request) {
	// The blob data path's required op IS the HTTP method (PUT/GET/HEAD/DELETE),
	// so pass the method-as-op to the shared verification path.
	vr, ok := s.verifyRequest(w, r, capability.Op(r.Method), strings.ToLower(r.Method))
	if !ok {
		return
	}
	defer vr.finish()
	switch r.Method {
	case http.MethodGet:
		s.handleGet(vr.w, vr.r, vr.gw, vr.claims, vr.liveSub, vr.ref)
	case http.MethodHead:
		s.handleHead(vr.w, vr.r, vr.gw, vr.claims, vr.liveSub, vr.ref)
	case http.MethodPut:
		s.handlePut(vr.w, vr.r, vr.gw, vr.claims, vr.liveSub, vr.ref)
	case http.MethodDelete:
		s.handleDelete(vr.w, vr.r, vr.gw, vr.claims, vr.liveSub, vr.ref)
	default:
		vr.deny(http.StatusMethodNotAllowed, "method not allowed")
	}
}

// verified bundles everything the shared verification path produced for a
// data-path/stage/finalize handler: the status-capturing writer + request (with
// the per-request span ctx), the routed per-tenant gateway, the verified claims,
// the ref, a deny closure that audits+writes an error under the same span, and
// finish, which records the request histogram + closes the span with the final
// status. Callers MUST `defer vr.finish()` after an ok verifyRequest so the
// span/outcome is recorded regardless of how the handler returns. ok=false from
// verifyRequest means the response (and its span) are already finished; the
// caller returns immediately without touching finish.
type verified struct {
	w      http.ResponseWriter
	r      *http.Request
	gw     *gateway.Gateway
	claims capability.Claims
	ref    string
	// liveSub is the live asserted subject that satisfied an auth-bound match
	// (empty on the shareable path). Handlers thread it into the access audit so a
	// used-by-owner access is distinguishable from anonymous shareable access.
	liveSub string
	deny    func(status int, reason string)
	finish  func()
}

// verifyRequest is the shared external-trust-boundary gate for every
// capability-gated route (/blob, /staged, /finalize). It extracts the token
// (?cap= or Bearer), verifies it (bumping the auth canary on failure), opens the
// per-request server span, wraps the writer so the span + edge.request.duration
// histogram capture the final status, and runs the common denials — revocation,
// op match against requiredOp (NOT r.Method, so the POST-based stage/finalize
// routes bind their LOGICAL op), ref match, confidential-mode existence
// suppression for HEAD, per-(tenant,subject) rate limit, and tenant-scoped
// gateway routing. Tenant comes ONLY from the verified capability, so
// cross-tenant access is structurally impossible on every route.
//
// opLabel is the low-cardinality metric/span operation label
// (get/put/head/delete/stage/finalize). On any failure it writes the response,
// closes the span, and returns ok=false; on success the returned verified
// carries the routed gateway + claims + a deny closure sharing the same span and
// a finish() the caller must defer.
func (s *Server) verifyRequest(w http.ResponseWriter, r *http.Request, requiredOp capability.Op, opLabel string) (verified, bool) {
	ref := r.PathValue("ref")
	token := r.URL.Query().Get("cap")
	if token == "" {
		if auth := r.Header.Get("Authorization"); len(auth) > 7 && auth[:7] == "Bearer " {
			token = auth[7:]
		}
	}
	if token == "" {
		writeErr(w, http.StatusUnauthorized, "missing capability")
		return verified{}, false
	}
	claims, err := s.verifier.Verify(token)
	if err != nil {
		// Authenticity/temporal failures are all 401 — never reveal which. Bump the
		// auth canary (a spike = attack or key-rotation issue) before returning.
		s.metrics.VerifyFailure(r.Context())
		writeErr(w, http.StatusUnauthorized, "invalid capability")
		return verified{}, false
	}

	// The capability is verified: tenant/ref/op are now known, so mint the
	// per-request server span HERE (SpanKind=Server), continuing an upstream W3C
	// trace. The gateway Get/Put → casstore backing leaf spans nest under it, and
	// edge.request.duration — recorded within this span's ctx below — picks up a
	// trace exemplar. ref is a fine SPAN attribute (per-request, never a metric
	// label). The span is a global no-op when tracing is disabled.
	ctx := s.propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := s.tracer.Start(ctx, "edge."+opLabel,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("operation", opLabel),
			attribute.String("object.ref", ref),
			attribute.String("tenant", claims.Tenant),
		))
	r = r.WithContext(ctx)

	// sw captures the final status so the span + request histogram see the outcome
	// the handlers/deny paths wrote (outcome = ok when status < 400). finish records
	// that outcome and ends the span exactly once (guarded), whether the caller
	// defers it on the success path or a deny closure hits it on an early return.
	sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
	start := time.Now()
	finished := false
	finish := func() {
		if finished {
			return
		}
		finished = true
		s.metrics.RecordRequest(ctx, opLabel, claims.Tenant, start, sw.status < 400)
		span.SetAttributes(attribute.Int("http.status_code", sw.status))
		if sw.status >= 400 {
			span.SetStatus(codes.Error, http.StatusText(sw.status))
		}
		span.End()
	}

	// liveSub is the LIVE asserted subject (from Identify) on the auth-bound path,
	// distinct from claims.Subject (the minter). It is populated only after a
	// successful auth-binding match below; on the shareable path there is no live
	// subject and it stays empty. The deny/audit closures read it so an access
	// event records who actually presented the URL (§3.4).
	var liveSub string
	deny := func(status int, reason string) {
		s.deps.Audit.Record(ctx, AuditEvent{
			Kind: "access", Tenant: claims.Tenant, Subject: claims.Subject, LiveSubject: liveSub,
			Op: string(requiredOp), Ref: ref, JTI: claims.JTI, Allowed: false, Reason: reason,
		})
		writeErr(sw, status, reason)
	}
	// denyFinish is used for the pre-handoff early returns: it denies AND finishes
	// the span (the caller returns ok=false and never defers finish itself).
	denyFinish := func(status int, reason string) {
		deny(status, reason)
		finish()
	}
	if s.deps.Revocation.IsRevoked(ctx, claims.JTI) {
		denyFinish(http.StatusForbidden, "capability revoked")
		return verified{}, false
	}
	if claims.Op != requiredOp {
		denyFinish(http.StatusForbidden, "operation not permitted by capability")
		return verified{}, false
	}
	if claims.Ref != ref {
		denyFinish(http.StatusForbidden, "ref not permitted by capability")
		return verified{}, false
	}
	if requiredOp == capability.OpHead && s.cfg.ExistenceMode == ModeConfidential {
		denyFinish(http.StatusNotFound, "not found")
		return verified{}, false
	}
	if !s.deps.Rate.Allow(ctx, claims.Tenant+":"+claims.Subject) {
		denyFinish(http.StatusTooManyRequests, "rate limited")
		return verified{}, false
	}

	// Tenant comes ONLY from the verified capability → cross-tenant access is
	// structurally impossible.
	gw, err := s.router.For(claims.Tenant)
	if err != nil {
		// Log the concrete cause server-side (tenant not in resolver, STS
		// AssumeRole failure, unreachable manifest DB, S3 client build error) — the
		// client-facing reason stays generic so no backend detail crosses the trust
		// boundary, but an operator debugging a failed tenant onboarding needs the
		// wrapped error, not just "tenant routing failed".
		slog.Error("blobgw-edge: tenant routing failed",
			"tenant", claims.Tenant, "subject", claims.Subject, "op", string(requiredOp),
			"ref", ref, "err", err)
		denyFinish(http.StatusInternalServerError, "tenant routing failed")
		return verified{}, false
	}

	// Auth-binding enforcement (§3.3): an auth-bound capability (ra=true) must ALSO
	// match the LIVE asserted principal — the identity headers the auth proxy
	// stamps — per its signed match-mode. This is the seam that makes a leaked
	// auth-bound URL inert without a session as the right principal. When ra is
	// false the URL is shareable (pure bearer) and behavior is byte-identical to
	// today: no Identify call, principal irrelevant.
	if claims.RequireAuth {
		liveTenant, liveSubject, err := s.deps.Identity.Identify(r)
		if err != nil {
			denyFinish(http.StatusForbidden, "capability requires authenticated principal")
			return verified{}, false
		}
		ok := false
		switch claims.MatchMode {
		case "", "exact":
			ok = liveTenant == claims.Tenant && liveSubject == claims.Subject
		case "same-tenant":
			ok = liveTenant == claims.Tenant
		default:
			ok = false // unknown/reserved (e.g. group) => fail closed
		}
		if !ok {
			denyFinish(http.StatusForbidden, "capability principal mismatch")
			return verified{}, false
		}
		liveSub = liveSubject
	}

	return verified{
		w:       sw,
		r:       r,
		gw:      gw,
		claims:  claims,
		ref:     ref,
		liveSub: liveSub,
		deny:    deny,
		finish:  finish,
	}, true
}

// statusWriter wraps http.ResponseWriter to capture the status code the edge's
// handlers/deny paths wrote, so the data-path span + request histogram can derive
// the request outcome. It mirrors the default writer's implicit-200 behavior.
type statusWriter struct {
	http.ResponseWriter
	status      int
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
	sw.wroteHeader = true
	return sw.ResponseWriter.Write(p)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, gw *gateway.Gateway, c capability.Claims, liveSub, ref string) {
	rc, info, err := gw.Get(r.Context(), ref)
	if err != nil {
		// A data-integrity fault can surface from the manifest resolve before any
		// body streams (e.g. a referenced pack missing). Classify + count it as the
		// corruption / GC-over-deletion canary (docs/OBSERVABILITY.md), same as blobgw.
		s.metrics.IntegrityError(r.Context(), snapshot.IntegrityKind(err))
		s.gatewayErr(w, r, c, liveSub, ref, err)
		return
	}
	defer rc.Close()
	httpx.SetObjectHeaders(w, info)
	w.WriteHeader(http.StatusOK)
	n, copyErr := io.Copy(w, rc)
	s.metrics.AddBytesGet(r.Context(), c.Tenant, n)
	// The body is read lazily from casstore as io.Copy pulls it, so a chunk-hash
	// mismatch / missing-pack / ranged-EOF surfaces HERE, mid-stream (after the 200
	// header is already sent) — the integrity canary's primary site.
	if copyErr != nil {
		s.metrics.IntegrityError(r.Context(), snapshot.IntegrityKind(copyErr))
	}
	s.audit(r, c, liveSub, ref, true, "", info.Size)
}

func (s *Server) handleHead(w http.ResponseWriter, r *http.Request, gw *gateway.Gateway, c capability.Claims, liveSub, ref string) {
	info, err := gw.Head(r.Context(), ref)
	if err != nil {
		s.gatewayErr(w, r, c, liveSub, ref, err)
		return
	}
	httpx.SetObjectHeaders(w, info)
	w.WriteHeader(http.StatusOK)
	s.audit(r, c, liveSub, ref, true, "", 0)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, gw *gateway.Gateway, c capability.Claims, liveSub, ref string) {
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	if c.ContentType != "" && ct != c.ContentType {
		s.deny(w, r, c, liveSub, ref, http.StatusForbidden, "content type does not match capability")
		return
	}
	if s.ctAllow != nil && !s.contentTypeAllowed(ct) {
		s.deny(w, r, c, liveSub, ref, http.StatusForbidden, "content type not allowed")
		return
	}
	// Size ceiling: tightest of capability and edge limits.
	ceiling := c.MaxSize
	if s.cfg.MaxObjectSize > 0 && (ceiling == 0 || s.cfg.MaxObjectSize < ceiling) {
		ceiling = s.cfg.MaxObjectSize
	}
	if err := s.deps.Quota.Authorize(r.Context(), c.Tenant, ceiling); err != nil {
		s.deny(w, r, c, liveSub, ref, http.StatusInsufficientStorage, "quota exceeded")
		return
	}
	body := r.Body
	if ceiling > 0 {
		body = http.MaxBytesReader(w, r.Body, ceiling)
	}
	info, err := gw.Put(r.Context(), ref, ct, body)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			s.deny(w, r, c, liveSub, ref, http.StatusRequestEntityTooLarge, "object exceeds size limit")
			return
		}
		s.gatewayErr(w, r, c, liveSub, ref, err)
		return
	}
	// ContentHash binding: a capability minted "you may upload exactly this
	// content hash" must bind the bytes, not just authorize the ref. The
	// capability's ch claim and gateway ObjectInfo.ContentHash use the SAME
	// encoding — lowercase hex of the whole-object sha256, no algorithm prefix
	// (see capability.Claims.ContentHash and gateway hashingCounter.hexSum).
	// We compare-and-delete AFTER the Put (simplest, correct): the bytes briefly
	// land in the store before deletion on mismatch, which is acceptable since
	// the ref is never returned to the caller as valid (403, body discarded).
	if c.ContentHash != "" && info.ContentHash != c.ContentHash {
		// Best-effort cleanup of the just-written ref; the deny path still fires
		// regardless of Delete's outcome.
		_ = gw.Delete(r.Context(), ref)
		s.deny(w, r, c, liveSub, ref, http.StatusForbidden, "content hash does not match capability")
		return
	}
	s.deps.Quota.RecordPut(r.Context(), c.Tenant, info.Size)
	s.metrics.AddBytesPut(r.Context(), c.Tenant, info.Size)
	s.audit(r, c, liveSub, ref, true, "", info.Size)
	writeJSON(w, http.StatusCreated, info)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, gw *gateway.Gateway, c capability.Claims, liveSub, ref string) {
	// Best-effort size accounting for quota release.
	var size int64
	if info, err := gw.Head(r.Context(), ref); err == nil {
		size = info.Size
	}
	if err := gw.Delete(r.Context(), ref); err != nil {
		s.gatewayErr(w, r, c, liveSub, ref, err)
		return
	}
	s.deps.Quota.RecordDelete(r.Context(), c.Tenant, size)
	s.audit(r, c, liveSub, ref, true, "", 0)
	w.WriteHeader(http.StatusNoContent)
}

// stage handles POST /staged/{ref...} (op=STAGE): after the shared capability
// gate it mints a presigned staging upload for exactly (tenant, ref) and returns
// the gateway.StagedUpload as JSON — the presigned URL + staging key the client
// PUTs raw bytes to. The mint binds the capability's ContentType/MaxSize/
// ContentHash so the same content-type/size guards handlePut applies also gate
// the staged upload (and the ContentHash flows to Finalize's integrity gate).
// The bytes never traverse the edge here; only the presign is minted.
func (s *Server) stage(w http.ResponseWriter, r *http.Request) {
	vr, ok := s.verifyRequest(w, r, capability.OpStage, "stage")
	if !ok {
		return
	}
	defer vr.finish()
	c := vr.claims

	// Content-type allowlist parity with handlePut: a stage cap that named a
	// content type must clear the edge allowlist (already checked at mint, but the
	// allowlist can change between mint and stage).
	if c.ContentType != "" && s.ctAllow != nil && !s.contentTypeAllowed(c.ContentType) {
		vr.deny(http.StatusForbidden, "content type not allowed")
		return
	}
	// Size ceiling: tightest of capability and edge limits, exactly like handlePut,
	// passed to the presign so the staging PUT itself is bounded.
	ceiling := c.MaxSize
	if s.cfg.MaxObjectSize > 0 && (ceiling == 0 || s.cfg.MaxObjectSize < ceiling) {
		ceiling = s.cfg.MaxObjectSize
	}
	if err := s.deps.Quota.Authorize(r.Context(), c.Tenant, ceiling); err != nil {
		vr.deny(http.StatusInsufficientStorage, "quota exceeded")
		return
	}
	ttl := s.cfg.DefaultTTL
	staged, err := vr.gw.MintRefWithOptions(r.Context(), vr.ref, gateway.MintOptions{
		ContentType: c.ContentType,
		MaxSize:     ceiling,
		TTL:         ttl,
		// Arm the finalize integrity gate with the capability's content hash so a
		// staged upload whose bytes disagree is rejected at Finalize (mirrors the
		// handlePut ContentHash binding for the direct path). ExpectedSize is left
		// unset (0): the capability carries no exact size, only a ceiling.
		ExpectedHash: c.ContentHash,
	})
	if err != nil {
		s.gatewayErr(vr.w, r, c, vr.liveSub, vr.ref, err)
		return
	}
	s.audit(r, c, vr.liveSub, vr.ref, true, "", 0)
	writeJSON(vr.w, http.StatusOK, staged)
}

// finalize handles POST /finalize/{ref...} (op=FINALIZE): after the shared
// capability gate it finalizes the staged upload for exactly (tenant, ref),
// chunking+deduping the staged bytes into casstore, and returns the resulting
// ObjectInfo JSON (Size + ContentHash — what lets the client skip a head/get
// round-trip). It records the finalized size against quota (RecordPut) + audit +
// bytes metric, mirroring handlePut. The capability's ContentHash claim, if set,
// must match the finalized whole-object hash; on mismatch the ref is deleted and
// the request denied, exactly like handlePut's post-Put binding check.
func (s *Server) finalize(w http.ResponseWriter, r *http.Request) {
	vr, ok := s.verifyRequest(w, r, capability.OpFinalize, "finalize")
	if !ok {
		return
	}
	defer vr.finish()
	c := vr.claims

	info, err := vr.gw.Finalize(r.Context(), vr.ref)
	if err != nil {
		s.gatewayErr(vr.w, r, c, vr.liveSub, vr.ref, err)
		return
	}
	// ContentHash binding: identical to handlePut. The gateway ALSO enforces the
	// hash via the finalize integrity gate armed at stage time (ExpectedHash), but
	// we re-check at the edge so the capability's ch claim binds the bytes even if
	// the pending row was minted through a different path — and delete + deny on a
	// mismatch so no readable ref is left behind.
	if c.ContentHash != "" && info.ContentHash != c.ContentHash {
		_ = vr.gw.Delete(r.Context(), vr.ref)
		vr.deny(http.StatusForbidden, "content hash does not match capability")
		return
	}
	s.deps.Quota.RecordPut(r.Context(), c.Tenant, info.Size)
	s.metrics.AddBytesPut(r.Context(), c.Tenant, info.Size)
	s.audit(r, c, vr.liveSub, vr.ref, true, "", info.Size)
	writeJSON(vr.w, http.StatusOK, info)
}

type revokeRequest struct {
	JTI string `json:"jti"`
	// ExpiresAt is the revoked token's own expiry (unix seconds), so the denylist
	// entry can be dropped exactly when the token would have expired anyway. It is
	// OPTIONAL: the revoke endpoint takes only a jti (it does not re-verify the
	// token, which the operator may no longer hold), so when omitted (0) the
	// backend falls back to its fixed retention TTL — never honoring a revocation
	// past that safety bound.
	ExpiresAt int64 `json:"expires_at,omitempty"`
}

func (s *Server) revoke(w http.ResponseWriter, r *http.Request) {
	// Revocation is an operator action; require an asserted identity.
	if _, _, err := s.deps.Identity.Identify(r); err != nil {
		writeErr(w, http.StatusUnauthorized, "unauthenticated")
		return
	}
	var req revokeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.JTI == "" {
		writeErr(w, http.StatusBadRequest, "jti required")
		return
	}
	// A zero expiry maps to time.Time{} (the "unknown expiry" sentinel), which the
	// backend replaces with its fixed retention TTL.
	var expiry time.Time
	if req.ExpiresAt > 0 {
		expiry = time.Unix(req.ExpiresAt, 0).UTC()
	}
	s.deps.Revocation.Revoke(r.Context(), req.JTI, expiry)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) contentTypeAllowed(ct string) bool {
	if s.ctAllow == nil {
		return true
	}
	_, ok := s.ctAllow[ct]
	return ok
}

func (s *Server) gatewayErr(w http.ResponseWriter, r *http.Request, c capability.Claims, liveSub, ref string, err error) {
	// NotFound/InvalidRef get specific edge statuses+reasons; the NotStaged/
	// Integrity sentinels — which the FINALIZE path can now produce (finalize on a
	// ref that was never staged, or staged bytes that fail the integrity gate armed
	// at stage time) — map to 409 Conflict, mirroring blobgw's JSON server. Every
	// other classification stays the internal-error default. The edge keeps its OWN
	// reason strings, distinct from the JSON server's raw err.Error() bodies, so no
	// internal detail leaks across the trust boundary.
	status := http.StatusInternalServerError
	reason := "internal error"
	switch gateway.Classify(err) {
	case gateway.KindNotFound:
		status, reason = http.StatusNotFound, "not found"
	case gateway.KindInvalidRef:
		status, reason = http.StatusBadRequest, "invalid ref"
	case gateway.KindNotStaged:
		status, reason = http.StatusConflict, "no pending staged upload"
	case gateway.KindIntegrity:
		status, reason = http.StatusConflict, "staged content failed integrity check"
	}
	// Surface the concrete cause server-side for the opaque internal-error case
	// (presign / ref-store / staging failures that don't map to a sentinel) — the
	// client-facing reason stays generic, but an operator debugging a failed upload
	// needs the wrapped error, not just "internal error".
	if status == http.StatusInternalServerError {
		slog.Error("blobgw-edge: gateway op failed",
			"tenant", c.Tenant, "subject", c.Subject, "ref", ref, "err", err)
	}
	s.deny(w, r, c, liveSub, ref, status, reason)
}

func (s *Server) deny(w http.ResponseWriter, r *http.Request, c capability.Claims, liveSub, ref string, status int, reason string) {
	s.audit(r, c, liveSub, ref, false, reason, 0)
	writeErr(w, status, reason)
}

func (s *Server) audit(r *http.Request, c capability.Claims, liveSub, ref string, allowed bool, reason string, size int64) {
	s.deps.Audit.Record(r.Context(), AuditEvent{
		Kind: "access", Tenant: c.Tenant, Subject: c.Subject, LiveSubject: liveSub, Op: r.Method,
		Ref: ref, JTI: c.JTI, Allowed: allowed, Reason: reason, SizeBytes: size,
	})
}

// SlogAuditSink writes audit events to a slog.Logger.
type SlogAuditSink struct{ Logger *slog.Logger }

func (s SlogAuditSink) Record(ctx context.Context, ev AuditEvent) {
	l := s.Logger
	if l == nil {
		l = slog.Default()
	}
	l.InfoContext(ctx, "edge.audit",
		"kind", ev.Kind, "tenant", ev.Tenant, "subject", ev.Subject, "live_subject", ev.LiveSubject,
		"op", ev.Op, "ref", ev.Ref, "jti", ev.JTI, "allowed", ev.Allowed, "reason", ev.Reason, "size", ev.SizeBytes)
}

// capabilityURL builds the capability_url for a mint response: the route the
// token authorizes, the escaped ref, and the token in the ?cap= query. STAGE and
// FINALIZE point at their own POST routes (/staged, /finalize); every other op
// keeps the /blob data path. Keeping the {token, capability_url} shape uniform
// lets the client follow capability_url regardless of op.
func capabilityURL(op capability.Op, ref, token string) string {
	base := "/blob/"
	switch op {
	case capability.OpStage:
		base = "/staged/"
	case capability.OpFinalize:
		base = "/finalize/"
	}
	return base + escapeRefPath(ref) + "?cap=" + token
}

// escapeRefPath percent-escapes a ref for use in a URL path while preserving
// the '/' separators, so a multi-segment ref maps onto the /blob/{ref...} route.
func escapeRefPath(ref string) string {
	parts := strings.Split(ref, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

// writeErr/writeJSON are thin local aliases over the shared httpx helpers so the
// many edge call sites read unchanged; the wire output (the {"error":msg} body
// and the application/json Content-Type) is identical to the prior local copies.
func writeErr(w http.ResponseWriter, status int, msg string) { httpx.WriteError(w, status, msg) }

func writeJSON(w http.ResponseWriter, status int, v any) { httpx.WriteJSON(w, status, v) }

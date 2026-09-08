# Storage observability (OTEL/OTLP)

The conventions every storage layer follows so metrics, logs, and traces compose
into one coherent picture across casstore (L0), blobgw (L1), and mlfs (L2). New
instrumentation MUST follow this; it is the spec, not a suggestion.

## Signals (all three flow over OTLP)

- **Metrics** — rates, latency distributions, saturation, errors. The bulk of the
  coverage. Exported via OTLP push (`-otel-endpoint`) and/or a Prometheus `/metrics`
  scrape; both off by default.
- **Traces** — the architecture is a fan-out (gateway → PG dedup → S3 pack; cold
  mlfs read → cache miss → manifest PG → pack S3). Tail latency lives in the
  fan-out, so request spans that cross the layer boundary are where to look. Wire
  **exemplars** on latency histograms so a slow bucket links to a trace.
- **Logs** — structured (slog), correlated by trace id where a trace is active.
  Reserve for events, not per-request spam: errors, GC/compaction summaries,
  lease/fence transitions, **data-integrity events**, and the operational canaries
  below.

## Instrument naming

`<layer>.<subsystem>.<metric>` — dot-separated, snake_case segments. Examples:
`casstore.backing.op.duration`, `blobgw.cp.presign.duration`,
`mlfs.writeback.oldest_unflushed_age`. Durations are `Float64Histogram` in
**seconds** (`WithUnit("s")`) with explicit buckets; byte totals are `Int64Counter`
with `WithUnit("By")`; point-in-time levels are `Int64ObservableGauge`.

Bucket sets:
- **GC/compaction (coarse, sub-ms → minutes):** `0.01,0.05,0.1,0.5,1,2,5,10,30,60,120`
- **Backing-store / DB / request IO (fine, sub-ms → 10s):** `0.0005,0.001,0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2.5,5,10`

## Attribute vocabulary

One vocabulary across all layers. Bounded keys are safe everywhere; `tenant` is
fine on counters, heavier on histograms (decide per signal).

| key | values | notes |
|---|---|---|
| `operation` | `get`/`put`/`delete`/`list`/`get_metadata`/`lookup`/`record`/`presign`/`gc.purge`/`mint`/`finalize`/… | bounded |
| `outcome` | `ok` \| `error` | every request-driven instrument carries it |
| `error.kind` | `not_found`,`already_exists`,`timeout`,`auth`,`range`,`corrupt`,`conflict`,`canceled`,`other` | only on `outcome=error` |
| `backend` | `s3` \| `local` \| `pg` | which dependency |
| `tenant` / `mlfs.domain` | the dedup domain / tenant | identity; mlfs uses `mlfs.domain` |
| `service.instance.id` | node id | resource-level (mlfs/blobgw set it) |

**Cardinality — hard rules:** NEVER label by object key, pack hash, chunk hash, or
version (unbounded). `operation`/`outcome`/`error.kind`/`backend` are bounded and
always safe. Map raw backend errors to the small `error.kind` set at the boundary;
never put a raw error string in a label.

## Models

- **RED for request-driven surfaces** (HTTP handlers, control-plane RPCs, backing
  store, DB): Rate (request counter), Errors (`outcome`/`error.kind`), Duration
  (histogram). One `*.op.duration` histogram + derive rate/errors from its
  attributes where possible.
- **USE for resources** (caches, staging/write-back, connection pools, GC):
  Utilization, Saturation, Errors. e.g. cache hit/miss counters, write-back
  oldest-unflushed-age gauge, `db.pool.in_use/idle/wait` gauges.

## Operational canaries (alert on these — they map to real incidents we've hit)

- **`*.integrity.errors`** counter (`error.kind` = `corrupt`/`not_found`/`range`):
  chunk-hash mismatch, pack-not-found, ranged-EOF. Should be ~0 forever — the smoke
  alarm for corruption AND GC over-deletion.
- **GC live-set == 0 while blobs exist**: the #50 disaster as a single condition.
- **backing-store `error.kind=auth`**: STS credential expiry ("token has expired").
- **`mlfs.writeback.oldest_unflushed_age`**: the GC safety-window (HC1) is only sound
  while nodes drain their backlog (HC2). If this exceeds the window, GC can reclaim
  live data — correctness, not just perf.
- **mount flap / lease loss / fence rejection** counters (multi-node).

## Ownership: casstore stays telemetry-agnostic

casstore core (L0) holds no OTEL dependency — same principle as the existing
`GCMetrics` hook. OTEL adapters live in opt-in sibling packages
(`casstore/blobstore/obs`) or in the host (blobgw/mlfs), which own the meter
provider and wire adapters in at construction.

## Status

Instrumentation coverage:

- **casstore (L0):** `casstore.backing.*` (via `blobstore/obs.Wrap`), integrity
  sentinels (`snapshot.ErrCorrupt`/`ErrMissingBlob`/`IntegrityKind`), GC/compaction.
- **blobgw (L1):** `blobgw.pg.*`, `blobgw.cp.*`, `blobgw.http.request.duration` +
  bytes, `blobgw.integrity.errors`, `blobgw.gc.empty_live_set`, plus the pre-existing
  GC/compaction metrics — all on one shared meter (noop when disabled). **Traces:**
  a TracerProvider (OTLP/gRPC, same endpoint/resource as metrics, gated by
  `-otel-endpoint`, parent-based sampler via `-otel-trace-sampling`) registered
  globally so casstore backing spans export; parent spans `blobgw.http.<op>` (HTTP,
  W3C trace-context continued from request headers) and `blobgw.cp.<op>` (control
  plane, continued from NATS message headers). Data-path histograms recorded within
  these spans pick up trace exemplars (SDK default trace-based filter).
- **mlfs (L2):** backing via `obs.Wrap`, `mlfs.writeback.oldest_unflushed_age`,
  `mlfs.fuse.op.duration`, `mlfs.meta.*`, `mlfs.integrity.errors`, multi-node
  `mlfs.lease.*`/`fence.rejected`/`leader.transitions`/`mount.flap`, plus the
  pre-existing fuse/cache/upload/prefetch counters. **Traces:** a TracerProvider
  (OTLP/gRPC, same endpoint/resource as metrics, gated by `-otel-endpoint`,
  `-otel-trace-sample-ratio`) registered globally; parent `mlfs.fuse.<op>` spans and
  a `mlfs.read.cold` child span that decomposes a cold read into manifest(PG) +
  pack(S3) — the FUSE span ctx threads through `files.Read` → `meta.ReadSlices` /
  `chunks.ReadAt` → casstore backing leaf span, so the histograms recorded within it
  pick up trace exemplars.

Traces + exemplars: **done across all three layers (L0/L1/L2).** A request now traces
end to end — `blobgw.http.<op>` or `mlfs.fuse.<op>` → PG/manifest + `casstore.backing.*`
S3 leaf spans — and the fine-grained latency histograms carry exemplars into those
traces (SDK default trace-based filter). All gated by `-otel-endpoint`; a no-op global
provider when off. Deferred instruments: manifeststore op-RED, s3stage, optional
`blobgw.pg.*`/`mlfs.meta.*` child spans (the exemplar bridge already links those
histograms to the parent trace). Keep this list current as instrumentation lands.

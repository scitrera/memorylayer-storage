// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Command mlfs is the L2 POSIX filesystem daemon: it wires the PostgreSQL
// metadata engine, a casstore-backed chunk store with a durable write-back
// disk cache, the data path, and the FUSE bridge into a real mount.
//
// Single-node scope (L2.6): a local-filesystem casstore backend under
// -data-dir. Multi-node / S3 / region placement land in later phases; the
// flags here keep the CLI forward-compatible.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/nats-io/nats.go"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/cache"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/coord"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/fileio"
	fusebridge "github.com/scitrera/memorylayer-storage/mlfs/internal/fuse"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/gc"
	nethttp "net/http"

	"github.com/scitrera/memorylayer-storage/manifeststore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/metrics"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/mountlock"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/remoteindex"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/s3http"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/wal"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if err := run(); err != nil {
		slog.Error("mlfs: fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	mount := flag.String("mount", "/mnt/mlfs", "FUSE mountpoint (must exist)")
	allowOther := flag.Bool("allow-other", false, "FUSE allow_other: let processes other than the daemon's uid access the mount. REQUIRED for non-root workloads (e.g. the CSI subdir-bind-mount model where pods run as arbitrary uids) — without it the kernel denies every non-mounter uid with EACCES before permission checks run")
	maxRWSize := flag.Int("max-rw-size", 0, "FUSE max read/write request size in bytes (0 = 1 MiB default; the kernel caps it at 1 MiB). Larger sequential reads/writes are one FUSE op; mainly a tuning floor since 1 MiB is already the cap")
	maxBackground := flag.Int("max-background", 64, "max concurrent FUSE background (readahead) requests (go-fuse's own default is 12). 64 is a safe universal default — with the bounded metadata pool it lifts parallel-read throughput (e.g. model loading) with no downside; raise further (128-256) for very read-heavy nodes, or set 12 to match stock go-fuse")
	dbMaxOpenConns := flag.Int("db-max-open-conns", 8, "max open PostgreSQL connections for the metadata engine. Bounds connection churn under high read concurrency. 8 is the measured sweet spot (full parallel-read throughput; 16/32 add no gain, 4 costs ~20%). With Postgres max_connections≈100 this allows ~12 domains/daemons — LOWER it (e.g. 4) on high-domain-density nodes, raise it (with -max-background) on read-heavy ones")
	dbMaxIdleConns := flag.Int("db-max-idle-conns", 8, "max idle PostgreSQL connections kept warm (= -db-max-open-conns by default, so the pool never churns open/close under bursty load)")
	dbConnMaxLifetime := flag.Duration("db-conn-max-lifetime", 30*time.Minute, "max lifetime of a pooled PostgreSQL connection")
	readaheadBytes := flag.Int64("readahead-bytes", 16<<20, "sequential-read prefetch window in bytes: once a read stream looks sequential, proactively materialize this many bytes ahead into the local cache, hiding the per-slice backing (S3) + metadata (PG) round trips the kernel's ≤128 KiB FUSE readahead can't cover (e.g. cold model loading). Best-effort + strictly bounded (never blocks a read). 0 = disabled")
	readaheadConcurrency := flag.Int("readahead-concurrency", 8, "max in-flight readahead backing fetches. On a cold sequential read this is the throughput knob (aggregate ≈ this × per-slice S3 rate); a deeper -readahead-bytes window alone won't help past it. Scale it TOGETHER with -db-max-open-conns (each prefetched slice also does a metadata query — a high concurrency starved of PG connections just queues). Raise to 32-64 on GPU model-loading pools")
	metaDSN := flag.String("meta-dsn", envOr("MLFS_META_DSN", ""), "PostgreSQL DSN for the metadata engine (pgx)")
	dataDir := flag.String("data-dir", "/var/lib/mlfs/data", "local casstore backend directory (manifests + chunks)")
	cacheDir := flag.String("cache-dir", "/var/lib/mlfs/cache", "local disk cache + write-back staging directory")
	stagingDir := flag.String("staging-dir", "", "directory for the DURABLE write-back staging + quarantine (default: <cache-dir>/staging). Put on a PERSISTENT disk so -cache-dir can use ephemeral storage (e.g. instance-store NVMe) without losing the acked-write-survives-crash guarantee; pair with -wal-dir on the same persistent disk")
	domain := flag.String("domain", "mlfs", "casstore dedup domain (blob-ID prefix); in -remote mode this is the tenant / NATS dedup-domain token")
	remote := flag.Bool("remote", false, "use the converged direct-S3 backend (bytes node<->S3 via presigned URLs, dedup index over NATS, PG-backed manifest store) instead of the local casstore; GC is blobgw-owned")
	presignTTL := flag.Duration("presign-ttl", 0, "remote mode: override the requested presigned-URL validity (0 = backend default)")
	region := flag.Uint("region", 0, "this node's 8-bit region id (inode prefix; 0-255)")
	cacheBytes := flag.Int64("cache-max-bytes", 0, "clean-cache size cap in bytes (0 = unlimited)")
	cacheMinFree := flag.Float64("cache-min-free-fraction", 0, "evict the clean cache to keep at least this fraction of the cache filesystem free (0 = disabled; e.g. 0.15 leaves ≥15% free). Lets the LRU self-size per node disk; composes with -cache-max-bytes")
	uploadConcurrency := flag.Int("upload-concurrency", 0, "parallel write-back uploads per drain (0 = auto = min(NumCPU,16); 1 = serial; N = explicit). The -remote data path uses a DEDICATED NATS connection, so concurrency does not starve coordination's lease/heartbeat")
	packTarget := flag.Int("pack-target-bytes", 0, "casstore pack target size (0 = default ~16 MiB)")
	packFraming := flag.String("pack-framing", "per-chunk", "framing for COMPRESSED packs: per-chunk (each chunk compressed independently behind an in-pack index, so compressed packs stay range-readable) | whole-pack (one codec stream per pack: better ratio, but any chunk read fetches+decompresses the whole pack — rollback). No effect on slices stored uncompressed: the model/tensor class keeps its store_uncompressed tag and identical framing either way")
	compression := flag.String("compression", "", "pack-blob compression policy: none|zstd|gzip. Empty = backend default (-remote: none/uncompressed, optimized for the model-store workload; local: zstd). Set zstd/gzip to opt -remote into MIXED mode — generic slices compress while files classified model/tensor (see -default-store-class) stay uncompressed via their per-slice tag (preserving ranged reads + mmap). Set none to force all-uncompressed. Pair zstd with -default-store-class=uncompressed on a model-heavy tenant so an unclassified model never gets compressed")
	defaultStoreClass := flag.String("default-store-class", "default", "compression class for files whose type is NOT a recognized model/tensor: 'default' (generic data compresses under a compressing policy) or 'uncompressed' (pure model-store mount — even unrecognized files stay uncompressed/range-readable/mmap-safe). Recognized tensor extensions (.safetensors/.pt/.pth/.bin/.npy/.npz/.gguf/.ggml/.onnx/.ckpt) are ALWAYS uncompressed regardless. NOTE: -remote currently forces a global CompressNone, so this only changes behavior under a compressing policy (local mode)")
	maintInterval := flag.Duration("maintenance-interval", 0, "interval for the in-daemon maintenance loop (GC + changelog trim); 0 = disabled")
	changelogRetention := flag.Duration("changelog-retention", 168*time.Hour, "drop fs_changelog entries older than this during maintenance")
	walEnabled := flag.Bool("wal", true, "local data write-ahead log for crash recovery of uncommitted metadata writes")
	walDir := flag.String("wal-dir", "", "write-ahead log directory (default <cache-dir>/wal)")
	metricsEnabled := flag.Bool("metrics", true, "collect runtime counters and serve them at <mount>/.mlfs/metrics (Prometheus text); read live with `mlfs-bench stats`")
	metricsAddr := flag.String("metrics-addr", "", "serve a Prometheus /metrics scrape endpoint on this address (e.g. :9100); empty = off (requires -metrics)")
	otelEndpoint := flag.String("otel-endpoint", "", "push metrics to this OTLP/gRPC collector endpoint (host:port) for centralized reporting; empty = off (requires -metrics)")
	otelInsecure := flag.Bool("otel-insecure", false, "use an insecure (no-TLS) OTLP connection (in-cluster / dev collectors)")
	metricsPushInterval := flag.Duration("metrics-push-interval", 10*time.Second, "OTLP push cadence (-otel-endpoint)")
	otelTraceSampleRatio := flag.Float64("otel-trace-sample-ratio", 1, "head-sampling probability for ROOT trace spans (a span with an already-sampled parent is always kept). 1 = trace every request (default; the fan-out spans + latency exemplars are the point), <1 to sample under high op rates, ≤0 = SDK default. Only meaningful with -otel-endpoint set (traces need a collector)")
	// Multi-node coordination (L2.8). Empty -node-id keeps the single-node path.
	nodeID := flag.String("node-id", "", "this mount's coordination identity; empty = single-node (no NATS, no fencing)")
	natsURL := flag.String("nats-url", "", "external NATS URL for coordination; empty = embedded server")
	natsCreds := flag.String("nats-creds", "", "NATS credentials file selecting this domain's account (multi-tenant isolation on a shared external NATS)")
	natsCluster := flag.String("nats-cluster", "", "embedded JetStream cluster name (set with -nats-routes for multi-node); empty = standalone in-process")
	natsListenHost := flag.String("nats-listen-host", "0.0.0.0", "embedded NATS listen host (cluster mode)")
	natsPort := flag.Int("nats-port", 0, "embedded NATS client port (0 = auto; cluster mode)")
	natsClusterPort := flag.Int("nats-cluster-port", 6222, "embedded NATS route port (cluster mode)")
	natsRoutes := flag.String("nats-routes", "", "comma-separated peer route URLs (cluster mode)")
	natsStoreDir := flag.String("nats-store-dir", "/var/lib/mlfs/nats", "embedded NATS JetStream storage directory")
	leaseTTL := flag.Duration("lease-ttl", 30*time.Second, "ownership lease lifetime")
	leaseRefresh := flag.Duration("lease-refresh", 10*time.Second, "ownership lease renew interval (<= lease-ttl/2)")
	natsReplicas := flag.Int("nats-replicas", 1, "JetStream replica count for coordination buckets (cluster mode)")
	flag.Parse()

	if *metaDSN == "" {
		return errors.New("-meta-dsn (or $MLFS_META_DSN) is required")
	}
	if *region > 255 {
		return fmt.Errorf("-region must be 0-255, got %d", *region)
	}
	if *cacheMinFree < 0 || *cacheMinFree >= 1 {
		return fmt.Errorf("-cache-min-free-fraction must be in [0,1), got %v", *cacheMinFree)
	}
	// -upload-concurrency: 0 = auto = min(NumCPU,16); an explicit value is honored
	// verbatim (uncapped — the operator's call).
	uploadConc := *uploadConcurrency
	if uploadConc == 0 {
		if uploadConc = runtime.NumCPU(); uploadConc > 16 {
			uploadConc = 16
		}
	}
	if uploadConc < 1 {
		uploadConc = 1
	}
	ctx := context.Background()

	// Metadata engine.
	db, err := sql.Open("pgx", *metaDSN)
	if err != nil {
		return fmt.Errorf("open meta db: %w", err)
	}
	// Bound the connection pool. Without this it is unlimited, so high FUSE read
	// concurrency (many parallel ReadSlices/GetAttr — e.g. a model loader pulling
	// shards, amplified by -max-background) opens hundreds of PG connections and
	// exhausts the client's ephemeral ports ("cannot assign requested address").
	// A bounded pool makes queries QUEUE instead of churning connections.
	db.SetMaxOpenConns(*dbMaxOpenConns)
	db.SetMaxIdleConns(*dbMaxIdleConns)
	db.SetConnMaxLifetime(*dbConnMaxLifetime)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping meta db: %w", err)
	}
	engine := meta.Open(db, uint8(*region))
	if err := engine.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate meta schema: %w", err)
	}

	// Runtime instrument registry, built EARLY (before coordination and the chunk
	// store) so its OTEL meter can wrap the backing blob store (backing-store RED)
	// and the coordination plane can count lease/leader transitions. Always-on by
	// default: it feeds the <mount>/.mlfs/metrics virtual file (a node-local,
	// no-network surface), the HTTP /metrics scrape, and OTLP push — all sharing one
	// meter, tagged with this daemon's identity so a central collector can tell
	// tenants apart. A nil registry is fully no-op (Meter() returns an otel noop
	// meter), so disabling metrics needs no call-site branching downstream.
	var mreg *metrics.Registry
	if *metricsEnabled {
		instanceID := *nodeID
		if instanceID == "" {
			host, _ := os.Hostname()
			instanceID = fmt.Sprintf("%s-%d", host, os.Getpid())
		}
		mopts := []metrics.Option{metrics.WithIdentity(*domain, instanceID, int(*region))}
		if *otelEndpoint != "" {
			mopts = append(mopts, metrics.WithOTLP(*otelEndpoint, *otelInsecure, *metricsPushInterval))
		}
		mreg, err = metrics.New(ctx, mopts...)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("metrics: %w", err)
		}
		defer func() { _ = mreg.Shutdown(context.Background()) }()

		// TracerProvider (the trace half of observability): stands up the global
		// OTEL provider so the casstore backing leaf spans (casstore.backing.*, opened
		// by obs.Wrap) and the FUSE bridge's parent op spans actually export, and so
		// the latency histograms recorded within a span's ctx attach exemplars. Built
		// ONLY when an OTLP endpoint is configured (a trace is useless without a
		// collector) and shares the meter's endpoint + identity (same domain/instance/
		// region resource). When -otel-endpoint is empty NewTracing is a no-op and the
		// global provider stays the OTEL default no-op, so spans cost nothing.
		if *otelEndpoint != "" {
			tr, terr := metrics.NewTracing(ctx, *otelTraceSampleRatio,
				metrics.WithIdentity(*domain, instanceID, int(*region)),
				metrics.WithOTLP(*otelEndpoint, *otelInsecure, *metricsPushInterval))
			if terr != nil {
				_ = mreg.Shutdown(context.Background())
				_ = db.Close()
				return fmt.Errorf("tracing: %w", terr)
			}
			defer func() { _ = tr.Shutdown(context.Background()) }()
			slog.Info("mlfs: tracing enabled", "otlp_endpoint", *otelEndpoint,
				"sample_ratio", *otelTraceSampleRatio)
		}
		// Metadata DB connection-pool gauges (USE): read sql.DB.Stats() lazily on
		// each metrics collect — a rising wait_count at the cap is the signal to
		// raise -db-max-open-conns.
		mreg.SetMetaPoolObserver(func() (inUse, idle, waitCount int64) {
			s := db.Stats()
			return int64(s.InUse), int64(s.Idle), s.WaitCount
		})
		// Meta-DB RED: the engine records slice/attr/lease op latency + errors (and
		// fence rejections) through this recorder.
		engine.SetMetricsRecorder(mreg)

		// Optional Prometheus /metrics scrape endpoint (the same registry that backs
		// the in-mount virtual file), for centralized scraping alongside OTLP push.
		if *metricsAddr != "" {
			stopHTTP := startMetricsHTTP(*metricsAddr, mreg)
			defer stopHTTP()
		}
		slog.Info("mlfs: metrics enabled", "virtual_file", "<mount>/.mlfs/metrics",
			"scrape_addr", *metricsAddr, "otlp_endpoint", *otelEndpoint)
	}

	// Coordination context: drives the lease/heartbeat/leader loops; cancelled at
	// shutdown before the mount is torn down.
	coordCtx, stopCoord := context.WithCancel(ctx)
	defer stopCoord()

	// Multi-node coordination (L2.8) when -node-id is set. The ownership
	// coordinator must be registered BEFORE serving so the very first write is
	// fenced; lock reaping then scopes to this node's session instead of the
	// single-node wholesale clear.
	var coordn *coordination
	multiNode := *nodeID != ""
	if multiNode {
		var err error
		coordn, err = setupCoordination(coordCtx, engine, coordConfig{
			NodeID:      *nodeID,
			NATSURL:     *natsURL,
			Creds:       *natsCreds,
			Cluster:     *natsCluster,
			ListenHost:  *natsListenHost,
			ListenPort:  *natsPort,
			ClusterPort: *natsClusterPort,
			Routes:      splitCSV(*natsRoutes),
			StoreDir:    *natsStoreDir,
			TTL:         *leaseTTL,
			Refresh:     *leaseRefresh,
			Replicas:    *natsReplicas,
		}, mreg)
		if err != nil {
			return fmt.Errorf("coordination: %w", err)
		}
		defer coordn.Close()
		// Reap only THIS node's stale locks (a crashed prior incarnation shares
		// our sid); peers' live locks are untouched. Dead peers are reaped by the
		// liveness reaper on the GC leader.
		if err := engine.ClearLocksForSID(ctx, engine.LockSID()); err != nil {
			return fmt.Errorf("clear stale locks (self): %w", err)
		}
	} else {
		// Single-node: lock rows present at startup are stale leftovers from a
		// previous (possibly crashed) instance — clear wholesale.
		if err := engine.ClearAllLocks(ctx); err != nil {
			return fmt.Errorf("clear stale locks: %w", err)
		}
	}

	// Chunk store + durable write-back disk cache. Two backends share the same
	// downstream path (cache -> fileio -> fuse) via the chunkstore.Store
	// interface: the default local casstore, or — under -remote — the converged
	// direct-S3 backend (bytes node<->S3 over presigned URLs, a global dedup index
	// served by blobgw over NATS, and a PG-backed manifest store on the SAME meta
	// DB). In remote mode the node constructs NO casstore GC: pack reclamation is
	// blobgw-owned (it holds the tenant credentials and the authoritative index),
	// so `local` stays nil and the GC maintenance loop is skipped below.
	var (
		local   *chunkstore.Local // nil in remote mode (no node-side GC)
		csStore chunkstore.Store
		// ownedNATS is the data-path NATS connection THIS process owns and drains on
		// shutdown. On external NATS it is ALWAYS a dedicated connection (separate
		// from coordination's), so concurrent presign + upload traffic cannot starve
		// coordination's lease/heartbeat and self-fence the mount — the cause of the
		// mount-flap regression. Only the embedded single-node/test path reuses the
		// coordination connection (no bulk uploads there).
		ownedNATS *coord.NATS
	)
	// Resolve the pack-blob compression policy. Empty keeps each backend's default
	// (remote: nil → uncompressed; local: zstd). When set, it applies to BOTH and
	// — combined with the per-slice store_uncompressed tag the bridge stamps on
	// model/tensor files — gives mixed compression: generic data shrinks, models
	// stay uncompressed (range-readable + mmap).
	var compPolicy snapshot.CompressionPolicy // nil = backend default
	if *compression != "" {
		algo, perr := snapshot.ParseCompressionAlgo(*compression)
		if perr != nil {
			_ = db.Close()
			return fmt.Errorf("invalid -compression: %w", perr)
		}
		compPolicy = snapshot.NewContentTypePolicy(algo)
	}

	packFramingMode, perr := snapshot.ParsePackCompressionMode(*packFraming)
	if perr != nil {
		_ = db.Close()
		return fmt.Errorf("invalid -pack-framing: %w", perr)
	}

	if *remote {
		// Resolve the NATS connection for the data path.
		var nc *nats.Conn
		if dataPathOwnsNATS(*natsURL, coordn != nil) {
			// Open + own the data-path connection. On external NATS this is a
			// DEDICATED connection (separate from coordination's) so parallel
			// presign/upload traffic can't starve coordination's lease/heartbeat and
			// self-fence the mount (the mount-flap regression). Embedded-no-coord
			// starts its own server.
			var nerr error
			if *natsURL != "" {
				var nopts []nats.Option
				if *natsCreds != "" {
					nopts = append(nopts, nats.UserCredentials(*natsCreds))
				}
				ownedNATS, nerr = coord.Connect(*natsURL, nopts...)
			} else {
				ownedNATS, nerr = coord.StartEmbedded(coord.EmbeddedConfig{
					NodeName: *domain,
					StoreDir: *natsStoreDir,
				})
			}
			if nerr != nil {
				_ = db.Close()
				return fmt.Errorf("remote backend nats: %w", nerr)
			}
			nc = ownedNATS.Conn()
		} else {
			// Embedded NATS + coordination (single-node/test): reuse it — no bulk
			// upload traffic to isolate.
			nc = coordn.nats.Conn()
		}

		// Manifest store on the SAME meta DB pool (do not open a second pool; do
		// not close db here — the single deferred/explicit db.Close at shutdown
		// owns it).
		if err := manifeststore.Migrate(ctx, db); err != nil {
			if ownedNATS != nil {
				ownedNATS.Close()
			}
			_ = db.Close()
			return fmt.Errorf("remote backend: migrate manifest store: %w", err)
		}
		manifests := manifeststore.New(db, slog.Default())

		rmt, err := chunkstore.NewRemote(chunkstore.RemoteConfig{
			Tenant:          *domain,
			PackTargetBytes: *packTarget,
			Requester:       remoteindex.NewNatsRequester(nc, 0),
			HTTP:            s3http.NewClient(),
			Manifests:       manifests,
			Logger:          slog.Default(),
			PresignTTL:      *presignTTL,
			Compression:     compPolicy, // nil → NewRemote default (uncompressed)
			PackCompression: packFramingMode,
			Meter:           mreg.Meter(), // backing-store RED (backend=s3); noop when metrics off
		})
		if err != nil {
			if ownedNATS != nil {
				ownedNATS.Close()
			}
			_ = db.Close()
			return fmt.Errorf("remote chunk store: %w", err)
		}
		csStore = rmt.Store
		// Lazy-formatted (slog key/value, no eager fmt.Sprintf).
		slog.Info("mlfs: remote backend", "tenant", *domain)
	} else {
		var localOpts []chunkstore.LocalOption
		if compPolicy != nil {
			localOpts = append(localOpts, chunkstore.WithCompression(compPolicy))
		}
		localOpts = append(localOpts, chunkstore.WithPackCompression(packFramingMode))
		// Backing-store RED (backend=local); noop meter when metrics are disabled.
		localOpts = append(localOpts, chunkstore.WithMeter(mreg.Meter()))
		local, err = chunkstore.NewLocal(*dataDir, *domain, *packTarget, localOpts...)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("chunk store: %w", err)
		}
		csStore = local.Store
	}

	dc, err := cache.New(csStore, cache.Config{
		Dir:               *cacheDir,
		StagingDir:        *stagingDir,
		MinFreeFraction:   *cacheMinFree,
		UploadConcurrency: uploadConc,
		AutoUpload:        true,
		MaxBytes:          *cacheBytes,
		Metrics:           mreg,
		OnCorrupt: func(id uint64, path string, cause error) {
			slog.Error("mlfs: corrupt staging entry quarantined", "id", id, "path", path, "cause", cause)
		},
	})
	if err != nil {
		return fmt.Errorf("disk cache: %w", err)
	}

	// Local data WAL (L2.4) + crash recovery. The chunk data itself was already
	// recovered into the backing store by the cache's staging drain above; the
	// WAL recovers the METADATA side — slice_ref rows whose commit was lost to a
	// crash after the data was staged + logged. Replay runs here, after the
	// ownership coordinator is registered (so the fence governs it) and before we
	// serve, so no live op interleaves with recovery.
	var w *wal.WAL
	if *walEnabled {
		dir := *walDir
		if dir == "" {
			dir = filepath.Join(*cacheDir, "wal")
		}
		w, err = wal.Open(dir)
		if err != nil {
			_ = dc.Close()
			_ = db.Close()
			return fmt.Errorf("open wal: %w", err)
		}
		res, rerr := fileio.ReplayWAL(ctx, w, engine)
		if rerr != nil {
			_ = w.Close()
			_ = dc.Close()
			_ = db.Close()
			return fmt.Errorf("wal replay: %w", rerr)
		}
		if res.Applied > 0 || res.Skipped > 0 {
			slog.Info("mlfs: wal recovery", "applied", res.Applied, "skipped", res.Skipped)
		}
	}

	// Data path + FUSE bridge.
	var files *fileio.Files
	if w != nil {
		files = fileio.NewWithWAL(engine, dc, w)
	} else {
		files = fileio.New(engine, dc)
	}
	// Sequential-read prefetch: on a detected sequential stream, warm -readahead-bytes
	// ahead into the cache so cold backing (S3) + metadata (PG) round trips are paid
	// before the demand read, not on it. Best-effort and bounded; 0 disables it.
	files.EnableReadahead(*readaheadBytes, *readaheadConcurrency, mreg)
	bridge := fusebridge.New(engine, files, dc)
	bridge.SetMetrics(mreg)
	switch strings.ToLower(strings.TrimSpace(*defaultStoreClass)) {
	case "", "default", "compressible":
		// ClassDefault (0): generic files compress under a compressing policy.
	case "uncompressed", "none":
		bridge.SetDefaultStoreClass(uint8(chunkstore.ClassUncompressed))
	default:
		return fmt.Errorf("invalid -default-store-class %q (valid: default, uncompressed)", *defaultStoreClass)
	}
	srv, err := fusebridge.Mount(bridge, *mount, &fuse.MountOptions{
		Name: "mlfs", FsName: *domain,
		MaxWrite:      *maxRWSize,     // 0 → Mount applies the 1 MiB default
		MaxReadAhead:  *maxRWSize,     // 0 → Mount applies the 1 MiB default
		MaxBackground: *maxBackground, // 0 → go-fuse default (12)
	}, *allowOther)
	if err != nil {
		_ = dc.Close()
		_ = db.Close()
		return fmt.Errorf("mount %s: %w", *mount, err)
	}
	// Record our PID in a data-dir lock file so offline tools (mlfs-admin
	// fsck/gc) can detect this live mount and refuse to run against it (they
	// would otherwise race the daemon and risk reaping an open-but-unlinked
	// inode). Removed on clean shutdown below.
	if err := mountlock.Write(*dataDir); err != nil {
		slog.Warn("mlfs: could not write mount lock file (offline tools will not detect this mount)", "err", err)
	}

	mreg.MountEvent("mount") // mount-flap canary: repeated mount/unmount cycling
	slog.Info("mlfs: mounted", "mount", *mount, "data_dir", *dataDir, "cache_dir", *cacheDir,
		"domain", *domain, "region", *region, "node_id", *nodeID)

	// Start the coordination loops (lease refresh, node heartbeat, GC leader
	// election, handoff responder) once we are serving. The handoff release hook
	// flushes the write-back cache so an incoming owner can read this node's data
	// from the shared backing store.
	if coordn != nil {
		coordn.owner.SetReleaseHook(func(ctx context.Context, scope string) error {
			return dc.Flush(ctx)
		})
		if err := coordn.run(coordCtx); err != nil {
			return err
		}
		// Cross-node cache invalidation feed (latency only; close-to-open is
		// already correct). The leader publishes changelog tail entries; this node
		// applies them to the FUSE caches.
		if err := coordn.startChangefeed(coordCtx, engine, bridge, *leaseRefresh); err != nil {
			return fmt.Errorf("changefeed: %w", err)
		}
		slog.Info("mlfs: coordination enabled", "node_id", *nodeID,
			"lease_ttl", *leaseTTL, "lease_refresh", *leaseRefresh)
	}

	// Optional in-daemon maintenance loop (#26/#8): periodically reclaim orphaned
	// slices (GC), trim the durable changelog, and (multi-node) reap dead peers'
	// locks. In multi-node mode this runs only on the GC leader, closing the
	// double-sweep hazard (#29). The goroutine stops cleanly when maintCtx is
	// cancelled at shutdown.
	maintCtx, stopMaint := context.WithCancel(ctx)
	defer stopMaint()
	if *maintInterval > 0 {
		if local != nil {
			go runMaintenance(maintCtx, engine, local, coordn, *maintInterval, *changelogRetention)
			slog.Info("mlfs: maintenance loop enabled", "interval", *maintInterval,
				"changelog_retention", *changelogRetention, "leader_gated", coordn != nil)
		} else {
			// Remote mode: casstore GC is blobgw-owned (it holds the tenant
			// credentials and the authoritative dedup index), so the node never
			// constructs/runs the casstore ChunkedGC. The current maintenance loop
			// is GC-coupled (gc.New requires a *chunkstore.Local), so it is skipped
			// wholesale here; node-appropriate metadata maintenance (changelog trim,
			// lock reaping) would need a GC-free maintenance loop and is out of scope
			// for this change.
			slog.Info("mlfs: maintenance loop skipped (remote backend; GC is blobgw-owned)")
		}
	}

	// Serve until signalled, then unmount cleanly and flush the cache.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	s := <-sig
	slog.Info("mlfs: shutting down", "signal", s.String())
	stopMaint() // halt maintenance before tearing the mount down
	stopCoord() // release leases/heartbeat/leadership (loops watch coordCtx)

	if err := unmountWithRetry(srv); err != nil {
		slog.Error("mlfs: unmount failed", "err", err)
	}
	mreg.MountEvent("unmount") // pairs with the mount event for the flap canary
	if err := mountlock.Remove(*dataDir); err != nil {
		slog.Error("mlfs: remove mount lock file failed", "err", err)
	}
	// Stop the readahead prefetcher (and wait for in-flight prefetches) BEFORE
	// closing the cache, so no speculative read races the staging drain.
	if err := files.Close(); err != nil {
		slog.Error("mlfs: file path close failed", "err", err)
	}
	if err := dc.Close(); err != nil { // drains staging to the backing store
		slog.Error("mlfs: cache close (staging drain) failed", "err", err)
	}
	if w != nil {
		// All writes have committed to the metadata engine by now; checkpoint past
		// the last logged seq so recovered segments are dropped, then close.
		if err := w.Checkpoint(w.LastSeq()); err != nil {
			slog.Error("mlfs: wal checkpoint failed", "err", err)
		}
		if err := w.Close(); err != nil {
			slog.Error("mlfs: wal close failed", "err", err)
		}
	}
	// Remote backend that owns its NATS connection (coordination disabled) drains
	// it here, AFTER the cache staging drain above (final S3 transfers + dedup
	// records ride this connection) and BEFORE db.Close. When coordination is
	// enabled the connection is REUSED from coordn and drained by coordn.Close
	// (deferred earlier) — we do NOT double-close it.
	if ownedNATS != nil {
		ownedNATS.Close()
	}
	if err := db.Close(); err != nil {
		slog.Error("mlfs: meta db close failed", "err", err)
	}
	slog.Info("mlfs: stopped")
	return nil
}

// unmountWithRetry retries Unmount briefly: a just-signalled mount may still
// have the kernel holding a reference (EBUSY) for a moment.
func unmountWithRetry(srv *fuse.Server) error {
	var err error
	for i := 0; i < 20; i++ {
		if err = srv.Unmount(); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return err
}

// runMaintenance ticks every interval and, each tick, runs one GC pass over the
// shared engine+chunk store and trims changelog entries older than retention. It
// returns when ctx is cancelled (shutdown). Errors are logged, not fatal: a
// failed pass should never bring the mount down. Single-node scope (see #29).
func runMaintenance(ctx context.Context, engine *meta.Engine, local *chunkstore.Local, coordn *coordination, interval, retention time.Duration) {
	g := gc.New(engine, local)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// In multi-node mode only the GC leader performs cluster-wide
			// singletons (GC, changelog trim, lock reaping); followers skip.
			if coordn != nil && !coordn.leader.IsLeader() {
				continue
			}
			res, err := g.Run(ctx)
			if err != nil {
				slog.Error("mlfs: maintenance gc failed", "err", err)
			} else {
				slog.Info("mlfs: maintenance gc", "orphans_deleted", res.SlicesDeleted,
					"chunks_reclaimed", res.Cas.ChunksReclaimed, "bytes_reclaimed", res.Cas.BytesReclaimed)
			}
			trimmed, err := engine.TrimChangelogOlderThan(ctx, retention)
			if err != nil {
				slog.Error("mlfs: maintenance changelog trim failed", "err", err)
			} else {
				slog.Info("mlfs: maintenance changelog trim", "entries_removed", trimmed)
			}
			if coordn != nil {
				reaped, err := coordn.reg.ReapDeadLocks(ctx, engine)
				if err != nil {
					slog.Error("mlfs: maintenance lock reap failed", "err", err)
				} else if reaped > 0 {
					slog.Info("mlfs: maintenance lock reap", "dead_sessions_cleared", reaped)
				}
			}
		}
	}
}

// startMetricsHTTP serves a Prometheus /metrics scrape endpoint on addr from the
// daemon's registry, for centralized scraping (the same counters the in-mount
// virtual file exposes). Returns a stop function that drains the server.
func startMetricsHTTP(addr string, mreg *metrics.Registry) func() {
	mux := nethttp.NewServeMux()
	mux.Handle("/metrics", mreg.Handler())
	srv := &nethttp.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, nethttp.ErrServerClosed) {
			slog.Error("mlfs: metrics http server", "addr", addr, "err", err)
		}
	}()
	slog.Info("mlfs: metrics scrape endpoint up", "addr", addr, "path", "/metrics")
	return func() {
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// dataPathOwnsNATS reports whether the -remote data path opens its OWN NATS
// connection (vs reusing coordination's). It ALWAYS owns a DEDICATED connection
// on external NATS, so bulk presign/upload traffic is isolated from
// coordination's lease/heartbeat — a shared connection lets parallel uploads
// starve coordination and self-fence the mount (the mount-flap regression). It
// also owns one when coordination is disabled. Only embedded-NATS + coordination
// (single-node/test, no bulk uploads) reuses the coordination connection.
func dataPathOwnsNATS(natsURL string, coordEnabled bool) bool {
	return natsURL != "" || !coordEnabled
}

// splitCSV splits a comma-separated flag value into trimmed, non-empty parts.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Command mlfs-csi is the Kubernetes CSI driver for mlfs (L2.11). It runs in the
// node DaemonSet beside a shared mlfs FUSE mount and serves the Identity,
// Controller, and Node gRPC services over a unix socket. A volume is a
// subdirectory of the shared mount; NodePublishVolume bind-mounts it into the
// pod. Multi-node safety is mlfs's job (ownership leases / fence, L2.8).
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/scitrera/memorylayer-storage/mlfs-csi/internal/driver"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	endpoint := flag.String("endpoint", "unix:///csi/csi.sock", "CSI gRPC endpoint")
	nodeID := flag.String("node-id", envOr("NODE_ID", ""), "node identity (typically spec.nodeName via the downward API)")
	root := flag.String("mlfs-root", "", "legacy single shared mlfs FUSE mount (optional; the no-domain publish path + controller meta)")
	domainsBase := flag.String("domains-base", "", "base dir for per-domain mlfs mounts (enables multi-tenant multi-domain-per-node; a hostPath in prod)")
	mlfsBin := flag.String("mlfs-bin", envOr("MLFS_BINARY", "/usr/local/bin/mlfs"), "path to the mlfs binary (launch-mode=process)")
	defaultNATSURL := flag.String("default-nats-url", envOr("MLFS_NATS_URL", ""), "fallback NATS URL when a StorageClass omits the natsUrl parameter")
	launchMode := flag.String("launch-mode", envOr("MLFS_LAUNCH_MODE", "process"), "how per-domain mlfs runs: 'pod' (k8s mount pod; survives a plugin-container restart) or 'process' (supervised child; non-k8s/local/tests)")
	mountPodNS := flag.String("mount-pod-namespace", envOr("MLFS_MOUNT_POD_NAMESPACE", "kube-system"), "namespace for mount pods (launch-mode=pod)")
	mountPodImage := flag.String("mount-pod-image", envOr("MLFS_MOUNT_POD_IMAGE", ""), "image running the mlfs daemon for mount pods (launch-mode=pod)")
	cacheMaxBytes := flag.Int64("cache-max-bytes", 0, "per-domain mlfs clean-cache cap in bytes (0 = unlimited)")
	cacheMinFree := flag.Float64("cache-min-free-fraction", 0, "per-domain mlfs cache: keep ≥ this fraction of the cache disk free (0 = disabled; e.g. 0.15). Lets the LRU self-size per node")
	cacheBase := flag.String("cache-base", envOr("MLFS_CACHE_BASE", ""), "separate hostPath root for the per-domain cache, e.g. a fast NVMe (empty = under -domains-base). NOTE: this dir holds the durable write-back staging too, so on ephemeral storage un-drained writes are lost on node termination")
	packTargetBytes := flag.Int64("pack-target-bytes", 0, "per-domain mlfs casstore pack target in bytes (0 = mlfs default ~16 MiB)")
	uploadConcurrency := flag.Int("upload-concurrency", 0, "per-domain mlfs write-back upload parallelism (0 = defer to mlfs auto = min(NumCPU,16); 1 = serial; N = explicit). Safe with the dedicated data-path NATS connection (uploads no longer starve coordination)")
	maxBackground := flag.Int("max-background", 0, "per-NODE mlfs FUSE -max-background (concurrent readahead/in-flight requests; 0 = mlfs default 12). Set 64-256 on read-heavy node pools, e.g. GPU model loading, to keep many parallel reads in flight")
	maxRWSize := flag.Int64("max-rw-size", 0, "per-NODE mlfs FUSE -max-rw-size (max read/write request bytes; 0 = mlfs default = the 1 MiB kernel cap)")
	dbMaxOpenConns := flag.Int("db-max-open-conns", 0, "per-NODE mlfs metadata PG pool bound -db-max-open-conns (0 = mlfs default 8). LOWER (e.g. 4) on nodes running many domains so N daemons don't exhaust Postgres max_connections; raise alongside -max-background on read-heavy nodes")
	readaheadBytes := flag.Int64("readahead-bytes", 0, "per-NODE mlfs sequential-read prefetch window -readahead-bytes (0 = mlfs default 16 MiB, on). RAISE (e.g. 64-128 MiB) on GPU model-loading pools to hide more cold S3 latency ahead of the read")
	readaheadConcurrency := flag.Int("readahead-concurrency", 0, "per-NODE mlfs max in-flight prefetch backing fetches -readahead-concurrency (0 = mlfs default 8). The cold-load throughput knob; raise (e.g. 32-64) on GPU pools and scale -db-max-open-conns with it")
	compression := flag.String("compression", envOr("MLFS_COMPRESSION", ""), "per-NODE mlfs file-type compression policy -compression (none|zstd|gzip; empty = mlfs backend default = -remote all-uncompressed). Set zstd to opt -remote into MIXED mode: model/tensor files (by extension) stay uncompressed via their per-slice tag while generic data compresses. Pair with -default-store-class=uncompressed on a model-heavy pool")
	packFraming := flag.String("pack-framing", envOr("MLFS_PACK_FRAMING", ""), "per-NODE mlfs -pack-framing (per-chunk|whole-pack; empty = mlfs default per-chunk) — framing for COMPRESSED packs only. per-chunk keeps compressed packs range-readable; whole-pack is the better-ratio rollback. No effect on uncompressed (model/tensor) slices, so this only matters when -compression is set")
	defaultStoreClass := flag.String("default-store-class", envOr("MLFS_DEFAULT_STORE_CLASS", ""), "per-NODE mlfs -default-store-class (default|uncompressed; empty = mlfs default 'default') — compression class for files NOT a recognized model/tensor type. Use 'uncompressed' on a model-heavy pool so an unrecognized model is never compressed (only takes effect with -compression set)")
	// OTLP/gRPC metrics: when set, every per-domain mlfs mount pushes its fs metrics
	// to this central OpenTelemetry collector (mlfs's own -otel-endpoint/-otel-insecure
	// flags). The central (mt) collector stamps scitrera.tenant=_platform so storage
	// telemetry lands in the platform plane; mlfs's mlfs.domain/mlfs.region resource
	// attributes distinguish the storage domain. Empty endpoint = no otel flags emitted.
	mountOTELEndpoint := flag.String("mount-otel-endpoint", envOr("MLFS_OTEL_ENDPOINT", ""), "per-domain mlfs -otel-endpoint (OTLP/gRPC collector host:port pushed to every mount pod; empty = no fs-metrics push)")
	mountOTELInsecure := flag.Bool("mount-otel-insecure", envBool("MLFS_OTEL_INSECURE", true), "per-domain mlfs -otel-insecure (plaintext OTLP/gRPC; default true — the in-cluster collector has no TLS)")
	mountOTELTraceSampleRatio := flag.Float64("mount-otel-trace-sample-ratio", envFloat("MLFS_OTEL_TRACE_SAMPLE_RATIO", 0), "per-domain mlfs -otel-trace-sample-ratio (root FUSE-span head-sampling; 0 = mlfs default 1.0). FUSE ops are high-frequency, so set <1 (e.g. 0.05) to keep traces+exemplars without flooding the collector; the RED metric histograms stay full regardless")
	// Domain-mount handoff tuning (fixes the ~30s sandbox kill+recreate flap). Grace:
	// keep a domain mount alive briefly after its last PVC leaves so a quick recreate
	// re-adopts the running mount instead of restarting the daemon + waiting out the
	// coordination lease TTL. Ready-timeout: must exceed the mlfs coordination lease
	// TTL (default 30s) so the first NodePublish survives a worst-case daemon handoff.
	mountLingerGrace := flag.Duration("mount-linger-grace", envDur("MLFS_MOUNT_LINGER_GRACE", 180*time.Second), "keep a domain mount alive this long after its LAST PVC unpublishes before teardown, so a quick pod kill+recreate re-adopts the live mount instead of paying a fresh mlfs start + coordination handoff. 0 = immediate teardown")
	mountReadyTimeout := flag.Duration("mount-ready-timeout", envDur("MLFS_MOUNT_READY_TIMEOUT", 60*time.Second), "how long NodePublish waits for a freshly-launched domain mount to serve. Keep > the mlfs coordination lease TTL (default 30s) so the first publish survives a worst-case daemon handoff")
	flag.Parse()

	if *nodeID == "" {
		slog.Error("mlfs-csi: -node-id (or $NODE_ID) is required")
		os.Exit(1)
	}
	if *cacheMinFree < 0 || *cacheMinFree >= 1 {
		slog.Error("mlfs-csi: -cache-min-free-fraction must be in [0,1)", "value", *cacheMinFree)
		os.Exit(1)
	}
	cacheOpts := driver.CacheOpts{
		Base:                 *cacheBase,
		MaxBytes:             *cacheMaxBytes,
		MinFreeFraction:      *cacheMinFree,
		PackTargetBytes:      *packTargetBytes,
		UploadConcurrency:    *uploadConcurrency,
		MaxBackground:        *maxBackground,
		MaxRWSize:            *maxRWSize,
		DBMaxOpenConns:       *dbMaxOpenConns,
		ReadaheadBytes:       *readaheadBytes,
		ReadaheadConcurrency: *readaheadConcurrency,
		Compression:          *compression,
		PackFraming:          *packFraming,
		DefaultStoreClass:    *defaultStoreClass,
		OTELEndpoint:         *mountOTELEndpoint,
		OTELInsecure:         *mountOTELInsecure,
		OTELTraceSampleRatio: *mountOTELTraceSampleRatio,
	}

	mounter := driver.NewLinuxMounter()

	// Multi-domain mount manager: one mlfs mount per domain per node, ref-counted,
	// brought up lazily per PVC. Enabled when -domains-base is set; otherwise the
	// driver serves only the legacy single shared mount (-mlfs-root).
	var mgr *driver.MountManager
	if *domainsBase != "" {
		var launcher driver.Launcher
		switch *launchMode {
		case "pod":
			// Production: each domain's mlfs runs as a k8s mount pod (a peer pod,
			// not a CSI-container child), so a plugin restart/upgrade does not
			// cgroup-kill it.
			if *mountPodImage == "" {
				slog.Error("mlfs-csi: -mount-pod-image is required for -launch-mode=pod")
				os.Exit(1)
			}
			l, lerr := driver.NewK8sPodLauncher(*mountPodNS, *nodeID, *mountPodImage, *domainsBase, cacheOpts)
			if lerr != nil {
				slog.Error("mlfs-csi: build mount-pod launcher", "err", lerr)
				os.Exit(1)
			}
			launcher = l
		case "process":
			// Supervised child process — non-k8s / local / integration tests. Does
			// NOT survive a CSI-container restart (children share its cgroup scope).
			launcher = driver.NewExecLauncherWithCache(*mlfsBin, cacheOpts)
		default:
			slog.Error("mlfs-csi: invalid -launch-mode (want 'pod' or 'process')", "launch_mode", *launchMode)
			os.Exit(1)
		}
		mgr = driver.NewMountManager(*domainsBase, launcher, mounter)
		mgr.SetLingerGrace(*mountLingerGrace)
		mgr.SetReadyTimeout(*mountReadyTimeout)
		// Re-adopt domains whose mlfs mounts survived a prior plugin restart before
		// serving, so live PVCs are not disturbed.
		if err := mgr.Adopt(context.Background()); err != nil {
			slog.Error("mlfs-csi: adopt existing domain mounts", "err", err)
			os.Exit(1)
		}
	}

	d, err := driver.New(driver.Config{
		Endpoint:       *endpoint,
		NodeID:         *nodeID,
		Root:           *root,
		DefaultNATSURL: *defaultNATSURL,
	}, mounter, mgr)
	if err != nil {
		slog.Error("mlfs-csi: init", "err", err)
		os.Exit(1)
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		s := <-sig
		slog.Info("mlfs-csi: shutting down", "signal", s.String())
		d.Stop()
	}()

	slog.Info("mlfs-csi: serving", "endpoint", *endpoint, "node_id", *nodeID,
		"mlfs_root", *root, "domains_base", *domainsBase, "multi_domain", mgr != nil)
	if err := d.Run(); err != nil {
		slog.Error("mlfs-csi: serve", "err", err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// envBool parses a boolean env var (1/t/true/0/f/false, case-insensitive),
// falling back to def when unset or unparseable. Used for -mount-otel-insecure
// so an in-cluster plaintext collector is the default.
func envBool(k string, def bool) bool {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

// envFloat parses a float env var, falling back to def when unset or unparseable.
// Used for -mount-otel-trace-sample-ratio.
func envFloat(k string, def float64) float64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

// envDur parses a Go duration env var (e.g. "90s", "2m"), falling back to def when
// unset or unparseable. Used for -mount-linger-grace / -mount-ready-timeout.
func envDur(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

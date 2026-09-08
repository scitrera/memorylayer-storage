// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

//go:build linux

package driver

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// execLauncher spawns the real mlfs binary. The child is started in its own
// session (Setsid) so it is reparented to host init rather than reaped when the
// CSI process exits: paired with the DaemonSet's hostPID + a hostPath mount dir
// (Bidirectional propagation), a per-domain mount survives a node-plugin
// restart and is re-adopted by the manager (§7.3). The manager owns ret/ signal
// lifecycle; this type only knows how to spawn and signal a process.
type execLauncher struct {
	binary string    // path to the mlfs binary (default /usr/local/bin/mlfs)
	args   MlfsArgs  // builds the mlfs CLI for a domain mount
	cache  CacheOpts // per-domain disk-cache sizing/placement
}

// MlfsArgs builds the mlfs command-line for a domain mount. It is injectable so
// operators can override mlfs flags without a code change, and so integration
// tests can drive the real binary in local single-node mode. Production uses
// remoteMlfsArgs.
type MlfsArgs func(spec DomainSpec, mountDir, dataDir, cacheDir, credsPath string) []string

// CacheOpts configures the per-domain mlfs disk cache (staging + clean LRU),
// passed through to the daemon. Base relocates the CLEAN read cache off the
// domains-base disk (e.g. onto a fast instance-store NVMe); MaxBytes /
// MinFreeFraction bound its size. When Base is set, the launcher keeps the
// DURABLE write-back (mlfs -staging-dir) and the WAL (-wal-dir) on the
// persistent domains-base dataDir, so a Base on ephemeral storage no longer
// trades away the "acknowledged write survives node termination" guarantee —
// only the reclaimable clean cache lives on the fast/ephemeral disk.
type CacheOpts struct {
	Base            string  // separate cache disk root; empty = under the domain's data dir
	MaxBytes        int64   // -cache-max-bytes (0 = unlimited)
	MinFreeFraction float64 // -cache-min-free-fraction (0 = disabled; e.g. 0.15)
	// PackTargetBytes (-pack-target-bytes) sets the casstore pack size; 0 = mlfs
	// default (~16 MiB). UploadConcurrency (-upload-concurrency) bounds parallel
	// write-back pack uploads: 0 defers to mlfs's auto default (min(NumCPU,16)),
	// 1 forces serial, N is explicit. On the high-latency -remote path serial
	// uploads are round-trip-bound (see docs/MLFS_REMOTE_WRITE_THROUGHPUT_DESIGN.md).
	PackTargetBytes   int64
	UploadConcurrency int

	// Per-NODE mlfs tuning, forwarded to the daemon. Set these high on read-heavy
	// node pools (e.g. GPU model loading) via the CSI DaemonSet, default elsewhere.
	// MaxBackground (-max-background): concurrent FUSE readahead/in-flight requests
	// (mlfs default 12); 64-256 lets a model loader keep many shard reads in flight.
	// MaxRWSize (-max-rw-size): FUSE max read/write size (mlfs default = the 1 MiB
	// kernel cap). DBMaxOpenConns (-db-max-open-conns): metadata PG pool bound
	// (mlfs default 8 — the throughput sweet spot); lower it (e.g. 4) on nodes
	// running many domains so N daemons don't exhaust Postgres max_connections.
	// ReadaheadBytes (-readahead-bytes): sequential-read prefetch window (mlfs
	// default 16 MiB, on); RAISE it (e.g. 64-128 MiB) on GPU model-loading pools to
	// hide more cold S3 latency ahead of the read. 0 leaves the mlfs default.
	// ReadaheadConcurrency (-readahead-concurrency): max in-flight prefetch backing
	// fetches (mlfs default 8). On a cold load this is the throughput knob; raise it
	// (e.g. 32-64) on GPU pools and scale DBMaxOpenConns with it (each prefetched
	// slice also does a metadata query). 0 leaves the mlfs default.
	MaxBackground        int
	MaxRWSize            int64
	DBMaxOpenConns       int
	ReadaheadBytes       int64
	ReadaheadConcurrency int

	// Compression (-compression: none|zstd|gzip) and DefaultStoreClass
	// (-default-store-class: default|uncompressed) select the mlfs file-type
	// compression policy for this node pool's domains. Empty leaves the mlfs
	// backend default (-remote: all-uncompressed). Set Compression=zstd to opt
	// -remote into MIXED mode: files classified model/tensor (by extension) stay
	// uncompressed (range-readable + mmap) via their per-slice tag while generic
	// data compresses. On a model-heavy pool pair it with
	// DefaultStoreClass=uncompressed so an unrecognized file is never compressed.
	// These align with the node pool's workload, like the readahead knobs above.
	Compression       string
	PackFraming       string
	DefaultStoreClass string

	// OTELEndpoint (-otel-endpoint host:port) and OTELInsecure (-otel-insecure)
	// point each per-domain mlfs at a central OTLP/gRPC OpenTelemetry collector so
	// the mlfs binary pushes its filesystem metrics there. When OTELEndpoint is
	// empty no otel flags are added (mlfs runs metrics-free, preserving current
	// mount pods). The central (mt) collector stamps scitrera.tenant=_platform so
	// storage telemetry lands in the platform plane; mlfs's own mlfs.domain /
	// mlfs.region resource attributes distinguish the storage domain. OTELInsecure
	// defaults true because the in-cluster collector serves plaintext gRPC.
	OTELEndpoint string
	OTELInsecure bool
	// OTELTraceSampleRatio (-otel-trace-sample-ratio) head-samples ROOT mlfs FUSE
	// trace spans. FUSE ops are high-frequency, so tracing EVERY op (the mlfs binary
	// default 1.0) floods the collector; a low ratio keeps representative traces +
	// latency exemplars while the RED metric histograms stay FULL (sampling only
	// affects spans, not metrics). 0 = leave the mlfs binary default. Only appended
	// when OTELEndpoint is set.
	OTELTraceSampleRatio float64
}

func (o CacheOpts) dirFor(dataDir, domain string) string {
	if o.Base != "" {
		return filepath.Join(o.Base, domainDirKey(domain), "cache")
	}
	return dataDir + "/cache"
}

func (o CacheOpts) appendFlags(args []string, dataDir string) []string {
	if o.MaxBytes > 0 {
		args = append(args, "-cache-max-bytes", strconv.FormatInt(o.MaxBytes, 10))
	}
	if o.MinFreeFraction > 0 {
		args = append(args, "-cache-min-free-fraction", strconv.FormatFloat(o.MinFreeFraction, 'f', -1, 64))
	}
	if o.PackTargetBytes > 0 {
		args = append(args, "-pack-target-bytes", strconv.FormatInt(o.PackTargetBytes, 10))
	}
	if o.UploadConcurrency > 0 {
		// Any explicit value is forwarded (1 forces serial); 0 leaves mlfs to its
		// auto default (min(NumCPU,16)).
		args = append(args, "-upload-concurrency", strconv.Itoa(o.UploadConcurrency))
	}
	if o.MaxBackground > 0 {
		args = append(args, "-max-background", strconv.Itoa(o.MaxBackground))
	}
	if o.MaxRWSize > 0 {
		args = append(args, "-max-rw-size", strconv.FormatInt(o.MaxRWSize, 10))
	}
	if o.DBMaxOpenConns > 0 {
		// mlfs caps idle to open via database/sql, so we need only set open here.
		args = append(args, "-db-max-open-conns", strconv.Itoa(o.DBMaxOpenConns))
	}
	if o.ReadaheadBytes > 0 {
		args = append(args, "-readahead-bytes", strconv.FormatInt(o.ReadaheadBytes, 10))
	}
	if o.ReadaheadConcurrency > 0 {
		args = append(args, "-readahead-concurrency", strconv.Itoa(o.ReadaheadConcurrency))
	}
	if o.Compression != "" {
		args = append(args, "-compression", o.Compression)
	}
	if o.PackFraming != "" {
		args = append(args, "-pack-framing", o.PackFraming)
	}
	if o.DefaultStoreClass != "" {
		args = append(args, "-default-store-class", o.DefaultStoreClass)
	}
	if o.OTELEndpoint != "" {
		// Point this domain's mlfs at the central OTLP/gRPC collector so it pushes
		// its fs metrics there; empty endpoint adds nothing (no behavior change).
		args = append(args, "-otel-endpoint", o.OTELEndpoint)
		if o.OTELInsecure {
			args = append(args, "-otel-insecure")
		}
		if o.OTELTraceSampleRatio > 0 {
			// Head-sample root FUSE spans (high-frequency) so traces stay meaningful
			// without flooding the collector; metric histograms are unaffected.
			args = append(args, "-otel-trace-sample-ratio",
				strconv.FormatFloat(o.OTELTraceSampleRatio, 'g', -1, 64))
		}
	}
	if o.Base != "" {
		// The clean cache (-cache-dir) is on the possibly-ephemeral Base; pin the
		// durable write-back staging and the WAL to the persistent domains-base
		// dataDir so an acked write still survives node termination.
		args = append(args, "-staging-dir", dataDir, "-wal-dir", filepath.Join(dataDir, "wal"))
	}
	return args
}

// NewExecLauncher returns the production Launcher (converged -remote backend).
// binary is the mlfs executable path; empty defaults to /usr/local/bin/mlfs.
func NewExecLauncher(binary string) Launcher {
	return NewExecLauncherWithArgs(binary, remoteMlfsArgs)
}

// NewExecLauncherWithArgs returns a Launcher with a custom arg builder.
func NewExecLauncherWithArgs(binary string, args MlfsArgs) Launcher {
	if binary == "" {
		binary = "/usr/local/bin/mlfs"
	}
	if args == nil {
		args = remoteMlfsArgs
	}
	return execLauncher{binary: binary, args: args}
}

// NewExecLauncherWithCache returns the production exec Launcher with cache
// sizing/placement options.
func NewExecLauncherWithCache(binary string, cache CacheOpts) Launcher {
	if binary == "" {
		binary = "/usr/local/bin/mlfs"
	}
	return execLauncher{binary: binary, args: remoteMlfsArgs, cache: cache}
}

// remoteMlfsArgs is the production arg set: converged direct-S3 backend
// (-remote) — per-domain dedup boundary, NATS account creds, PG meta, exactly
// the per-process isolation the gap doc asks to multiplex N-per-node (§4.4). The
// node holds no S3 creds; blobgw resolves the tenant bucket.
func remoteMlfsArgs(spec DomainSpec, mountDir, dataDir, cacheDir, credsPath string) []string {
	return []string{
		"-remote",
		// Pods consume the volume (a bind mount of a subdir) as arbitrary,
		// usually non-root uids; without allow_other the FUSE kernel denies every
		// non-mounter uid with EACCES before permission checks run. Required for the
		// subdir-bind-mount model.
		"-allow-other",
		"-domain", spec.Domain,
		"-meta-dsn", spec.MetaDSN,
		"-mount", mountDir,
		"-cache-dir", cacheDir,
		"-node-id", spec.NodeID + "-" + spec.Domain,
		"-nats-url", spec.NATSURL,
		"-nats-creds", credsPath,
		"-region", strconv.FormatUint(uint64(spec.Region), 10),
	}
}

func (l execLauncher) Start(ctx context.Context, spec DomainSpec, mountDir, dataDir, credsPath string) (MountHandle, error) {
	cacheDir := l.cache.dirFor(dataDir, spec.Domain)
	for _, d := range []string{dataDir, cacheDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, fmt.Errorf("create scratch dir %s: %w", d, err)
		}
	}
	cmd := exec.Command(l.binary, l.cache.appendFlags(l.args(spec, mountDir, dataDir, cacheDir, credsPath), dataDir)...)
	// Detach into a new session so a CSI-process exit does not SIGHUP/-reap the
	// mount daemon; the manager terminates it explicitly on the last unpublish.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdout = os.Stdout // mlfs logs JSON to stderr/stdout; surface in node-plugin logs
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start mlfs (domain %q): %w", spec.Domain, err)
	}
	// Reap the child when it exits so it does not linger as a zombie; the
	// manager's lifecycle is authoritative, this just prevents <defunct>.
	go func() { _ = cmd.Wait() }()
	return &execHandle{pid: cmd.Process.Pid, domain: spec.Domain}, nil
}

// Adopt reconstructs a handle for an mlfs child started by a prior plugin
// incarnation, from its persisted pid (Ref). Marked adopted so Stop applies the
// /proc identity guard (we did not spawn this exact process).
func (execLauncher) Adopt(ref, domain string) MountHandle {
	pid, _ := strconv.Atoi(ref)
	return &execHandle{pid: pid, domain: domain, adopted: true}
}

// execHandle signals a live (or re-adopted) mlfs process by pid. Ref is the pid
// as a string (persisted to state.json).
type execHandle struct {
	pid     int
	domain  string
	adopted bool // re-adopted across a restart ==> Stop uses the pid-reuse guard
}

func (h *execHandle) Ref() string { return strconv.Itoa(h.pid) }

func (h *execHandle) Stop(ctx context.Context) error {
	if h.adopted {
		return stopAdoptedPID(ctx, h.pid, h.domain)
	}
	return stopPID(ctx, h.pid)
}

// stopAdoptedPID terminates a re-adopted domain's mlfs process by pid, but only
// after confirming the pid STILL belongs to that domain's mlfs — across a
// restart the original process may have exited and the OS may have recycled its
// pid for an unrelated process. If the identity check fails (process gone, or
// the cmdline no longer matches `mlfs … -domain <domain>`), it is treated as
// already-stopped and returns nil rather than signaling a stranger.
func stopAdoptedPID(ctx context.Context, pid int, domain string) error {
	if pid <= 0 {
		return nil
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
	if err != nil {
		return nil // process gone ==> nothing to stop
	}
	// /proc cmdline is NUL-separated; join with spaces for substring matching.
	cmd := strings.ReplaceAll(string(b), "\x00", " ")
	if !strings.Contains(cmd, "mlfs") || !strings.Contains(cmd, "-domain "+domain) {
		slog.Warn("mlfs-csi: adopted pid no longer matches domain mlfs; skipping signal (pid reuse)",
			"pid", pid, "domain", domain)
		return nil
	}
	return stopPID(ctx, pid)
}

// stopPID sends SIGTERM and waits (bounded by ctx, with a default cap) for the
// process to disappear; it then escalates to SIGKILL. Used both for handles we
// launched and for pids re-adopted from persisted state after a restart.
func stopPID(ctx context.Context, pid int) error {
	if pid <= 0 {
		return nil
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return nil // not found ==> already gone
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		if err == os.ErrProcessDone || isESRCH(err) {
			return nil
		}
		return fmt.Errorf("SIGTERM pid %d: %w", pid, err)
	}
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = proc.Signal(syscall.SIGKILL)
			return ctx.Err()
		case <-deadline.C:
			_ = proc.Signal(syscall.SIGKILL)
			return nil
		case <-tick.C:
			// Signal 0 probes liveness without delivering a signal.
			if err := proc.Signal(syscall.Signal(0)); err != nil {
				return nil // process gone ==> clean unmount completed
			}
		}
	}
}

func isESRCH(err error) bool {
	return err == syscall.ESRCH
}

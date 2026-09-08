// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gc_test

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/gc"
	"github.com/scitrera/memorylayer-storage/casstore/blobstore"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// tenantBackends gives each tenant a physically distinct LOCAL casstore backend
// (its own manifests + chunks dir), exactly like a per-tenant S3 bucket without
// real S3 — mirroring gateway's localBackendFactory test fixture. It exposes a
// GCFunc over those backends plus a shared dedup index, matching what
// (*gateway.TenantRouter).GCForTenant yields in production.
type tenantBackends struct {
	t     *testing.T
	root  string
	dedup *snapshot.MemoryDedupStore

	mu     sync.Mutex
	chunks map[string]blobstore.Storage
}

func newTenantBackends(t *testing.T) *tenantBackends {
	t.Helper()
	// Each tenant resolves to a physically distinct local backend (its own
	// manifests + chunks dir), exactly like a per-tenant S3 bucket without real
	// S3. The Runner consumes the same GCFunc seam that
	// (*gateway.TenantRouter).GCForTenant satisfies in production.
	return &tenantBackends{
		t:      t,
		root:   t.TempDir(),
		dedup:  snapshot.NewMemoryDedupStore(),
		chunks: make(map[string]blobstore.Storage),
	}
}

// backendFor lazily creates the local stores for a tenant and remembers the
// chunk store so the test can inspect physical blob counts.
func (tb *tenantBackends) backendFor(ctx context.Context, tenant string) (snapshot.SnapshotStore, blobstore.Storage) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if cs, ok := tb.chunks[tenant]; ok {
		us, _ := snapshot.NewLocalStore(filepath.Join(tb.root, tenant, "manifests"))
		return us, cs
	}
	dir := filepath.Join(tb.root, tenant)
	us, err := snapshot.NewLocalStore(filepath.Join(dir, "manifests"))
	if err != nil {
		tb.t.Fatalf("local manifest store: %v", err)
	}
	cs, err := blobstore.NewLocalStorage(ctx, blobstore.LocalConfig{Root: filepath.Join(dir, "chunks")})
	if err != nil {
		tb.t.Fatalf("local chunk store: %v", err)
	}
	tb.t.Cleanup(func() { _ = cs.Close(context.Background()) })
	tb.chunks[tenant] = cs
	return us, cs
}

// gateway returns a per-tenant gateway over that tenant's local backend + the
// shared dedup index (DedupDomain=tenant), for writing test data.
func (tb *tenantBackends) gateway(ctx context.Context, tenant string) *gateway.Gateway {
	us, cs := tb.backendFor(ctx, tenant)
	store, err := snapshot.NewChunkedStore(us, cs, snapshot.ChunkedConfig{
		DedupDomain:       tenant,
		Index:             snapshot.NewGlobalIndex(tb.dedup, nil),
		CompressionPolicy: nil, // raw bytes for predictable physical assertions
	})
	if err != nil {
		tb.t.Fatalf("chunked store: %v", err)
	}
	return gateway.New(store, gateway.NewMemoryRefStore(), nil, tenant)
}

// gcFunc is the gc.GCFunc the Runner consumes: a per-tenant ChunkedGC over the
// tenant's own backend + the shared dedup index, exactly like
// (*gateway.TenantRouter).GCForTenant. now (when set) overrides the GC clock so
// the safety window can be exercised deterministically.
func (tb *tenantBackends) gcFunc(now func() time.Time) gc.GCFunc {
	return func(ctx context.Context, tenant string) (*snapshot.ChunkedGC, error) {
		us, cs := tb.backendFor(ctx, tenant)
		g := snapshot.NewChunkedGC(us, cs, nil)
		g.Index = tb.dedup
		if now != nil {
			g.SetClock(now)
		}
		return g, nil
	}
}

// countPacks totals the live pack/chunk blobs in a tenant's backend.
func (tb *tenantBackends) countPacks(tenant string) int {
	tb.mu.Lock()
	cs := tb.chunks[tenant]
	tb.mu.Unlock()
	if cs == nil {
		return 0
	}
	count := 0
	for _, pfx := range []blobstore.ID{
		blobstore.ID("pack-" + tenant + "-"),
		blobstore.ID("chunk-" + tenant + "-"),
	} {
		_ = cs.ListBlobs(context.Background(), pfx, func(blobstore.Metadata) error {
			count++
			return nil
		})
	}
	return count
}

// TestRunner_SweepsUnreferencedRespectsSafetyWindow writes packs for a tenant,
// removes the manifest references (making the packs orphaned), then runs a GC
// pass through the Runner's per-tenant seam and asserts:
//   - with the safety window LARGER than the packs' age, nothing is reclaimed
//     (packs within the window are preserved);
//   - with the window advanced PAST the packs' age (via the clock), the
//     unreferenced+old packs are swept.
//
// This drives sweepTenant via a leader-elected Runner end to end.
func TestRunner_SweepsUnreferencedRespectsSafetyWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tb := newTenantBackends(t)

	const tenant = "acme"
	gw := tb.gateway(ctx, tenant)

	// Write an object, producing packs in acme's backend.
	payload := bytes.Repeat([]byte("garbage-collector payload bytes. "), 8192) // ~256 KiB
	if _, err := gw.Put(ctx, "doc.bin", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if n := tb.countPacks(tenant); n == 0 {
		t.Fatalf("expected packs written, got 0")
	}

	// Orphan the packs: delete the manifest that references them. After this the
	// packs are unreferenced and eligible for GC (subject to the safety window).
	if err := gw.Delete(ctx, "doc.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// A virtual clock pinned at the packs' write time. We advance it to cross the
	// safety window deterministically without sleeping.
	base := time.Now()
	clk := &fakeClock{now: base}

	// Build a leader-elected Runner with a TINY interval and a safety window of
	// 1h. Because the clock has not advanced, the packs are "young" → preserved.
	connect := startJetStreamNATS(t)
	leader, err := gc.NewLeader(ctx, connect("solo"), gc.LeaderConfig{
		Key: "gc", NodeID: "solo", TTL: time.Second, Renew: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeader: %v", err)
	}
	go leader.Run(ctx)
	waitFor(t, 5*time.Second, "leader elected", leader.IsLeader)

	runner, err := gc.NewRunner(leader, tb.gcFunc(clk.Now),
		func() []string { return []string{tenant} },
		gc.RunnerConfig{Interval: 50 * time.Millisecond, SafetyWindow: time.Hour})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	runCtx, runCancel := context.WithCancel(ctx)
	go runner.Run(runCtx)

	// Within-window: give the runner several intervals; packs must SURVIVE.
	time.Sleep(400 * time.Millisecond)
	if n := tb.countPacks(tenant); n == 0 {
		t.Fatalf("packs within safety window were reclaimed; want preserved")
	}

	// Advance the clock past the window → packs are now old enough to sweep.
	clk.advance(2 * time.Hour)
	waitFor(t, 5*time.Second, "old unreferenced packs to be swept", func() bool {
		return tb.countPacks(tenant) == 0
	})
	runCancel()
}

// TestRunner_FollowerDoesNotSweep asserts that a Runner whose Leader is a
// FOLLOWER (a peer holds the lease) never invokes GC.
func TestRunner_FollowerDoesNotSweep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connect := startJetStreamNATS(t)

	cfg := gc.LeaderConfig{Key: "gc", TTL: 30 * time.Second, Renew: time.Second}

	// Elect the incumbent first and let it settle so the second instance is
	// guaranteed to be the follower.
	cfgA := cfg
	cfgA.NodeID = "incumbent"
	incumbent, err := gc.NewLeader(ctx, connect("incumbent"), cfgA)
	if err != nil {
		t.Fatalf("NewLeader incumbent: %v", err)
	}
	go incumbent.Run(ctx)
	waitFor(t, 5*time.Second, "incumbent to lead", incumbent.IsLeader)

	cfgB := cfg
	cfgB.NodeID = "follower"
	followerLeader, err := gc.NewLeader(ctx, connect("follower"), cfgB)
	if err != nil {
		t.Fatalf("NewLeader follower: %v", err)
	}
	go followerLeader.Run(ctx)

	// The follower's GCFunc must never be called.
	var gcCalls atomic.Int64
	followerGC := func(ctx context.Context, tenant string) (*snapshot.ChunkedGC, error) {
		gcCalls.Add(1)
		return nil, context.Canceled // would error, but it must never be reached
	}

	runner, err := gc.NewRunner(followerLeader, followerGC,
		func() []string { return []string{"acme"} },
		gc.RunnerConfig{Interval: 50 * time.Millisecond, SafetyWindow: time.Hour})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	go runner.Run(runCtx)

	// The follower must not be leader, and over many intervals its GCFunc stays
	// untouched.
	if followerLeader.IsLeader() {
		t.Fatalf("expected follower to stand by, but it claimed leadership")
	}
	time.Sleep(500 * time.Millisecond)
	if n := gcCalls.Load(); n != 0 {
		t.Fatalf("follower ran GC %d times; expected 0 (only the leader sweeps)", n)
	}
}

// writeObject writes a ~256 KiB object to a tenant's backend (producing packs)
// and returns nothing; the caller inspects via countPacks. It does NOT delete
// the manifest, so the packs stay referenced (live) unless the caller orphans
// them.
func writeObject(ctx context.Context, t *testing.T, tb *tenantBackends, tenant, name string) {
	t.Helper()
	gw := tb.gateway(ctx, tenant)
	payload := bytes.Repeat([]byte("garbage-collector payload bytes. "), 8192) // ~256 KiB
	if _, err := gw.Put(ctx, name, "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put %s/%s: %v", tenant, name, err)
	}
	if n := tb.countPacks(tenant); n == 0 {
		t.Fatalf("expected packs written for %s, got 0", tenant)
	}
}

// runPassForDomain runs a single safety-window-enforced GC pass over one
// tenant's backend through the SAME seam the Runner uses (the gcFunc + the
// Runner's safety-window stamp), and returns the GCResult. This lets a test
// assert the reclaim counters (e.g. ChunksReclaimed==0) directly — the Runner
// itself only logs them. It mirrors Runner.sweepTenant exactly: resolve the
// per-tenant ChunkedGC, stamp the safety window, RunOnceForDomain.
func runPassForDomain(ctx context.Context, t *testing.T, gcFn gc.GCFunc, tenant string,
	safetyWindow time.Duration) snapshot.GCResult {
	t.Helper()
	g, err := gcFn(ctx, tenant)
	if err != nil {
		t.Fatalf("gcFunc %s: %v", tenant, err)
	}
	g.SafetyWindow = safetyWindow
	res, err := g.RunOnceForDomain(ctx, tenant)
	if err != nil {
		t.Fatalf("RunOnceForDomain %s: %v", tenant, err)
	}
	return res
}

// TestRunner_NeverSweepsReferencedPacks is the single most important GC
// invariant: a pack that is still referenced by a live manifest is NEVER swept,
// no matter how old it is. We write an object, do NOT delete its manifest,
// advance the clock to 2× the safety window (so age can't be the thing keeping
// it alive), run the sweep, and assert the pack count is UNCHANGED and
// ChunksReclaimed==0.
func TestRunner_NeverSweepsReferencedPacks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tb := newTenantBackends(t)

	const tenant = "acme"
	const window = time.Hour
	writeObject(ctx, t, tb, tenant, "live.bin")
	before := tb.countPacks(tenant)

	// Clock pinned at write time, then advanced PAST the window: if anything but
	// liveness were keeping the pack, this would expose it.
	clk := &fakeClock{now: time.Now()}
	gcFn := tb.gcFunc(clk.Now)
	clk.advance(2 * window)

	// Drive a real leader-elected Runner sweep end to end.
	connect := startJetStreamNATS(t)
	leader, err := gc.NewLeader(ctx, connect("solo"), gc.LeaderConfig{
		Key: "gc", NodeID: "solo", TTL: time.Second, Renew: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeader: %v", err)
	}
	go leader.Run(ctx)
	waitFor(t, 5*time.Second, "leader elected", leader.IsLeader)

	runner, err := gc.NewRunner(leader, gcFn,
		func() []string { return []string{tenant} },
		gc.RunnerConfig{Interval: 50 * time.Millisecond, SafetyWindow: window})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	runCtx, runCancel := context.WithCancel(ctx)
	go runner.Run(runCtx)

	// Give the runner several sweep intervals; the referenced pack must survive.
	time.Sleep(400 * time.Millisecond)
	runCancel()

	if got := tb.countPacks(tenant); got != before {
		t.Fatalf("referenced packs were swept: before=%d after=%d (live data must never be reclaimed)", before, got)
	}

	// Belt-and-suspenders: a direct pass through the same seam must report
	// ChunksReclaimed==0 — the canonical assertion that nothing live was touched.
	res := runPassForDomain(ctx, t, gcFn, tenant, window)
	if res.ChunksReclaimed != 0 {
		t.Fatalf("ChunksReclaimed=%d, want 0 (referenced packs must never be reclaimed)", res.ChunksReclaimed)
	}
	if got := tb.countPacks(tenant); got != before {
		t.Fatalf("direct pass swept referenced packs: before=%d after=%d", before, got)
	}
}

// TestRunner_TenantSweepIsolatedToItsOwnBackend asserts per-tenant isolation:
// sweeping tenant A leaves tenant B's packs untouched, covering BOTH of B's
// packs that are referenced AND packs that are orphaned-and-old. Only A's
// orphaned-old packs are reclaimed; B's backend is never in A's scope.
func TestRunner_TenantSweepIsolatedToItsOwnBackend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tb := newTenantBackends(t)

	const (
		tenantA = "acme"
		tenantB = "globex"
		window  = time.Hour
	)

	// Tenant A: one object we then orphan (delete its manifest) so it is eligible
	// for reclamation once old enough.
	gwA := tb.gateway(ctx, tenantA)
	payload := bytes.Repeat([]byte("garbage-collector payload bytes. "), 8192)
	if _, err := gwA.Put(ctx, "orphan.bin", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put A: %v", err)
	}
	if err := gwA.Delete(ctx, "orphan.bin"); err != nil {
		t.Fatalf("Delete A: %v", err)
	}

	// Tenant B: a referenced (live) object AND an orphaned-old object. NEITHER
	// must be touched by A's sweep — B's backend is out of A's scope entirely.
	gwB := tb.gateway(ctx, tenantB)
	if _, err := gwB.Put(ctx, "live.bin", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put B live: %v", err)
	}
	if _, err := gwB.Put(ctx, "orphan.bin", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put B orphan: %v", err)
	}
	if err := gwB.Delete(ctx, "orphan.bin"); err != nil {
		t.Fatalf("Delete B orphan: %v", err)
	}
	beforeB := tb.countPacks(tenantB)
	if beforeB == 0 {
		t.Fatalf("expected B to have packs, got 0")
	}

	// Clock advanced past the window so A's orphan is old enough to reclaim.
	clk := &fakeClock{now: time.Now()}
	gcFn := tb.gcFunc(clk.Now)
	clk.advance(2 * window)

	// Sweep ONLY tenant A through a real leader-elected Runner.
	connect := startJetStreamNATS(t)
	leader, err := gc.NewLeader(ctx, connect("solo"), gc.LeaderConfig{
		Key: "gc", NodeID: "solo", TTL: time.Second, Renew: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeader: %v", err)
	}
	go leader.Run(ctx)
	waitFor(t, 5*time.Second, "leader elected", leader.IsLeader)

	runner, err := gc.NewRunner(leader, gcFn,
		func() []string { return []string{tenantA} }, // ONLY A is in the sweep set
		gc.RunnerConfig{Interval: 50 * time.Millisecond, SafetyWindow: window})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	runCtx, runCancel := context.WithCancel(ctx)
	go runner.Run(runCtx)

	// A's orphaned-old pack should be reclaimed...
	waitFor(t, 5*time.Second, "tenant A orphaned-old packs swept", func() bool {
		return tb.countPacks(tenantA) == 0
	})
	runCancel()

	// ...while tenant B's backend (both live AND orphaned-old) is untouched: B
	// was never in A's sweep set, so isolation must hold.
	if got := tb.countPacks(tenantB); got != beforeB {
		t.Fatalf("tenant A's sweep touched tenant B: before=%d after=%d (per-tenant isolation violated)", beforeB, got)
	}
}

// TestNewRunner_SafetyWindowFloor asserts the safety-window floor (ADR §4
// I3/HC1): a positive-but-tiny window (e.g. 1s) is clamped UP to
// DefaultSafetyWindow rather than passed through, so a typo can't arm a
// data-loss config. A non-positive window is likewise defaulted, and a window
// at/above the floor is honored.
func TestNewRunner_SafetyWindowFloor(t *testing.T) {
	connect := startJetStreamNATS(t)
	ctx := context.Background()
	leader, err := gc.NewLeader(ctx, connect("solo"), gc.LeaderConfig{Key: "gc", NodeID: "solo"})
	if err != nil {
		t.Fatalf("NewLeader: %v", err)
	}
	gcFn := func(context.Context, string) (*snapshot.ChunkedGC, error) { return nil, nil }
	tenants := func() []string { return nil }

	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"tiny positive clamped up", time.Second, gc.DefaultSafetyWindow},
		{"below floor clamped up", 59 * time.Minute, gc.DefaultSafetyWindow},
		{"zero defaulted", 0, gc.DefaultSafetyWindow},
		{"negative defaulted", -time.Hour, gc.DefaultSafetyWindow},
		{"at floor honored", gc.MinSafetyWindow, gc.MinSafetyWindow},
		{"above floor honored", 6 * time.Hour, 6 * time.Hour},
		{"default honored", gc.DefaultSafetyWindow, gc.DefaultSafetyWindow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := gc.NewRunner(leader, gcFn, tenants,
				gc.RunnerConfig{SafetyWindow: tc.in})
			if err != nil {
				t.Fatalf("NewRunner: %v", err)
			}
			if got := r.SafetyWindowForTest(); got != tc.want {
				t.Fatalf("safety window: in=%s got=%s want=%s", tc.in, got, tc.want)
			}
		})
	}
}

// fakeClock is a monotonic, advanceable clock for the safety-window test.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

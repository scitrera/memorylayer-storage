// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// MountManager multiplexes many mlfs domains on one node (Design A). It keeps at
// most one mlfs mount per domain, ref-counted by the set of pod target paths
// publishing it, brings a domain's mount up lazily on the first PVC and tears it
// down on the last unpublish, and persists enough state to re-adopt live mounts
// across a node-plugin restart without unmounting in-use PVCs (§4, §7).
//
// Isolation across co-located domains is the mlfs per-process guarantee, now
// N-per-node: each domain is a separate FUSE mount, NATS account (creds file),
// dedup domain / S3-bucket-via-blobgw, and meta DSN.
type MountManager struct {
	base         string // root for per-domain dirs + state.json (a hostPath in prod)
	statePath    string
	launcher     Launcher
	mounter      Mounter
	readyTimeout time.Duration

	// lingerGrace, when > 0, keeps a domain mount alive for this long after its
	// LAST holder leaves before tearing it down, so a quick kill+recreate of the
	// same domain on this node re-adopts the still-running mount instead of paying
	// a fresh daemon start + coordination handoff (the ~30s ownership/liveness
	// lease-TTL stall). 0 (the library default) tears down immediately on the last
	// release — production sets this via the CSI flag. See
	// docs/DESIGN_mlfs_sandbox_mount_flap.md.
	lingerGrace time.Duration

	mu      sync.Mutex
	domains map[string]*domainMount // keyed by domain token
}

// domainMount is the live, in-memory record for one domain's mount. The full
// DomainSpec (with the meta DSN) lives only here, never on disk.
type domainMount struct {
	domain    string
	mountDir  string
	dataDir   string
	credsPath string
	handle    MountHandle         // backend handle (fresh or re-adopted from Ref)
	refs      map[string]struct{} // pod target paths holding this domain
	// teardown is non-nil while a deferred (idle-grace) teardown is pending — armed
	// by Release when the last holder leaves and lingerGrace > 0, cancelled by a
	// re-Acquire within the window. graceTeardown re-checks the ref count under the
	// lock before acting, so a lost Stop race is harmless.
	teardown *time.Timer
}

// persisted{State,Domain} is the on-disk shape. It deliberately omits the meta
// DSN and creds content (the creds file path is recorded; the file itself is
// 0600). Enough to (a) re-adopt a still-mounted domain and rebuild ref counts,
// and (b) signal/cleanup a domain whose process we no longer own.
type persistedState struct {
	Domains map[string]persistedDomain `json:"domains"`
}

type persistedDomain struct {
	Domain    string   `json:"domain"`
	MountDir  string   `json:"mountDir"`
	DataDir   string   `json:"dataDir"`
	CredsPath string   `json:"credsPath"`
	Ref       string   `json:"ref"` // backend handle ref (pid string | mount-pod name)
	Refs      []string `json:"refs"`
}

// NewMountManager builds a manager rooted at base. mounter is reused for mount
// readiness probing; launcher spawns mlfs.
func NewMountManager(base string, launcher Launcher, mounter Mounter) *MountManager {
	return &MountManager{
		base:      base,
		statePath: filepath.Join(base, "state.json"),
		launcher:  launcher,
		mounter:   mounter,
		// 60s (> the mlfs coordination lease/liveness TTL, default 30s) so the FIRST
		// NodePublish survives a worst-case domain handoff: a freshly-launched daemon
		// taking over from a just-killed one may not serve its mount until the
		// predecessor's stale coordination session lapses (~TTL). At 30s the timeout
		// raced the TTL and the first publish flapped; give it headroom.
		readyTimeout: 60 * time.Second,
		lingerGrace:  0, // immediate teardown by default; prod opts in via the flag
		domains:      map[string]*domainMount{},
	}
}

// SetLingerGrace configures the idle-grace window before a domain mount whose last
// holder left is torn down (0 = immediate, the default). Production wires this from
// the -mount-linger-grace flag so a quick sandbox kill+recreate re-adopts the live
// mount. Call before serving.
func (m *MountManager) SetLingerGrace(d time.Duration) {
	if d < 0 {
		d = 0
	}
	m.lingerGrace = d
}

// SetReadyTimeout overrides the mount-readiness wait (default 60s). Call before
// serving. A non-positive value is ignored.
func (m *MountManager) SetReadyTimeout(d time.Duration) {
	if d > 0 {
		m.readyTimeout = d
	}
}

// cancelTeardownLocked stops any pending deferred teardown for dm. Safe to call
// when none is armed. Caller holds m.mu.
func (m *MountManager) cancelTeardownLocked(dm *domainMount) {
	if dm.teardown != nil {
		dm.teardown.Stop()
		dm.teardown = nil
	}
}

// Acquire ensures spec's domain is mounted and records target as a holder,
// returning the domain's mount directory (the parent of csi-volumes/<id>). The
// global lock serializes mount bring-up across domains; bring-up is seconds-long
// FUSE work, so a future refinement is per-domain locking.
func (m *MountManager) Acquire(ctx context.Context, spec DomainSpec, target string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if dm := m.domains[spec.Domain]; dm != nil {
		// Self-heal: a previously-running or re-adopted domain whose mlfs has died
		// (mount gone OR stale FUSE endpoint) must not be handed back as a live
		// mount. Discard the stale entry and relaunch with THIS request's fresh
		// creds, carrying existing holders forward so ref-counting stays correct.
		mounted, err := m.mounter.IsMountedLive(dm.mountDir)
		if err != nil {
			return "", err
		}
		if mounted {
			// A quick kill+recreate can re-acquire during the idle-grace window; cancel
			// any pending deferred teardown so the live mount is reused (the fast path
			// this whole feature exists for) rather than torn down under us.
			m.cancelTeardownLocked(dm)
			dm.refs[target] = struct{}{}
			if err := m.persistLocked(); err != nil {
				return "", err
			}
			return dm.mountDir, nil
		}
		// The cached mount is dead; relaunch. Stop any pending grace timer first so it
		// can't fire against the replacement entry we install below.
		m.cancelTeardownLocked(dm)
		carried := dm.refs
		_ = m.teardownLocked(context.Background(), dm)
		delete(m.domains, spec.Domain)
		slog.Warn("mlfs-csi: relaunching dead domain mount", "domain", spec.Domain, "carried_refs", len(carried))
		dm2, err := m.startLocked(ctx, spec)
		if err != nil {
			return "", err
		}
		for t := range carried {
			dm2.refs[t] = struct{}{}
		}
		dm2.refs[target] = struct{}{}
		m.domains[spec.Domain] = dm2
		if err := m.persistLocked(); err != nil {
			_ = m.teardownLocked(context.Background(), dm2)
			delete(m.domains, spec.Domain)
			return "", err
		}
		return dm2.mountDir, nil
	}

	dm, err := m.startLocked(ctx, spec)
	if err != nil {
		return "", err
	}
	dm.refs[target] = struct{}{}
	m.domains[spec.Domain] = dm
	if err := m.persistLocked(); err != nil {
		// Best-effort rollback so a persist failure does not leak a daemon.
		_ = m.teardownLocked(context.Background(), dm)
		delete(m.domains, spec.Domain)
		return "", err
	}
	return dm.mountDir, nil
}

// startLocked brings a new domain mount up: lay out dirs, write the creds file
// 0600, launch mlfs, and wait for the mount to appear.
func (m *MountManager) startLocked(ctx context.Context, spec DomainSpec) (*domainMount, error) {
	dir := filepath.Join(m.base, domainDirKey(spec.Domain))
	mountDir := filepath.Join(dir, "mnt")
	dataDir := filepath.Join(dir, "data")
	credsPath := filepath.Join(dir, "nats.creds")

	if err := os.MkdirAll(mountDir, 0o755); err != nil {
		return nil, fmt.Errorf("create domain mount dir: %w", err)
	}
	if err := os.WriteFile(credsPath, []byte(spec.Creds), 0o600); err != nil {
		return nil, fmt.Errorf("write nats creds: %w", err)
	}

	handle, err := m.launcher.Start(ctx, spec, mountDir, dataDir, credsPath)
	if err != nil {
		_ = os.Remove(credsPath)
		return nil, err
	}
	dm := &domainMount{
		domain:    spec.Domain,
		mountDir:  mountDir,
		dataDir:   dataDir,
		credsPath: credsPath,
		handle:    handle,
		refs:      map[string]struct{}{},
	}
	if err := m.waitReady(ctx, mountDir); err != nil {
		_ = m.teardownLocked(context.Background(), dm)
		return nil, fmt.Errorf("domain %q mount not ready: %w", spec.Domain, err)
	}
	slog.Info("mlfs-csi: domain mount up", "domain", spec.Domain, "mount", mountDir, "ref", handle.Ref())
	return dm, nil
}

// Release drops target as a holder of whatever domain owns it. When the last
// holder leaves, the domain's mlfs process is stopped (clean unmount) and its
// dirs removed. Returns false when no managed domain owns target (a legacy
// single-shared-mount publish the caller handled itself).
func (m *MountManager) Release(ctx context.Context, target string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	dm := m.ownerLocked(target)
	if dm == nil {
		return false, nil
	}
	delete(dm.refs, target)
	if len(dm.refs) > 0 {
		return true, m.persistLocked()
	}
	// Last holder gone. With an idle-grace window configured, keep the mount LIVE
	// (now zero-ref) and arm a deferred teardown, so a quick kill+recreate of the
	// same domain re-adopts the running mount instead of paying a fresh daemon start
	// + coordination handoff (the ~30s lease-TTL stall this fixes). A re-Acquire
	// within the window cancels the timer; graceTeardown re-checks the ref count.
	if m.lingerGrace > 0 {
		m.cancelTeardownLocked(dm) // defensive: no timer should be armed while refs>0
		dm.teardown = time.AfterFunc(m.lingerGrace, func() { m.graceTeardown(dm.domain) })
		slog.Info("mlfs-csi: domain mount idle; deferring teardown", "domain", dm.domain, "grace", m.lingerGrace)
		return true, m.persistLocked()
	}
	// No grace: tear the domain mount down now.
	if err := m.teardownLocked(ctx, dm); err != nil {
		// Keep the entry so a retried unpublish can try again rather than
		// orphaning a daemon silently.
		dm.refs[target] = struct{}{}
		return true, fmt.Errorf("teardown domain %q: %w", dm.domain, err)
	}
	delete(m.domains, dm.domain)
	slog.Info("mlfs-csi: domain mount down", "domain", dm.domain)
	return true, m.persistLocked()
}

// graceTeardown fires after the idle-grace window (armed by Release when
// lingerGrace > 0). It tears the domain mount down only if it is STILL idle: a
// kill+recreate that re-acquired the domain during the window added a ref (and
// cancelled this timer), making this a no-op. It runs in the AfterFunc goroutine,
// so it re-takes the manager lock and re-validates the ref count under it — a lost
// timer.Stop race (timer already fired when Acquire cancels) is therefore harmless.
func (m *MountManager) graceTeardown(domain string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	dm := m.domains[domain]
	if dm == nil || len(dm.refs) > 0 {
		return // already torn down/replaced, or re-acquired during the grace window
	}
	dm.teardown = nil
	if err := m.teardownLocked(context.Background(), dm); err != nil {
		// A still-detaching FUSE endpoint (or similar) blocked teardown; keep the
		// entry and retry after another window rather than orphaning the daemon.
		slog.Warn("mlfs-csi: deferred teardown failed; will retry", "domain", domain, "err", err)
		dm.teardown = time.AfterFunc(m.lingerGrace, func() { m.graceTeardown(domain) })
		return
	}
	delete(m.domains, domain)
	slog.Info("mlfs-csi: domain mount down (after idle grace)", "domain", domain)
	if err := m.persistLocked(); err != nil {
		slog.Warn("mlfs-csi: persist after deferred teardown failed", "domain", domain, "err", err)
	}
}

func (m *MountManager) ownerLocked(target string) *domainMount {
	for _, dm := range m.domains {
		if _, ok := dm.refs[target]; ok {
			return dm
		}
	}
	return nil
}

// teardownLocked stops the domain's mlfs process (ours via handle, or re-adopted
// via pid) so it unmounts cleanly, then removes the per-domain directory tree.
func (m *MountManager) teardownLocked(ctx context.Context, dm *domainMount) error {
	if dm.handle != nil {
		if err := dm.handle.Stop(ctx); err != nil {
			return err
		}
	}
	// A cleanly-stopped mlfs unmounts itself, but a dead/stale daemon (e.g.
	// cgroup-killed) does NOT — its FUSE endpoint lingers in the mount table.
	// Unmount it explicitly (lazy MNT_DETACH) so neither the endpoint nor the dir
	// leaks on the node.
	if mounted, _ := m.mounter.IsMounted(dm.mountDir); mounted {
		if err := m.mounter.Unmount(dm.mountDir); err != nil {
			slog.Warn("mlfs-csi: domain unmount failed", "domain", dm.domain, "err", err)
		}
	}
	// Remove the NATS account creds FIRST and unconditionally: it is the only
	// secret on disk, so it must never be orphaned even if the dir-tree RemoveAll
	// below fails (e.g. a still-detaching lazy FUSE unmount).
	if dm.credsPath != "" {
		if err := os.Remove(dm.credsPath); err != nil && !os.IsNotExist(err) {
			slog.Warn("mlfs-csi: domain creds cleanup failed", "domain", dm.domain, "err", err)
		}
	}
	// Process exit unmounts FUSE; now reclaim the dir tree (best-effort: a
	// lingering mount makes RemoveAll fail — we log and continue so state stays
	// consistent; the creds are already gone above).
	if err := os.RemoveAll(filepath.Dir(dm.mountDir)); err != nil {
		slog.Warn("mlfs-csi: domain dir cleanup failed", "domain", dm.domain, "err", err)
	}
	return nil
}

// Adopt re-reads persisted state at startup and re-attaches to domains whose
// mounts are still live (their mlfs processes survived this plugin's restart via
// hostPID + setsid). Dead/absent mounts are pruned and their dirs cleaned.
// Idempotent; safe to call once before serving.
func (m *MountManager) Adopt(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	st, err := m.loadStateLocked()
	if err != nil {
		return err
	}
	for domain, pd := range st.Domains {
		// Liveness, not mere mountinfo presence: a daemon killed across the restart
		// (e.g. cgroup-reaped) leaves a stale FUSE endpoint that still appears in
		// mountinfo. Re-adopting that corpse would hand every consumer pod a dead
		// mount (ENOTCONN); IsMountedLive statfs-probes it so we prune + relaunch.
		mounted, err := m.mounter.IsMountedLive(pd.MountDir)
		if err != nil {
			return fmt.Errorf("adopt %q: probe mount: %w", domain, err)
		}
		if !mounted {
			// Dead/absent: the mount did not survive. Stop the backend resource
			// (delete a lingering mount pod / no-op a dead pid), unmount a stale
			// FUSE endpoint a cgroup-killed daemon left behind, then reclaim its
			// scratch + creds so nothing leaks on the node.
			_ = m.launcher.Adopt(pd.Ref, domain).Stop(ctx)
			if pd.MountDir != "" {
				if present, _ := m.mounter.IsMounted(pd.MountDir); present {
					_ = m.mounter.Unmount(pd.MountDir)
				}
				if pd.CredsPath != "" {
					_ = os.Remove(pd.CredsPath)
				}
				_ = os.RemoveAll(filepath.Dir(pd.MountDir))
			}
			slog.Info("mlfs-csi: dropped stale domain on adopt", "domain", domain)
			continue
		}
		refs := make(map[string]struct{}, len(pd.Refs))
		for _, t := range pd.Refs {
			refs[t] = struct{}{}
		}
		m.domains[domain] = &domainMount{
			domain:    domain,
			mountDir:  pd.MountDir,
			dataDir:   pd.DataDir,
			credsPath: pd.CredsPath,
			handle:    m.launcher.Adopt(pd.Ref, domain), // reconstruct from persisted ref
			refs:      refs,
		}
		slog.Info("mlfs-csi: re-adopted live domain", "domain", domain, "mount", pd.MountDir, "refs", len(refs))
	}
	// A domain re-adopted with zero holders was idling in its teardown-grace window
	// when the plugin restarted (only possible once lingerGrace is enabled, which
	// persists zero-ref live domains). Resume that countdown so it does not linger
	// forever; with no grace configured, tear it down now.
	for domain, dm := range m.domains {
		if len(dm.refs) > 0 {
			continue
		}
		if m.lingerGrace <= 0 {
			if err := m.teardownLocked(ctx, dm); err == nil {
				delete(m.domains, domain)
			}
			continue
		}
		dm.teardown = time.AfterFunc(m.lingerGrace, func() { m.graceTeardown(domain) })
	}
	return m.persistLocked()
}

func (m *MountManager) waitReady(ctx context.Context, mountDir string) error {
	deadline := time.NewTimer(m.readyTimeout)
	defer deadline.Stop()
	tick := time.NewTicker(150 * time.Millisecond)
	defer tick.Stop()
	for {
		// Wait for an actually-serving mount, not just mountinfo presence.
		mounted, err := m.mounter.IsMountedLive(mountDir)
		if err != nil {
			return err
		}
		if mounted {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out after %s", m.readyTimeout)
		case <-tick.C:
		}
	}
}

func (m *MountManager) persistLocked() error {
	st := persistedState{Domains: make(map[string]persistedDomain, len(m.domains))}
	for domain, dm := range m.domains {
		var ref string
		if dm.handle != nil {
			ref = dm.handle.Ref()
		}
		refs := make([]string, 0, len(dm.refs))
		for t := range dm.refs {
			refs = append(refs, t)
		}
		st.Domains[domain] = persistedDomain{
			Domain:    domain,
			MountDir:  dm.mountDir,
			DataDir:   dm.dataDir,
			CredsPath: dm.credsPath,
			Ref:       ref,
			Refs:      refs,
		}
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(m.base, 0o700); err != nil {
		return err
	}
	// Atomic replace so a crash mid-write never leaves truncated state.
	tmp := m.statePath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, m.statePath)
}

func (m *MountManager) loadStateLocked() (persistedState, error) {
	var st persistedState
	b, err := os.ReadFile(m.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return persistedState{Domains: map[string]persistedDomain{}}, nil
		}
		return st, err
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return st, fmt.Errorf("parse %s: %w", m.statePath, err)
	}
	if st.Domains == nil {
		st.Domains = map[string]persistedDomain{}
	}
	return st, nil
}

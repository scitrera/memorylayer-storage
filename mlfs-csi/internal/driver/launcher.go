// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package driver

import "context"

// Launcher brings a single domain's mlfs FUSE mount up on a node and is the
// backend-abstraction point: production can run mlfs as a Kubernetes "mount pod"
// (k8sPodLauncher — survives a CSI-container cgroup teardown, §7.3) or, for
// tests / non-k8s, as a supervised child process (execLauncher). The
// MountManager is identical over either backend; it only deals in opaque Refs.
type Launcher interface {
	// Start brings up mlfs for spec, mounting at mountDir (which already exists).
	// credsPath is a file the manager has written with spec.Creds (0600);
	// dataDir is per-domain scratch (cache/wal/casstore staging). Start returns
	// once the mount is requested; readiness (the mount appearing) is polled by
	// the manager via Mounter.IsMountedLive.
	Start(ctx context.Context, spec DomainSpec, mountDir, dataDir, credsPath string) (MountHandle, error)
	// Adopt reconstructs a handle for a mount started by a prior incarnation of
	// the plugin, from its persisted Ref (so a restarted plugin can Stop it). The
	// domain is supplied for identity checks (e.g. pid-reuse / pod-label match).
	Adopt(ref, domain string) MountHandle
}

// MountHandle is a live (or re-adopted) per-domain mlfs mount.
type MountHandle interface {
	// Ref is an opaque, backend-specific identifier persisted to state.json so a
	// restarted plugin can re-adopt and tear the mount down — a pid for the exec
	// backend, a mount-pod name for the k8s backend.
	Ref() string
	// Stop terminates the mount (SIGTERM the process / delete the mount pod) so
	// mlfs unmounts cleanly, bounded by ctx.
	Stop(ctx context.Context) error
}

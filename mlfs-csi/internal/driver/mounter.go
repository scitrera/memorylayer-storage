// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package driver

import "path/filepath"

// Mounter abstracts the bind-mount/unmount of a volume subdirectory onto a pod
// target path. Abstracted so the CSI sanity suite can run without CAP_SYS_ADMIN
// (a fake mounter); production uses NewLinuxMounter.
type Mounter interface {
	// BindMount bind-mounts source onto target (read-only if ro).
	BindMount(source, target string, ro bool) error
	// Unmount detaches the mount at target.
	Unmount(target string) error
	// IsMounted reports whether target is currently a mount point (mountinfo
	// presence only). Use this for "is there a mount object to unmount?".
	IsMounted(target string) (bool, error)
	// IsMountedLive reports whether target is a mount point AND its filesystem is
	// actually serving — i.e. a FUSE mount whose daemon died (stale, ENOTCONN)
	// reports false. Use this for liveness decisions (adopt / self-heal /
	// readiness): a dead FUSE mount still appears in mountinfo, so IsMounted
	// alone would falsely treat a corpse as live.
	IsMountedLive(target string) (bool, error)
}

func joinClean(parts ...string) string { return filepath.Join(parts...) }

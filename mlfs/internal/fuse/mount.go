// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge

import "github.com/hanwen/go-fuse/v2/fuse"

// Mount starts serving the bridge at mountpoint and returns the running
// server. The caller unmounts via server.Unmount(). opts may be nil.
//
// allowOther appends the FUSE allow_other option so processes other than the
// mounting user (uid 0, the daemon) can access the mount. It is REQUIRED whenever
// non-root workloads use the filesystem — e.g. the CSI subdir-bind-mount model,
// where pods run as arbitrary uids — because without it the FUSE kernel denies
// every non-mounter uid with EACCES before default_permissions even evaluates.
func Mount(b *Bridge, mountpoint string, opts *fuse.MountOptions, allowOther bool) (*fuse.Server, error) {
	if opts == nil {
		opts = &fuse.MountOptions{Name: "mlfs", FsName: "mlfs"}
	}
	if allowOther {
		opts.AllowOther = true
	}
	// Use 1 MiB requests instead of the 128 KiB default: go-fuse ties max_read to
	// MaxWrite, so a 1 MiB sequential read/write becomes ONE FUSE op (one slice-list
	// query, one slice) instead of eight — cutting per-op metadata round-trips on
	// sequential I/O, and producing 1 MiB slices on sequential writes (fewer
	// NewSlice/meta.Write/upload ops per MiB). MaxReadAhead widens kernel prefetch.
	// Callers may override by presetting either field.
	if opts.MaxWrite == 0 {
		opts.MaxWrite = 1 << 20
	}
	if opts.MaxReadAhead == 0 {
		opts.MaxReadAhead = 1 << 20
	}
	// Let the kernel enforce POSIX permission checks (traverse/read/write/
	// create/delete/sticky) from the uid/gid/mode we report on each inode,
	// rather than re-implementing the VFS in the bridge. The engine additionally
	// enforces chmod/chown ownership rules for defense + non-FUSE callers.
	opts.Options = append(opts.Options, "default_permissions")
	// Forward flock(2) and POSIX fcntl byte-range locks to the bridge (→ the
	// metadata engine) instead of letting the kernel handle them mount-locally,
	// so locks coordinate across mounts/nodes via the shared metadata DB.
	opts.EnableLocks = true
	// Advertise CAP_POSIX_ACL so the kernel enforces POSIX ACLs (L2.8.7): it
	// reads/writes system.posix_acl_access / system.posix_acl_default via our
	// generic xattr store and applies them in permission checks (and handles
	// default-ACL inheritance + the mode/mask coupling). This stays consistent
	// with delegating permission checks to the kernel (default_permissions),
	// rather than re-implementing ACL evaluation in the bridge.
	opts.EnableAcl = true
	srv, err := fuse.NewServer(b, mountpoint, opts)
	if err != nil {
		return nil, err
	}
	go srv.Serve()
	if err := srv.WaitMount(); err != nil {
		_ = srv.Unmount()
		return nil, err
	}
	return srv, nil
}

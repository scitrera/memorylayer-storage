// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

//go:build linux

package driver

import (
	"bufio"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

// linuxMounter performs real bind mounts via the mount(2) syscall.
type linuxMounter struct{}

// NewLinuxMounter returns the production mounter (requires CAP_SYS_ADMIN).
func NewLinuxMounter() Mounter { return linuxMounter{} }

func (linuxMounter) BindMount(source, target string, ro bool) error {
	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		return err
	}
	if ro {
		// A read-only bind needs a second remount; the initial MS_BIND ignores
		// MS_RDONLY.
		return unix.Mount(source, target, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, "")
	}
	return nil
}

func (linuxMounter) Unmount(target string) error {
	// MNT_DETACH (lazy) so a busy mount still detaches and is cleaned up once the
	// last user goes away — the pod teardown path.
	return unix.Unmount(target, unix.MNT_DETACH)
}

func (linuxMounter) IsMounted(target string) (bool, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		// mountinfo columns: id pid major:minor root MOUNTPOINT ...
		fields := strings.Fields(sc.Text())
		if len(fields) > 4 && fields[4] == target {
			return true, nil
		}
	}
	return false, sc.Err()
}

// IsMountedLive reports whether target is a mount point whose filesystem is
// actually serving. A FUSE mount whose daemon was killed (e.g. a per-domain
// mlfs reaped when the CSI container's cgroup scope was torn down) lingers in
// mountinfo but a statfs on it returns ENOTCONN — that is a corpse, not a live
// mount, and must NOT be re-adopted. ESTALE is treated the same way.
func (m linuxMounter) IsMountedLive(target string) (bool, error) {
	mounted, err := m.IsMounted(target)
	if err != nil || !mounted {
		return false, err
	}
	var st unix.Statfs_t
	if err := unix.Statfs(target, &st); err != nil {
		if err == unix.ENOTCONN || err == unix.ESTALE {
			return false, nil // mount object present but daemon gone
		}
		return false, err
	}
	return true, nil
}

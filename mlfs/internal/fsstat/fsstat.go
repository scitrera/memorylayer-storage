// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package fsstat holds small, dependency-free helpers for reporting filesystem
// usage: human-readable byte sizes, on-disk directory accounting, and statfs
// capacity. They are shared by the operator inspector (mlfs-admin stats) and the
// standalone probe (mlfs-bench), neither of which should pull in the metadata or
// chunk-store packages just to print sizes.
package fsstat

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// HumanBytes formats a byte count with binary (IEC) units (KiB, MiB, …).
func HumanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// DirUsage returns (file count, total bytes) of the regular files under root.
// A missing or unreadable root yields (0,0); unreadable entries are skipped, so
// accounting is best-effort and never fails — callers want a number, not an
// error, when pointing at a directory that may not exist on this node.
func DirUsage(root string) (files, bytes int64) {
	if root == "" {
		return 0, 0
	}
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		files++
		bytes += info.Size()
		return nil
	})
	return files, bytes
}

// OldestModTime returns the oldest modification time among the regular files
// directly under root (NOT recursive — the staging dir is flat, one file per
// staged slice), and ok=false when the directory is empty or unreadable. The
// write-back oldest-unflushed-age gauge reads it: a staging file's mtime is when
// the (durable, fsync'd) write was staged, so the oldest mtime bounds how long
// the oldest still-unflushed write has waited. Tmp files (dotfiles) are skipped.
func OldestModTime(root string) (oldest time.Time, ok bool) {
	if root == "" {
		return time.Time{}, false
	}
	ents, err := os.ReadDir(root)
	if err != nil {
		return time.Time{}, false
	}
	for _, d := range ents {
		name := d.Name()
		if d.IsDir() || len(name) == 0 || name[0] == '.' { // skip .tmp-* and subdirs
			continue
		}
		info, ierr := d.Info()
		if ierr != nil {
			continue
		}
		if m := info.ModTime(); !ok || m.Before(oldest) {
			oldest, ok = m, true
		}
	}
	return oldest, ok
}

// StatfsBytes returns (available, total) bytes for the filesystem holding path,
// or (0,0) if it cannot be stat'd.
func StatfsBytes(path string) (avail, total int64) {
	if path == "" {
		return 0, 0
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0
	}
	bs := int64(st.Bsize)
	return int64(st.Bavail) * bs, int64(st.Blocks) * bs
}

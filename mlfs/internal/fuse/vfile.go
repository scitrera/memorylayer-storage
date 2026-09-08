// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge

// Virtual metrics file. A hidden directory `.mlfs` at the mount root exposes a
// single read-only file, `.mlfs/metrics`, whose contents are the daemon's live
// counters in Prometheus text format. Serving it through the FUSE mount (rather
// than a node-side socket or port) is deliberate: it inherits the mount's exact
// access model, so a workload pod that mounts mlfs can poll its own live feed
// with no extra credentials or network path — the same reason the standalone
// probe targets only the mountpoint.
//
// The magic inodes sit at the very top of the inode space, which the allocator
// (a 48-bit per-session counter) cannot reach in practice, so they never collide
// with a real inode. `.mlfs` is resolvable by path but intentionally absent from
// the root directory listing, keeping `ls /mnt` clean.

import (
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/metrics"
)

const (
	magicDirIno     = meta.Ino(0x7FFFFFFFFFFFFF00) // .mlfs
	magicMetricsIno = meta.Ino(0x7FFFFFFFFFFFFF01) // .mlfs/metrics
	magicDirName    = ".mlfs"
	magicMetricsTxt = "metrics"
)

// SetMetrics attaches the daemon's instrument registry; the virtual file renders
// from it. nil disables nothing structurally — the file still resolves and reads
// empty — but in practice the daemon always sets it.
func (b *Bridge) SetMetrics(m *metrics.Registry) { b.metrics = m }

func isMagicIno(ino meta.Ino) bool { return ino == magicDirIno || ino == magicMetricsIno }

// magicAttr synthesizes the attributes of a magic node (read-only; size 0, since
// the file is served with direct I/O and the kernel ignores the cached size).
func magicAttr(ino meta.Ino) *meta.Attr {
	if ino == magicDirIno {
		return &meta.Attr{Typ: meta.TypeDirectory, Mode: 0o555, Nlink: 2}
	}
	return &meta.Attr{Typ: meta.TypeFile, Mode: 0o444, Nlink: 1}
}

// magicLookup resolves the `.mlfs` hidden dir and its `metrics` child. It returns
// handled=false for any name it does not own, so the caller falls through to the
// normal metadata lookup.
func (b *Bridge) magicLookup(parent meta.Ino, name string, out *fuse.EntryOut) (handled bool, st fuse.Status) {
	switch {
	case parent == meta.RootInode && name == magicDirName:
		b.fillEntry(magicDirIno, magicAttr(magicDirIno), out)
		return true, fuse.OK
	case parent == magicDirIno:
		if name == magicMetricsTxt {
			b.fillEntry(magicMetricsIno, magicAttr(magicMetricsIno), out)
			return true, fuse.OK
		}
		return true, fuse.ENOENT
	}
	return false, fuse.OK
}

// magicGetAttr fills attributes for a magic inode. handled=false otherwise.
func (b *Bridge) magicGetAttr(ino meta.Ino, out *fuse.AttrOut) bool {
	if !isMagicIno(ino) {
		return false
	}
	out.SetTimeout(0) // never cache: the metrics file is live
	fillAttr(ino, magicAttr(ino), &out.Attr)
	return true
}

// magicOpen renders the metrics snapshot once at open and parks it under a fresh
// Fh so paged reads are consistent. Direct I/O so the kernel relays our byte
// counts and signals EOF on a short read, regardless of the (zero) cached size.
func (b *Bridge) magicOpen(ino meta.Ino, out *fuse.OpenOut) (handled bool, st fuse.Status) {
	if !isMagicIno(ino) {
		return false, fuse.OK
	}
	if ino != magicMetricsIno {
		return true, fuse.EISDIR
	}
	body, err := b.metrics.RenderPrometheus()
	if err != nil {
		return true, fuse.EIO
	}
	b.vmu.Lock()
	fh := b.vNext
	b.vNext++
	b.vfiles[fh] = body
	b.vmu.Unlock()
	out.Fh = fh
	out.OpenFlags |= fuse.FOPEN_DIRECT_IO
	return true, fuse.OK
}

// magicRead serves bytes from the snapshot parked at open. handled=false when the
// inode is not a magic file, so the caller falls through to the data path.
func (b *Bridge) magicRead(in *fuse.ReadIn, buf []byte) (handled bool, res fuse.ReadResult, st fuse.Status) {
	if !isMagicIno(meta.Ino(in.NodeId)) {
		return false, nil, fuse.OK
	}
	b.vmu.Lock()
	body := b.vfiles[in.Fh]
	b.vmu.Unlock()
	off := int(in.Offset)
	if off >= len(body) {
		return true, fuse.ReadResultData(nil), fuse.OK
	}
	end := off + int(in.Size)
	if end > len(body) {
		end = len(body)
	}
	n := copy(buf, body[off:end])
	return true, fuse.ReadResultData(buf[:n]), fuse.OK
}

// magicReleaseFh frees a parked snapshot. handled=false when the inode is not a
// magic file (the normal Release path closes the metadata inode instead).
func (b *Bridge) magicReleaseFh(ino meta.Ino, fh uint64) bool {
	if !isMagicIno(ino) {
		return false
	}
	b.vmu.Lock()
	delete(b.vfiles, fh)
	b.vmu.Unlock()
	return true
}

// magicOpenDir snapshots the `.mlfs` directory's single entry so `ls /mnt/.mlfs`
// works; it reuses the bridge's dir-handle table, which ReadDir/ReadDirPlus page.
func (b *Bridge) magicOpenDir(ino meta.Ino, out *fuse.OpenOut) bool {
	if ino != magicDirIno {
		return false
	}
	entries := []*meta.Entry{{Inode: magicMetricsIno, Name: magicMetricsTxt, Attr: magicAttr(magicMetricsIno)}}
	b.dmu.Lock()
	fh := b.dirNext
	b.dirNext++
	b.dirs[fh] = entries
	b.dmu.Unlock()
	out.Fh = fh
	return true
}

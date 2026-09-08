// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package fusebridge maps FUSE operations onto the metadata engine and file
// I/O layer. It handles filesystem requests, cache invalidation and errors.
package fusebridge

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/acl"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/fileio"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/metrics"
)

const (
	xattrACLAccess  = "system.posix_acl_access"
	xattrACLDefault = "system.posix_acl_default"
)

// Flusher lets the bridge push buffered writes to durable backing on
// flush/fsync (the disk cache implements it). Optional.
type Flusher interface {
	Flush(ctx context.Context) error
}

// Bridge implements fuse.RawFileSystem over a metadata engine + data path.
type Bridge struct {
	fuse.RawFileSystem // ENOSYS defaults for ops we don't implement
	meta               *meta.Engine
	files              *fileio.Files
	flusher            Flusher
	entryTTL           time.Duration

	// tracer opens the parent FUSE op spans (mlfs.fuse.<op>) so the casstore
	// backing leaf spans + meta-DB ops nest under one request span and the latency
	// histograms recorded within the op ctx attach exemplars. It is the GLOBAL OTEL
	// tracer, so it is a cheap no-op until main installs a real TracerProvider
	// (only when an OTLP endpoint is configured); no nil-branching at the call site.
	tracer trace.Tracer

	// Blocking-lock wait/notify. SetLkw parks here when the engine acquire
	// returns EAGAIN and is woken on every lock release, then re-attempts.
	// Notification uses a "closed channel broadcast": lockNotify is a channel
	// that wakeLockWaiters closes (and replaces) on every release, so any number
	// of parked SetLkw goroutines can select on it AND on the FUSE cancel
	// channel — a killed/interrupted syscall then returns EINTR instead of
	// hanging the request forever (sync.Cond.Wait cannot be cancelled, hence the
	// channel). A single broadcast on ANY release with a re-check under the loop
	// is correct: spurious wakeups only cost a re-attempt, so no per-inode waiter
	// bookkeeping is needed. In-memory is sufficient because a single mount
	// serves all FUSE requests for these locks. The lock is held only to swap
	// the channel pointer, never across the engine call, so the broadcaster
	// never blocks behind a slow acquire.
	lockMu     sync.Mutex
	lockNotify chan struct{}

	// Directory-read handles. OpenDir snapshots a directory's ordered entry
	// list and parks it here under a freshly minted Fh; ReadDirPlus pages
	// through the snapshot by offset (stable across concurrent create/delete);
	// ReleaseDir frees it. This makes readdir offsets stable, so a large scan
	// racing with mutations never skips or duplicates entries that existed at
	// open time.
	dmu     sync.Mutex
	dirNext uint64
	dirs    map[uint64][]*meta.Entry

	// server is captured at mount (Init) so the change feed (L2.8.5) can push
	// kernel cache invalidations for peer writes. atomic.Pointer because Init
	// runs on the mount goroutine while ApplyChange runs on a NATS goroutine.
	server atomic.Pointer[fuse.Server]

	// metrics is the daemon instrument registry; the .mlfs/metrics virtual file
	// (vfile.go) renders it and the hot paths record into it. May be nil (all
	// recording/render calls are nil-safe). vfiles parks per-open snapshots of
	// the virtual file under a fresh Fh so paged reads stay consistent.
	metrics *metrics.Registry
	vmu     sync.Mutex
	vNext   uint64
	vfiles  map[uint64][]byte

	// defaultStoreClass is the compression class applied to a created file whose
	// type is not recognized as a model/tensor (a model-store mount sets this to
	// ClassUncompressed so unrecognized files still stay uncompressed; a
	// mixed-content mount leaves it ClassDefault so generic data compresses).
	defaultStoreClass uint8
	// classCache memoizes inode → store class so the hot write path avoids a
	// per-write metadata read. The class is immutable (set once at create), so the
	// cache never needs coherence invalidation; entries are evicted on Release to
	// bound memory, and a reopened file re-populates lazily from the inode.
	classCache sync.Map // map[uint64]uint8

	// hashStates holds the per-open incremental whole-file sha256 accumulators
	// (contenthash.go): a running digest + a hashed_len watermark maintained on
	// the write path so the common sequential/append workload finalizes its
	// whole-file content hash on flush/close for free, falling back to a full
	// recompute only when a behind-watermark (random/backfill) write invalidated
	// it. Keyed by inode; evicted on Release. Nil-safe: an entry is created lazily
	// on the first Write to an inode.
	hashStates sync.Map // map[uint64]*hashState
	// hashDisabled gates the whole feature off the hot path (default false = on).
	hashDisabled bool
}

// Init captures the server handle (go-fuse calls it once at mount).
func (b *Bridge) Init(srv *fuse.Server) {
	b.server.Store(srv)
}

// inheritACL applies a parent directory's default ACL to a child being created
// (L2.8.7). When the parent has a default ACL, POSIX ignores the umask and the
// ACL determines the child's mode + access ACL (and a child directory inherits
// the same default ACL). has=false means no default ACL — the caller keeps its
// normal umask-masked mode and stamps nothing, so the no-ACL path is unchanged.
func (b *Bridge) inheritACL(ctx context.Context, parent meta.Ino, reqMode uint16, isDir bool) (effMode uint16, accessBlob, defaultBlob []byte, has bool) {
	pdef, st := b.meta.GetXAttr(ctx, parent, xattrACLDefault)
	if st != 0 || len(pdef) == 0 {
		return 0, nil, nil, false
	}
	def, ok := acl.Unmarshal(pdef)
	if !ok {
		return 0, nil, nil, false
	}
	access, newMode := acl.Create(def, reqMode)
	if access != nil {
		accessBlob = access.Marshal()
	}
	if isDir {
		defaultBlob = pdef // a child directory inherits the same default ACL
	}
	return newMode, accessBlob, defaultBlob, true
}

// stampACL writes the inherited ACL xattrs onto a freshly created child.
func (b *Bridge) stampACL(ctx context.Context, ino meta.Ino, accessBlob, defaultBlob []byte) {
	if accessBlob != nil {
		_ = b.meta.SetXAttr(ctx, ino, xattrACLAccess, accessBlob, 0)
	}
	if defaultBlob != nil {
		_ = b.meta.SetXAttr(ctx, ino, xattrACLDefault, defaultBlob, 0)
	}
}

// changePayload is the union of fs_changelog payload fields the feed cares about
// for cache invalidation (unused fields stay zero).
type changePayload struct {
	Parent    uint64 `json:"parent"`
	Name      string `json:"name"`
	SrcParent uint64 `json:"src_parent"`
	SrcName   string `json:"src_name"`
	DstParent uint64 `json:"dst_parent"`
	DstName   string `json:"dst_name"`
}

// ApplyChange invalidates the kernel caches affected by a peer's change so the
// next access on this mount re-reads fresh state from the shared metadata DB. It
// is best-effort: a not-cached target simply returns ENOENT, which we ignore.
// ino is the changed inode, op is a meta.Change* code, payload is the raw
// fs_changelog payload.
func (b *Bridge) ApplyChange(ino uint64, op uint8, payload []byte) {
	srv := b.server.Load()
	if srv == nil {
		return
	}
	var p changePayload
	if len(payload) > 0 {
		_ = json.Unmarshal(payload, &p)
	}
	switch op {
	case meta.ChangeWrite, meta.ChangeSetattr:
		// Data/attributes/size changed: drop cached page data + attrs.
		_ = srv.InodeNotify(ino, 0, -1)
	case meta.ChangeMknod, meta.ChangeMkdir, meta.ChangeSymlink, meta.ChangeLink:
		// New child: invalidate the parent dentry slot + the parent's attrs.
		if p.Parent != 0 {
			_ = srv.EntryNotify(p.Parent, p.Name)
			_ = srv.InodeNotify(p.Parent, 0, -1)
		}
	case meta.ChangeUnlink, meta.ChangeRmdir:
		// Removed child: drop the stale dentry + the removed inode + parent attrs.
		if p.Parent != 0 {
			_ = srv.EntryNotify(p.Parent, p.Name)
			_ = srv.InodeNotify(p.Parent, 0, -1)
		}
		_ = srv.InodeNotify(ino, 0, -1)
	case meta.ChangeRename:
		if p.SrcParent != 0 {
			_ = srv.EntryNotify(p.SrcParent, p.SrcName)
			_ = srv.InodeNotify(p.SrcParent, 0, -1)
		}
		if p.DstParent != 0 {
			_ = srv.EntryNotify(p.DstParent, p.DstName)
			_ = srv.InodeNotify(p.DstParent, 0, -1)
		}
	}
}

// New constructs a Bridge. flusher may be nil.
func New(m *meta.Engine, files *fileio.Files, flusher Flusher) *Bridge {
	b := &Bridge{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		meta:          m,
		files:         files,
		flusher:       flusher,
		entryTTL:      time.Second,
		tracer:        otel.Tracer("github.com/scitrera/memorylayer-storage/mlfs/internal/fuse"),
		dirNext:       1, // 0 is reserved as "no handle"
		dirs:          make(map[uint64][]*meta.Entry),
		vNext:         1,
		vfiles:        make(map[uint64][]byte),
	}
	b.lockNotify = make(chan struct{})
	return b
}

// lockWaitCh returns the current notification channel; a SETLKW waiter grabs it
// BEFORE re-attempting the acquire so it cannot miss a release that races
// between the failed attempt and parking on the channel.
func (b *Bridge) lockWaitCh() <-chan struct{} {
	b.lockMu.Lock()
	ch := b.lockNotify
	b.lockMu.Unlock()
	return ch
}

// wakeLockWaiters wakes every SETLKW waiter after a lock release so they can
// re-attempt their acquire, by closing the current notify channel and swapping
// in a fresh one. Broadcasting on any release (rather than tracking which
// inode/range freed up) is intentionally coarse: a waiter that wakes for an
// unrelated release simply re-checks and parks again.
func (b *Bridge) wakeLockWaiters() {
	b.lockMu.Lock()
	close(b.lockNotify)
	b.lockNotify = make(chan struct{})
	b.lockMu.Unlock()
}

func toStatus(e syscall.Errno) fuse.Status {
	if e == 0 {
		return fuse.OK
	}
	return fuse.Status(e)
}

// recordFuse records a FUSE op's latency + outcome and the op counter in one
// call. Deferred at the top of each instrumented handler with a pointer to its
// (named) status return, so the duration covers the whole handler and the outcome
// reflects the final status. ENOENT/ENOSYS are expected control-flow results
// (a lookup miss, an unimplemented op), not failures, so they record outcome=ok —
// matching the backing-store convention where not_found is not an error. start is
// captured by the caller before any work.
func (b *Bridge) recordFuse(op string, start time.Time, st *fuse.Status) {
	ok := *st == fuse.OK || *st == fuse.ENOENT || *st == fuse.ENOSYS
	b.metrics.RecordFuseOp(op, start, ok)
}

func callerCtx(h *fuse.InHeader) (context.Context, *meta.Context) {
	return context.Background(), &meta.Context{Uid: h.Uid, Gid: h.Gid, Pid: h.Pid}
}

// startFuseSpan opens the parent span for one FUSE op (mlfs.fuse.<op>,
// SpanKind=Internal) on the caller's base ctx and returns the SPAN CTX — the
// thing that must be threaded into every downstream call (b.files.Read/Write,
// the meta engine, the casstore backing wrapper) so their spans nest under this
// one AND the latency histograms they record (mlfs.meta.*, casstore.backing.*,
// mlfs.fuse.op.duration) attach exemplars to this trace. attrs are span-only
// detail (inode/offset/size) — high cardinality is fine on a span, never on a
// metric label (docs/OBSERVABILITY.md). The tracer is the global OTEL tracer, so
// when no TracerProvider is installed (the disabled case) this is a cheap no-op
// returning the input ctx and a no-op span. Pair with endFuseSpan via defer.
func (b *Bridge) startFuseSpan(ctx context.Context, op string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return b.tracer.Start(ctx, "mlfs.fuse."+op,
		trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attrs...))
}

// endFuseSpan closes a FUSE op span, stamping its status from the final FUSE
// status: ENOENT/ENOSYS are expected control-flow results (a lookup miss, an
// unimplemented op), not errors — matching recordFuse's outcome=ok mapping and
// the backing-store convention where not_found is not a failure. Deferred with a
// pointer to the handler's named status return so it observes the real outcome.
func endFuseSpan(span trace.Span, st *fuse.Status) {
	switch *st {
	case fuse.OK, fuse.ENOENT, fuse.ENOSYS:
		// ok / expected control flow: leave status Unset (no error).
	default:
		span.SetStatus(codes.Error, st.String())
	}
	span.End()
}

// unixType maps an mlfs file type to the st_mode type bits.
func unixType(t uint8) uint32 {
	switch t {
	case meta.TypeDirectory:
		return syscall.S_IFDIR
	case meta.TypeSymlink:
		return syscall.S_IFLNK
	case meta.TypeFIFO:
		return syscall.S_IFIFO
	case meta.TypeBlockDev:
		return syscall.S_IFBLK
	case meta.TypeCharDev:
		return syscall.S_IFCHR
	case meta.TypeSocket:
		return syscall.S_IFSOCK
	default:
		return syscall.S_IFREG
	}
}

func fillAttr(ino meta.Ino, a *meta.Attr, out *fuse.Attr) {
	out.Ino = uint64(ino)
	out.Size = a.Length
	out.Blocks = (a.Length + 511) / 512
	out.Atime, out.Mtime, out.Ctime = uint64(a.Atime), uint64(a.Mtime), uint64(a.Ctime)
	out.Atimensec, out.Mtimensec, out.Ctimensec = a.Atimensec, a.Mtimensec, a.Ctimensec
	out.Mode = unixType(a.Typ) | uint32(a.Mode)
	out.Nlink = a.Nlink
	out.Owner = fuse.Owner{Uid: a.Uid, Gid: a.Gid}
	out.Rdev = a.Rdev
	out.Blksize = 4096
}

// nameMax is NAME_MAX: the maximum bytes in a single path component. A longer
// component yields ENAMETOOLONG. The kernel does NOT pre-enforce this under
// default_permissions (it passes the over-long name straight through), so the
// bridge must, or a >255-byte create silently succeeds (pjdfstest */02.t).
const nameMax = 255

func nameTooLong(name string) bool { return len(name) > nameMax }

func (b *Bridge) fillEntry(ino meta.Ino, a *meta.Attr, out *fuse.EntryOut) {
	out.NodeId = uint64(ino)
	out.Generation = 1
	out.SetEntryTimeout(b.entryTTL)
	out.SetAttrTimeout(b.entryTTL)
	fillAttr(ino, a, &out.Attr)
}

func (b *Bridge) Lookup(cancel <-chan struct{}, h *fuse.InHeader, name string, out *fuse.EntryOut) (status fuse.Status) {
	ctx, _ := callerCtx(h)
	parent := meta.Ino(h.NodeId)
	defer b.recordFuse("lookup", time.Now(), &status)
	if handled, st := b.magicLookup(parent, name, out); handled {
		return st
	}
	// Span around the meta lookups so they nest + attach exemplars; opened after
	// the magic short-circuit (a virtual-inode lookup touches no metadata).
	var span trace.Span
	ctx, span = b.startFuseSpan(ctx, "lookup", attribute.Int64("mlfs.parent", int64(parent)))
	defer endFuseSpan(span, &status)
	if nameTooLong(name) {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	switch name {
	case ".":
		a, st := b.meta.GetAttr(ctx, parent)
		if st != 0 {
			return toStatus(st)
		}
		b.fillEntry(parent, a, out)
		return fuse.OK
	case "..":
		a, st := b.meta.GetAttr(ctx, parent)
		if st != 0 {
			return toStatus(st)
		}
		pp := a.Parent
		if pp == 0 {
			pp = meta.RootInode
		}
		pa, st := b.meta.GetAttr(ctx, pp)
		if st != 0 {
			return toStatus(st)
		}
		b.fillEntry(pp, pa, out)
		return fuse.OK
	}
	ino, a, st := b.meta.Lookup(ctx, parent, name)
	if st != 0 {
		return toStatus(st)
	}
	b.fillEntry(ino, a, out)
	return fuse.OK
}

func (b *Bridge) GetAttr(cancel <-chan struct{}, in *fuse.GetAttrIn, out *fuse.AttrOut) (status fuse.Status) {
	ctx, _ := callerCtx(&in.InHeader)
	defer b.recordFuse("getattr", time.Now(), &status)
	if b.magicGetAttr(meta.Ino(in.NodeId), out) {
		return fuse.OK
	}
	var span trace.Span
	ctx, span = b.startFuseSpan(ctx, "getattr", attribute.Int64("mlfs.inode", int64(in.NodeId)))
	defer endFuseSpan(span, &status)
	a, st := b.meta.GetAttr(ctx, meta.Ino(in.NodeId))
	if st != 0 {
		return toStatus(st)
	}
	out.SetTimeout(b.entryTTL)
	fillAttr(meta.Ino(in.NodeId), a, &out.Attr)
	return fuse.OK
}

func (b *Bridge) SetAttr(cancel <-chan struct{}, in *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx, c := callerCtx(&in.InHeader)
	ino := meta.Ino(in.NodeId)
	if in.Valid&fuse.FATTR_SIZE != 0 {
		if st := b.meta.Truncate(ctx, ino, int64(in.Size)); st != 0 {
			return toStatus(st)
		}
		// A truncate changes the file's bytes outside the sequential write flow
		// (shrink drops tail bytes; grow appends a sparse zero hole), so the running
		// whole-file hash can no longer be trusted — force the flush-time recompute.
		b.invalidateHash(ino)
	}
	var mask uint16
	var na meta.Attr
	if in.Valid&fuse.FATTR_MODE != 0 {
		mask |= meta.SetMode
		na.Mode = uint16(in.Mode)
	}
	if in.Valid&fuse.FATTR_UID != 0 {
		mask |= meta.SetUID
		na.Uid = in.Uid
	}
	if in.Valid&fuse.FATTR_GID != 0 {
		mask |= meta.SetGID
		na.Gid = in.Gid
	}
	if in.Valid&fuse.FATTR_ATIME != 0 {
		mask |= meta.SetAtime
		na.Atime = int64(in.Atime)
		na.Atimensec = in.Atimensec
	}
	if in.Valid&fuse.FATTR_MTIME != 0 {
		mask |= meta.SetMtime
		na.Mtime = int64(in.Mtime)
		na.Mtimensec = in.Mtimensec
	}
	if mask != 0 {
		if _, st := b.meta.SetAttr(ctx, ino, mask, &na, c); st != 0 {
			return toStatus(st)
		}
	}
	a, st := b.meta.GetAttr(ctx, ino)
	if st != 0 {
		return toStatus(st)
	}
	out.SetTimeout(b.entryTTL)
	fillAttr(ino, a, &out.Attr)
	return fuse.OK
}

func (b *Bridge) Mkdir(cancel <-chan struct{}, in *fuse.MkdirIn, name string, out *fuse.EntryOut) (status fuse.Status) {
	ctx, c := callerCtx(&in.InHeader)
	defer b.recordFuse("mkdir", time.Now(), &status)
	var span trace.Span
	ctx, span = b.startFuseSpan(ctx, "mkdir", attribute.Int64("mlfs.parent", int64(in.NodeId)))
	defer endFuseSpan(span, &status)
	if nameTooLong(name) {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	mode := uint16(in.Mode &^ in.Umask)
	effMode, accessACL, defaultACL, has := b.inheritACL(ctx, meta.Ino(in.NodeId), uint16(in.Mode&07777), true)
	if has {
		mode = effMode
	}
	ino, a, st := b.meta.Mkdir(ctx, meta.Ino(in.NodeId), name, mode, c)
	if st != 0 {
		return toStatus(st)
	}
	if has {
		b.stampACL(ctx, ino, accessACL, defaultACL)
		a = b.meta.GetAttrOrNil(ctx, ino) // reflect ACL-adjusted mode
	}
	b.fillEntry(ino, a, out)
	return fuse.OK
}

func (b *Bridge) Mknod(cancel <-chan struct{}, in *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	ctx, c := callerCtx(&in.InHeader)
	if nameTooLong(name) {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	typ := fileTypeFromMode(in.Mode)
	mode := uint16(in.Mode & 07777)
	effMode, accessACL, _, has := b.inheritACL(ctx, meta.Ino(in.NodeId), mode, false)
	if has {
		mode = effMode
	}
	ino, a, st := b.meta.Mknod(ctx, meta.Ino(in.NodeId), name, typ, mode, in.Rdev, c)
	if st != 0 {
		return toStatus(st)
	}
	if has {
		b.stampACL(ctx, ino, accessACL, nil)
		a = b.meta.GetAttrOrNil(ctx, ino)
	}
	b.fillEntry(ino, a, out)
	return fuse.OK
}

func (b *Bridge) Create(cancel <-chan struct{}, in *fuse.CreateIn, name string, out *fuse.CreateOut) (status fuse.Status) {
	ctx, c := callerCtx(&in.InHeader)
	defer b.recordFuse("create", time.Now(), &status)
	var span trace.Span
	ctx, span = b.startFuseSpan(ctx, "create", attribute.Int64("mlfs.parent", int64(in.NodeId)))
	defer endFuseSpan(span, &status)
	if nameTooLong(name) {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	mode := uint16(in.Mode &^ in.Umask)
	effMode, accessACL, _, has := b.inheritACL(ctx, meta.Ino(in.NodeId), uint16(in.Mode&07777), false)
	if has {
		mode = effMode
	}
	ino, a, st := b.meta.Create(ctx, meta.Ino(in.NodeId), name, mode, c)
	if st != 0 {
		return toStatus(st)
	}
	// Classify the file's compression class by name and stamp it on the inode, so
	// every slice it writes inherits the class (model/tensor → uncompressed). Then
	// cache it for the hot write path. A failed stamp degrades to ClassDefault.
	class := classifyStoreClass(name, b.defaultStoreClass)
	if class != uint8(chunkstore.ClassDefault) {
		if cst := b.meta.SetStoreClass(ctx, ino, class); cst != 0 {
			class = uint8(chunkstore.ClassDefault)
		}
	}
	b.classCache.Store(uint64(ino), class)
	if has {
		b.stampACL(ctx, ino, accessACL, nil)
		a = b.meta.GetAttrOrNil(ctx, ino)
	}
	b.fillEntry(ino, a, &out.EntryOut)
	b.meta.OpenRef(ino) // the created file is open
	out.Fh = 0
	return fuse.OK
}

func (b *Bridge) Unlink(cancel <-chan struct{}, h *fuse.InHeader, name string) (status fuse.Status) {
	ctx, c := callerCtx(h)
	defer b.recordFuse("unlink", time.Now(), &status)
	var span trace.Span
	ctx, span = b.startFuseSpan(ctx, "unlink", attribute.Int64("mlfs.parent", int64(h.NodeId)))
	defer endFuseSpan(span, &status)
	if nameTooLong(name) {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	return toStatus(b.meta.Unlink(ctx, meta.Ino(h.NodeId), name, c))
}

func (b *Bridge) Rmdir(cancel <-chan struct{}, h *fuse.InHeader, name string) fuse.Status {
	ctx, c := callerCtx(h)
	if nameTooLong(name) {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	return toStatus(b.meta.Rmdir(ctx, meta.Ino(h.NodeId), name, c))
}

func (b *Bridge) Rename(cancel <-chan struct{}, in *fuse.RenameIn, oldName, newName string) fuse.Status {
	ctx, c := callerCtx(&in.InHeader)
	if nameTooLong(oldName) || nameTooLong(newName) {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	_, _, st := b.meta.Rename(ctx, meta.Ino(in.NodeId), oldName, meta.Ino(in.Newdir), newName, in.Flags, c)
	return toStatus(st)
}

func (b *Bridge) Symlink(cancel <-chan struct{}, h *fuse.InHeader, target, name string, out *fuse.EntryOut) fuse.Status {
	ctx, c := callerCtx(h)
	if nameTooLong(name) {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	ino, a, st := b.meta.Symlink(ctx, meta.Ino(h.NodeId), name, target, c)
	if st != 0 {
		return toStatus(st)
	}
	b.fillEntry(ino, a, out)
	return fuse.OK
}

func (b *Bridge) Link(cancel <-chan struct{}, in *fuse.LinkIn, name string, out *fuse.EntryOut) fuse.Status {
	ctx, c := callerCtx(&in.InHeader)
	if nameTooLong(name) {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	a, st := b.meta.Link(ctx, meta.Ino(in.Oldnodeid), meta.Ino(in.NodeId), name, c)
	if st != 0 {
		return toStatus(st)
	}
	b.fillEntry(meta.Ino(in.Oldnodeid), a, out)
	return fuse.OK
}

func (b *Bridge) Readlink(cancel <-chan struct{}, h *fuse.InHeader) ([]byte, fuse.Status) {
	ctx, _ := callerCtx(h)
	target, st := b.meta.ReadLink(ctx, meta.Ino(h.NodeId))
	return target, toStatus(st)
}

func (b *Bridge) Open(cancel <-chan struct{}, in *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	ctx, _ := callerCtx(&in.InHeader)
	defer b.recordFuse("open", time.Now(), &status)
	if handled, st := b.magicOpen(meta.Ino(in.NodeId), out); handled {
		return st
	}
	var span trace.Span
	ctx, span = b.startFuseSpan(ctx, "open", attribute.Int64("mlfs.inode", int64(in.NodeId)))
	defer endFuseSpan(span, &status)
	if _, st := b.meta.Open(ctx, meta.Ino(in.NodeId)); st != 0 {
		return toStatus(st)
	}
	out.Fh = 0
	return fuse.OK
}

// Release drops the open handle; the last close of an unlinked file deletes it.
// If the kernel flagged a flock unlock for this release, drop the flock too.
func (b *Bridge) Release(cancel <-chan struct{}, in *fuse.ReleaseIn) {
	if b.magicReleaseFh(meta.Ino(in.NodeId), in.Fh) {
		return // virtual file: free the parked snapshot, no metadata inode to close
	}
	if in.ReleaseFlags&fuse.FUSE_RELEASE_FLOCK_UNLOCK != 0 && in.LockOwner != 0 {
		b.meta.Flock(context.Background(), meta.Ino(in.NodeId), in.LockOwner, meta.FlockUnlock)
		b.wakeLockWaiters() // the dropped flock may make a parked SETLKW grantable
	}
	// Finalize the whole-file content hash before dropping the per-open accumulator,
	// as a safety net for a Release that arrives without a preceding Flush (Flush
	// already stamped it on the common close(2) path; re-finalizing is idempotent —
	// the same digest). Then evict the state to bound memory; a reopen re-derives it
	// from the first Write or recomputes on finalize.
	b.finalizeWriteHash(context.Background(), meta.Ino(in.NodeId))
	b.hashStates.Delete(uint64(in.NodeId))
	b.classCache.Delete(uint64(in.NodeId)) // bound the cache; a reopen re-reads the class
	b.meta.Close(context.Background(), meta.Ino(in.NodeId))
}

// --- advisory locks (flock + POSIX byte-range), forwarded by EnableLocks ---

func (b *Bridge) GetLk(cancel <-chan struct{}, in *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	ctx, _ := callerCtx(&in.InHeader)
	typ, start, end, pid, st := b.meta.Getlk(ctx, meta.Ino(in.NodeId), in.Owner,
		uint8(in.Lk.Typ), in.Lk.Start, in.Lk.End)
	if st != 0 {
		return toStatus(st)
	}
	out.Lk = fuse.FileLock{Typ: uint32(typ), Start: start, End: end, Pid: pid}
	return fuse.OK
}

func (b *Bridge) SetLk(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	return b.setlk(in)
}

// SetLkw is the blocking variant. The engine acquires non-blockingly (EAGAIN on
// conflict), so we implement true blocking in-process: attempt the acquire, and
// on EAGAIN park on the bridge's notify channel until some lock release wakes us
// (or the FUSE cancel channel fires → EINTR), then re-attempt. We grab the wait
// channel BEFORE each attempt so a release that races between the failed acquire
// and parking still wakes us. This blocks until the lock becomes grantable
// rather than giving up under sustained contention.
func (b *Bridge) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	for {
		ch := b.lockWaitCh()
		st := b.setlk(in)
		if st != fuse.Status(syscall.EAGAIN) {
			return st
		}
		select {
		case <-cancel:
			return fuse.EINTR
		case <-ch:
			// A lock was released; re-attempt.
		}
	}
}

func (b *Bridge) setlk(in *fuse.LkIn) fuse.Status {
	ctx, _ := callerCtx(&in.InHeader)
	ino := meta.Ino(in.NodeId)
	if in.LkFlags&fuse.FUSE_LK_FLOCK != 0 {
		var ft uint8
		switch in.Lk.Typ {
		case syscall.F_RDLCK:
			ft = meta.FlockShared
		case syscall.F_WRLCK:
			ft = meta.FlockExclusive
		default:
			ft = meta.FlockUnlock
		}
		st := toStatus(b.meta.Flock(ctx, ino, in.Owner, ft))
		if ft == meta.FlockUnlock && st == fuse.OK {
			b.wakeLockWaiters() // a release may have made a parked SETLKW grantable
		}
		return st
	}
	st := toStatus(b.meta.Setlk(ctx, ino, in.Owner,
		uint8(in.Lk.Typ), in.Lk.Start, in.Lk.End, in.Lk.Pid))
	if in.Lk.Typ == syscall.F_UNLCK && st == fuse.OK {
		b.wakeLockWaiters() // ditto for byte-range unlock
	}
	return st
}

func (b *Bridge) Read(cancel <-chan struct{}, in *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	ctx, _ := callerCtx(&in.InHeader)
	start := time.Now()
	if handled, res, st := b.magicRead(in, buf); handled {
		b.metrics.RecordFuseOp("read", start, st == fuse.OK)
		return res, st
	}
	// Parent span for the read fan-out: the cache-miss → manifest(PG) → pack(S3)
	// leaf spans nest under it (via this ctx) so a slow cold read decomposes, and
	// the latency histograms recorded within it attach exemplars. inode/offset/size
	// are span-only detail (high cardinality, never a metric label).
	ctx, span := b.startFuseSpan(ctx, "read",
		attribute.Int64("mlfs.inode", int64(in.NodeId)),
		attribute.Int64("mlfs.offset", int64(in.Offset)),
		attribute.Int("mlfs.size", int(in.Size)))
	defer span.End()
	n, err := b.files.Read(ctx, meta.Ino(in.NodeId), int64(in.Offset), buf[:in.Size])
	if err != nil {
		// Data-integrity canary: classify the read error. A casstore corrupt /
		// missing-blob fault (chunk-hash mismatch, pack-not-found) is the corruption /
		// GC-over-deletion smoke alarm (docs/OBSERVABILITY.md) — bump the counter and
		// log ERROR so it is never lost in the generic EIO. snapshot.IntegrityKind
		// returns "" for an ordinary backend error, which records nothing here.
		if kind := snapshot.IntegrityKind(err); kind != "" {
			b.metrics.IntegrityError(kind)
			slog.Error("mlfs: read integrity fault", "inode", in.NodeId, "offset", in.Offset,
				"size", in.Size, "kind", kind, "err", err)
			// Mark the integrity fault on the span too (same kind we classify for the
			// metric canary) so a corrupt/missing-blob read is searchable in traces.
			span.SetAttributes(attribute.String("mlfs.integrity.kind", kind))
		} else {
			// The kernel only gets an errno, so log the real cause (which slice/pack,
			// requested [off,len), backend status) — otherwise a read failure surfaces
			// as an opaque `os error 5`/SIGBUS in the workload with nothing to debug.
			slog.Error("mlfs: read failed", "inode", in.NodeId, "offset", in.Offset, "size", in.Size, "err", err)
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "read failed")
		b.metrics.RecordFuseOp("read", start, false)
		return nil, fuse.EIO
	}
	b.metrics.AddReadBytes(n)
	b.metrics.RecordFuseOp("read", start, true)
	return fuse.ReadResultData(buf[:n]), fuse.OK
}

func (b *Bridge) Write(cancel <-chan struct{}, in *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	ctx, _ := callerCtx(&in.InHeader)
	start := time.Now()
	// Parent span for the write: the slice writes (chunk-store Put → backing pack)
	// + slice_ref commits nest under it via this ctx, and the write latency
	// histogram attaches an exemplar. inode/offset/size are span-only detail.
	ctx, span := b.startFuseSpan(ctx, "write",
		attribute.Int64("mlfs.inode", int64(in.NodeId)),
		attribute.Int64("mlfs.offset", int64(in.Offset)),
		attribute.Int("mlfs.size", len(data)))
	defer span.End()
	class := b.storeClassFor(ctx, meta.Ino(in.NodeId))
	n, err := b.files.Write(ctx, meta.Ino(in.NodeId), int64(in.Offset), data, class)
	if err != nil {
		slog.Error("mlfs: write failed", "inode", in.NodeId, "offset", in.Offset, "size", len(data), "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "write failed")
		b.metrics.RecordFuseOp("write", start, false)
		return 0, fuse.EIO
	}
	b.metrics.AddWriteBytes(n)
	b.metrics.AddWriteClassBytes(n, class == chunkstore.ClassUncompressed)
	b.metrics.RecordFuseOp("write", start, true)
	// Fold the just-written bytes into the file's running whole-file sha256 (a
	// cheap hash update on the sequential fast path, no I/O; a behind/ahead-watermark
	// write instead flips to the flush-time recompute). Uses n (bytes actually
	// written) so a short write feeds exactly the persisted range.
	b.noteWriteHash(meta.Ino(in.NodeId), int64(in.Offset), data[:n])
	return uint32(n), fuse.OK
}

// CopyFileRange serves copy_file_range(2): it copies a byte range from one file
// to another. The data path shares the underlying chunks copy-on-write for any
// chunk-aligned portion (zero data movement, like reflink) and falls back to a
// read+write copy for misaligned head/tail bytes, so the result is always
// byte-correct. The kernel may issue this in several calls for a large copy.
func (b *Bridge) CopyFileRange(cancel <-chan struct{}, in *fuse.CopyFileRangeIn) (uint32, fuse.Status) {
	ctx, _ := callerCtx(&in.InHeader)
	n, err := b.files.CopyFileRange(ctx, meta.Ino(in.NodeId), meta.Ino(in.NodeIdOut),
		int64(in.OffIn), int64(in.OffOut), int(in.Len))
	if err != nil {
		return 0, fuse.EIO
	}
	// The destination's bytes changed via a path other than Write, so its running
	// whole-file hash is stale — force the flush-time recompute for dst.
	b.invalidateHash(meta.Ino(in.NodeIdOut))
	return uint32(n), fuse.OK
}

func (b *Bridge) Flush(cancel <-chan struct{}, in *fuse.FlushIn) fuse.Status {
	// No cache drain on close — every Write is already durable (see Fsync).
	// POSIX: closing any fd releases the process's byte-range locks on the file.
	// The kernel sends FLUSH with the owning lock_owner on each close(2).
	if in.LockOwner != 0 {
		b.meta.ClearPlocks(context.Background(), meta.Ino(in.NodeId), in.LockOwner)
		b.wakeLockWaiters() // cleared record locks may unblock a parked SETLKW
	}
	// Finalize the whole-file content hash on close(2): the kernel sends FLUSH on
	// every fd close, so a written-and-closed file has its sha256 persisted here
	// (the running digest on the sequential fast path, or a recompute otherwise).
	// Best-effort — a hashing failure is logged, never surfaced as a close error
	// (the data is already durable; the hash is integrity metadata).
	b.finalizeWriteHash(context.Background(), meta.Ino(in.NodeId))
	return fuse.OK
}

// finalizeWriteHash persists ino's whole-file content hash if it was written
// through this mount, logging (never propagating) any failure. Shared by Flush
// (close) and Release (safety net for a release without a preceding flush).
func (b *Bridge) finalizeWriteHash(ctx context.Context, ino meta.Ino) {
	if _, err := b.finalizeHash(ctx, ino); err != nil {
		slog.Error("mlfs: finalize content hash failed", "inode", uint64(ino), "err", err)
	}
}

func (b *Bridge) Fsync(cancel <-chan struct{}, in *fuse.FsyncIn) (status fuse.Status) {
	defer b.recordFuse("fsync", time.Now(), &status)
	// No-op: every Write is already staged durably (the staging file AND its dir
	// are fsync'd before Write returns), so all of the file's written data — and
	// its slice_ref metadata (committed per write, WAL-logged) — is durable by the
	// time fsync(2) arrives. The upload to the backing store is async (the
	// write-back uploader); it is NODE-LOSS durability (HC1/HC2 + the safety
	// window), NOT what POSIX fsync requires, which is satisfied by the local
	// fsync'd staging (replayed on restart after a crash). Draining here was pure
	// cost — a full casstore Put (pack write + manifest) on every fsync.
	return fuse.OK
}

// --- extended attributes ---

func (b *Bridge) GetXAttr(cancel <-chan struct{}, h *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	ctx, _ := callerCtx(h)
	val, st := b.meta.GetXAttr(ctx, meta.Ino(h.NodeId), attr)
	if st != 0 {
		return 0, toStatus(st)
	}
	if len(dest) == 0 { // size probe: report the size, let the kernel re-ask
		return uint32(len(val)), fuse.OK
	}
	if len(dest) < len(val) {
		return uint32(len(val)), fuse.ERANGE
	}
	return uint32(copy(dest, val)), fuse.OK
}

func (b *Bridge) ListXAttr(cancel <-chan struct{}, h *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	ctx, _ := callerCtx(h)
	names, st := b.meta.ListXAttr(ctx, meta.Ino(h.NodeId))
	if st != 0 {
		return 0, toStatus(st)
	}
	var buf []byte // names are '\0'-delimited per the FUSE listxattr contract
	for _, n := range names {
		buf = append(buf, n...)
		buf = append(buf, 0)
	}
	if len(dest) == 0 {
		return uint32(len(buf)), fuse.OK
	}
	if len(dest) < len(buf) {
		return uint32(len(buf)), fuse.ERANGE
	}
	return uint32(copy(dest, buf)), fuse.OK
}

func (b *Bridge) SetXAttr(cancel <-chan struct{}, in *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	ctx, _ := callerCtx(&in.InHeader)
	return toStatus(b.meta.SetXAttr(ctx, meta.Ino(in.NodeId), attr, data, in.Flags))
}

func (b *Bridge) RemoveXAttr(cancel <-chan struct{}, h *fuse.InHeader, attr string) fuse.Status {
	ctx, _ := callerCtx(h)
	return toStatus(b.meta.RemoveXAttr(ctx, meta.Ino(h.NodeId), attr))
}

// OpenDir snapshots the directory's ordered entry list (with attributes) into a
// handle table under a fresh Fh, so ReadDirPlus can page by stable offset even
// while the directory is concurrently mutated.
func (b *Bridge) OpenDir(cancel <-chan struct{}, in *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	ctx, _ := callerCtx(&in.InHeader)
	if b.magicOpenDir(meta.Ino(in.NodeId), out) {
		return fuse.OK
	}
	entries, st := b.meta.Readdir(ctx, meta.Ino(in.NodeId), true)
	if st != 0 {
		return toStatus(st)
	}
	b.dmu.Lock()
	fh := b.dirNext
	b.dirNext++
	b.dirs[fh] = entries
	b.dmu.Unlock()
	out.Fh = fh
	return fuse.OK
}

func (b *Bridge) ReadDirPlus(cancel <-chan struct{}, in *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	b.dmu.Lock()
	entries := b.dirs[in.Fh]
	b.dmu.Unlock()
	for i := int(in.Offset); i < len(entries); i++ {
		e := entries[i]
		typ := meta.TypeFile
		if e.Attr != nil {
			typ = e.Attr.Typ
		}
		de := fuse.DirEntry{Mode: unixType(typ), Name: e.Name, Ino: uint64(e.Inode)}
		eo := out.AddDirLookupEntry(de)
		if eo == nil {
			break // buffer full; kernel will resume at this offset
		}
		if e.Attr != nil {
			b.fillEntry(e.Inode, e.Attr, eo)
		}
	}
	return fuse.OK
}

// ReadDir pages the OpenDir snapshot without filling attributes (the plain,
// non-plus variant). Shares the same stable offsets as ReadDirPlus.
func (b *Bridge) ReadDir(cancel <-chan struct{}, in *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	b.dmu.Lock()
	entries := b.dirs[in.Fh]
	b.dmu.Unlock()
	for i := int(in.Offset); i < len(entries); i++ {
		e := entries[i]
		typ := meta.TypeFile
		if e.Attr != nil {
			typ = e.Attr.Typ
		}
		de := fuse.DirEntry{Mode: unixType(typ), Name: e.Name, Ino: uint64(e.Inode)}
		if !out.AddDirEntry(de) {
			break // buffer full; kernel resumes at this offset
		}
	}
	return fuse.OK
}

// ReleaseDir frees the directory snapshot allocated by OpenDir.
func (b *Bridge) ReleaseDir(in *fuse.ReleaseIn) {
	b.dmu.Lock()
	delete(b.dirs, in.Fh)
	b.dmu.Unlock()
}

func (b *Bridge) StatFs(cancel <-chan struct{}, h *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	// Report a large, mostly-free volume (capacity tracking is L2.x).
	out.Blocks = 1 << 40
	out.Bfree = 1 << 39
	out.Bavail = 1 << 39
	out.Bsize = 4096
	out.Frsize = 4096
	out.NameLen = 255
	return fuse.OK
}

// fileTypeFromMode extracts the mlfs file type from a unix st_mode.
func fileTypeFromMode(mode uint32) uint8 {
	switch mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		return meta.TypeDirectory
	case syscall.S_IFLNK:
		return meta.TypeSymlink
	case syscall.S_IFIFO:
		return meta.TypeFIFO
	case syscall.S_IFBLK:
		return meta.TypeBlockDev
	case syscall.S_IFCHR:
		return meta.TypeCharDev
	case syscall.S_IFSOCK:
		return meta.TypeSocket
	default:
		return meta.TypeFile
	}
}

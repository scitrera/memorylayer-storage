// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"log/slog"
	"sync"
	"syscall"
	"time"
)

// inodeBatch is how many inodes a node reserves from the shared counter per
// round-trip (amortizes allocation across writes).
const inodeBatch = 1024

// Engine is the mlfs metadata system-of-record over a PostgreSQL database
// (via database/sql; the caller supplies the *sql.DB and driver). It is safe
// for concurrent use.
type Engine struct {
	db     *sql.DB
	prefix uint16 // 16-bit region+session tag in the high inode bits
	now    func() time.Time
	log    *slog.Logger // never nil; defaults to slog.Default()

	mu      sync.Mutex
	inoNext uint64 // next free counter value in the reserved batch
	inoEnd  uint64 // end (exclusive) of the reserved batch

	smu       sync.Mutex
	sliceNext uint64 // next free slice id in the reserved batch
	sliceEnd  uint64

	// Open-handle tracking for POSIX open-but-unlinked semantics. An inode
	// whose link count hits zero while still open is kept ("sustained") and
	// deleted on the last Close. In-memory per-mount; sufficient for the
	// single-node L2 (multi-node uses the sessions table, later).
	omu       sync.Mutex
	openRefs  map[Ino]int
	sustained map[Ino]struct{}

	// Multi-node coordination (L2.8). nodeID identifies this mount; owner is
	// the ownership-lease coordinator the write path consults before mutating.
	// In single-node mode owner is noopOwner (Enabled()==false), so the scope
	// resolution + fence are skipped entirely and behavior is byte-identical to
	// the pre-L2.8 engine. Set via SetCoordinator before serving.
	nodeID string
	owner  OwnerView

	// scopeCache memoizes inode → scope_key (the deterministic walk to the
	// first-level ancestor). Only a cross-scope rename mutates the mapping; that
	// path flushes the cache. Empty/unused in single-node mode.
	scopeCache sync.Map // Ino -> string

	// rec, when set, records meta-DB op latency + errors (the meta RED view) and
	// fence rejections. An interface (not a hard dependency on internal/metrics)
	// keeps the engine decoupled and testable; *metrics.Registry satisfies it.
	// Nil = no recording.
	rec MetaRecorder
}

// MetaRecorder is the engine's optional metrics hook. *metrics.Registry
// implements it. RecordMetaOp takes the op label, the op's start time, and a
// pointer to the error (nil = success), so a call site can
// `defer e.rec.RecordMetaOp("read_slices", time.Now(), &err)`. FenceRejected
// counts a write rejected because the ownership generation moved (multi-node).
type MetaRecorder interface {
	RecordMetaOp(op string, start time.Time, err *error)
	FenceRejected()
}

// SetMetricsRecorder installs the meta-DB RED + fence recorder (nil clears it).
// Call once before serving. A nil engine or nil recorder leaves recording off;
// the engine never assumes a recorder is present.
func (e *Engine) SetMetricsRecorder(r MetaRecorder) { e.rec = r }

// recordOp records one meta-DB op's latency + outcome through the recorder, if
// one is set. Deferred at the top of an instrumented op with a pointer to the op's
// underlying error (nil on success). A no-op when no recorder is installed.
func (e *Engine) recordOp(op string, start time.Time, errp *error) {
	if e.rec == nil {
		return
	}
	e.rec.RecordMetaOp(op, start, errp)
}

// OwnerView is the engine's hook into multi-node ownership coordination
// (L2.8). Before any metadata mutation, the write path calls Ensure(scope) to
// acquire-or-confirm this node's lease on the scope; the returned generation is
// the fencing token re-checked inside the write transaction (see checkFence).
//
// Single-node mode wires noopOwner, whose Enabled() reports false — the engine
// then skips scope resolution and the fence outright, preserving the exact
// single-node behavior and cost. The authoritative fence always lives in SQL
// (the ownership row); an OwnerView only decides liveness and accelerates
// discovery (e.g. the NATS-backed coordinator in internal/coord).
type OwnerView interface {
	// Enabled reports whether multi-node fencing is active. False short-circuits
	// the whole scope/fence path for the single-node fast path.
	Enabled() bool
	// Ensure makes this node the owner of scope (acquiring or renewing the
	// lease) and returns the generation it holds. held=false means another live
	// node owns it: the caller returns ESTALE (the kernel/caller retries, by
	// which point a handoff may have completed).
	Ensure(ctx context.Context, scope string) (gen int64, held bool, err error)
}

// noopOwner is the single-node default: fencing disabled, every scope "held".
type noopOwner struct{}

func (noopOwner) Enabled() bool { return false }
func (noopOwner) Ensure(context.Context, string) (int64, bool, error) {
	return 0, true, nil
}

// SetCoordinator enables multi-node ownership fencing: nodeID identifies this
// mount and owner drives lease acquisition/liveness. Passing a nil owner (or
// not calling this at all) leaves the engine in single-node mode. Call once,
// before the engine starts serving.
func (e *Engine) SetCoordinator(nodeID string, owner OwnerView) {
	e.nodeID = nodeID
	if owner == nil {
		owner = noopOwner{}
	}
	e.owner = owner
}

// NodeID returns this mount's coordination identity (empty in single-node mode).
func (e *Engine) NodeID() string { return e.nodeID }

// Option configures an Engine at construction time (see Open).
type Option func(*Engine)

// WithLogger sets the slog.Logger used for operational diagnostics (e.g. the
// underlying cause behind an EIO returned to FUSE). A nil logger is ignored so
// the engine never logs through a nil handler.
func WithLogger(l *slog.Logger) Option {
	return func(e *Engine) {
		if l != nil {
			e.log = l
		}
	}
}

// Open wraps a database/sql handle. region is this node's 8-bit region id; a
// random 8-bit session id is mixed in so concurrently-running nodes draw
// distinct inode prefixes. Call Migrate before use. Optional Options (e.g.
// WithLogger) tune the engine; the logger defaults to slog.Default().
func Open(db *sql.DB, region uint8, opts ...Option) *Engine {
	var b [1]byte
	_, _ = rand.Read(b[:])
	e := &Engine{
		db:        db,
		prefix:    uint16(region)<<8 | uint16(b[0]),
		now:       time.Now,
		log:       slog.Default(),
		openRefs:  make(map[Ino]int),
		sustained: make(map[Ino]struct{}),
		owner:     noopOwner{}, // single-node default; SetCoordinator enables L2.8
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// SetClock overrides the clock (tests).
func (e *Engine) SetClock(now func() time.Time) { e.now = now }

// eio logs the underlying cause of a metadata failure and returns syscall.EIO,
// the errno FUSE requires for an I/O error. It centralizes the EIO mapping so
// every collapsed Postgres error leaves a breadcrumb (op label + error only —
// never row contents or user bytes). op is a short, stable operation label
// (e.g. "getAttr", "rename"). A nil err still returns EIO but logs nothing
// (used for logic guards that have no underlying error to report).
func (e *Engine) eio(ctx context.Context, op string, err error) syscall.Errno {
	if err != nil {
		e.log.ErrorContext(ctx, "mlfs meta", "op", op, "err", err)
	}
	return syscall.EIO
}

const inoLowMask = (uint64(1) << 48) - 1

// nextInode hands out a globally-unique inode: the node's 16-bit prefix in the
// high bits, a counter from the shared DB sequence (batched) in the low 48.
func (e *Engine) nextInode(ctx context.Context) (Ino, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.inoNext >= e.inoEnd {
		var end int64
		if err := e.db.QueryRowContext(ctx,
			`UPDATE mlfs_counter SET value = value + $1 WHERE name = 'nextinode' RETURNING value`,
			inodeBatch).Scan(&end); err != nil {
			return 0, err
		}
		e.inoEnd = uint64(end)
		e.inoNext = uint64(end) - inodeBatch
	}
	c := e.inoNext
	e.inoNext++
	return Ino(uint64(e.prefix)<<48 | (c & inoLowMask)), nil
}

// NewSlice allocates a globally-unique slice id (batched from the shared
// counter). Slice ids name a slice's data in the chunk store.
func (e *Engine) NewSlice(ctx context.Context) (uint64, error) {
	e.smu.Lock()
	defer e.smu.Unlock()
	if e.sliceNext >= e.sliceEnd {
		var end int64
		if err := e.db.QueryRowContext(ctx,
			`UPDATE mlfs_counter SET value = value + $1 WHERE name = 'nextslice' RETURNING value`,
			inodeBatch).Scan(&end); err != nil {
			return 0, err
		}
		e.sliceEnd = uint64(end)
		e.sliceNext = uint64(end) - inodeBatch
	}
	id := e.sliceNext
	e.sliceNext++
	return id, nil
}

// appendChangelog records one change in the same transaction as the mutation,
// so the durable, ordered (BIGSERIAL lsn) log can never diverge from state.
func (e *Engine) appendChangelog(ctx context.Context, tx *sql.Tx, ino Ino, op uint8, payload any) error {
	var pj []byte
	if payload != nil {
		var err error
		if pj, err = json.Marshal(payload); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx,
		`INSERT INTO fs_changelog (ino, op, payload, ts) VALUES ($1, $2, $3, $4)`,
		int64(ino), int(op), pj, e.now().UnixNano())
	return err
}

// ChangeEntry is one row of the fs_changelog, returned by ScanChangelog.
type ChangeEntry struct {
	LSN     int64
	Ino     Ino
	Op      uint8
	Payload []byte
	TS      int64
}

// ScanChangelog yields changelog entries with lsn > after, in lsn order. This
// is the durable, replayable backstop for the change feed (L2.8).
func (e *Engine) ScanChangelog(ctx context.Context, after int64, fn func(ChangeEntry) error) error {
	rows, err := e.db.QueryContext(ctx,
		`SELECT lsn, ino, op, payload, ts FROM fs_changelog WHERE lsn > $1 ORDER BY lsn ASC`, after)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var ce ChangeEntry
		var ino int64
		var op int
		if err := rows.Scan(&ce.LSN, &ino, &op, &ce.Payload, &ce.TS); err != nil {
			return err
		}
		ce.Ino, ce.Op = Ino(ino), uint8(op)
		if err := fn(ce); err != nil {
			return err
		}
	}
	return rows.Err()
}

// nodeColumns is the SELECT list matching scanAttr.
const nodeColumns = `type, mode, uid, gid, atime, mtime, ctime, atimensec, mtimensec, ctimensec, nlink, length, rdev, parent, store_class, content_hash`

// scanAttr scans a node row (in nodeColumns order) into an Attr.
func scanAttr(scan func(dest ...any) error) (*Attr, error) {
	var a Attr
	var typ, mode, storeClass int
	var uid, gid, nlink, rdev, parent int64
	var length int64
	if err := scan(&typ, &mode, &uid, &gid, &a.Atime, &a.Mtime, &a.Ctime,
		&a.Atimensec, &a.Mtimensec, &a.Ctimensec, &nlink, &length, &rdev, &parent, &storeClass, &a.ContentHash); err != nil {
		return nil, err
	}
	a.Typ = uint8(typ)
	a.Mode = uint16(mode)
	a.Uid, a.Gid = uint32(uid), uint32(gid)
	a.Nlink = uint32(nlink)
	a.Length = uint64(length)
	a.Rdev = uint32(rdev)
	a.Parent = Ino(parent)
	a.StoreClass = uint8(storeClass)
	a.Full = true
	return &a, nil
}

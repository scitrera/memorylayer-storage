// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"database/sql"
	"encoding/json"
	"syscall"
)

// POSIX/flock locking over the flock and plock tables. Locks are stored in the
// metadata DB (not handled mount-locally by the kernel), so holders across
// mounts/nodes conflict — the basis for distributed coordination. Acquisition
// is non-blocking at the engine level (EAGAIN on conflict); the FUSE bridge
// implements blocking SETLKW by parking on a release-notify channel and
// re-attempting until grantable.
//
// Lock owners are identified by (sid, owner): sid is this engine instance's
// session (its inode prefix, distinct per mount), owner is the kernel's lock
// owner. flock holds a whole-file shared/exclusive lock; plock holds a set of
// byte ranges per owner.

// flock ltype values stored in flock.ltype (and passed by the bridge).
const (
	FlockShared    = 'R'
	FlockExclusive = 'W'
	FlockUnlock    = 'U'
)

// lockSID is this engine's lock-session id (stable for the instance, distinct
// across mounts because the prefix mixes in a random session byte).
func (e *Engine) lockSID() uint64 { return uint64(e.prefix) }

// Flock acquires ('R'/'W') or releases ('U') a whole-file BSD lock for owner.
// Non-blocking: EAGAIN if a conflicting holder exists.
func (e *Engine) Flock(ctx context.Context, inode Ino, owner uint64, typ uint8) syscall.Errno {
	sid := e.lockSID()
	var out syscall.Errno
	txErr := e.withTx(ctx, func(tx *sql.Tx) error {
		if typ == FlockUnlock {
			_, err := tx.ExecContext(ctx,
				`DELETE FROM flock WHERE inode=$1 AND sid=$2 AND owner=$3`,
				int64(inode), int64(sid), int64(owner))
			return err
		}
		rows, err := tx.QueryContext(ctx,
			`SELECT sid, owner, ltype FROM flock WHERE inode=$1`, int64(inode))
		if err != nil {
			return err
		}
		conflict := false
		for rows.Next() {
			var hsid, howner int64
			var lt int
			if err := rows.Scan(&hsid, &howner, &lt); err != nil {
				rows.Close()
				return err
			}
			if uint64(hsid) == sid && uint64(howner) == owner {
				continue // an existing lock of mine: upgrade/downgrade freely
			}
			// A write request conflicts with any other holder; a read request
			// conflicts only with another write holder.
			if typ == FlockExclusive || uint8(lt) == FlockExclusive {
				conflict = true
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if conflict {
			out = syscall.EAGAIN
			return errAbort
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO flock (inode, sid, owner, ltype) VALUES ($1,$2,$3,$4)
			 ON CONFLICT (inode, sid, owner) DO UPDATE SET ltype = EXCLUDED.ltype`,
			int64(inode), int64(sid), int64(owner), int(typ))
		return err
	})
	if out != 0 {
		return out
	}
	if txErr != nil {
		return e.eio(ctx, "flock", txErr)
	}
	return 0
}

// plockRec is one byte-range record (End is inclusive). Stored JSON-encoded in
// plock.records, one row per (inode, sid, owner).
type plockRec struct {
	Typ   uint8  `json:"t"` // F_RDLCK / F_WRLCK
	Start uint64 `json:"s"`
	End   uint64 `json:"e"`
	Pid   uint32 `json:"p"`
}

// Getlk tests whether the requested byte-range lock could be placed. It returns
// the first conflicting lock (its type/range/pid) or F_UNLCK if none conflict.
func (e *Engine) Getlk(ctx context.Context, inode Ino, owner uint64, typ uint8, start, end uint64) (uint8, uint64, uint64, uint32, syscall.Errno) {
	others, err := e.loadOtherPlocks(ctx, e.db, inode, owner)
	if err != nil {
		return 0, 0, 0, 0, e.eio(ctx, "getlk", err)
	}
	if c := plockConflict(others, typ, start, end); c != nil {
		return c.Typ, c.Start, c.End, c.Pid, 0
	}
	return uint8(syscall.F_UNLCK), start, end, 0, 0
}

// Setlk acquires (F_RDLCK/F_WRLCK) or releases (F_UNLCK) a byte-range lock for
// owner. Non-blocking: EAGAIN if another owner holds a conflicting range.
func (e *Engine) Setlk(ctx context.Context, inode Ino, owner uint64, typ uint8, start, end uint64, pid uint32) syscall.Errno {
	sid := e.lockSID()
	var out syscall.Errno
	txErr := e.withTx(ctx, func(tx *sql.Tx) error {
		if typ != uint8(syscall.F_UNLCK) {
			others, err := e.loadOtherPlocks(ctx, tx, inode, owner)
			if err != nil {
				return err
			}
			if plockConflict(others, typ, start, end) != nil {
				out = syscall.EAGAIN
				return errAbort
			}
		}
		mine, err := e.loadMyPlock(ctx, tx, inode, owner)
		if err != nil {
			return err
		}
		mine = splitRange(mine, typ, start, end, pid)
		if len(mine) == 0 {
			_, err := tx.ExecContext(ctx,
				`DELETE FROM plock WHERE inode=$1 AND sid=$2 AND owner=$3`,
				int64(inode), int64(sid), int64(owner))
			return err
		}
		blob, err := json.Marshal(mine)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO plock (inode, sid, owner, records) VALUES ($1,$2,$3,$4)
			 ON CONFLICT (inode, sid, owner) DO UPDATE SET records = EXCLUDED.records`,
			int64(inode), int64(sid), int64(owner), blob)
		return err
	})
	if out != 0 {
		return out
	}
	if txErr != nil {
		return e.eio(ctx, "setlk", txErr)
	}
	return 0
}

// ClearAllLocks deletes every flock and plock row. The FUSE daemon calls this
// once at mount startup. Locks are advisory state tied to open file
// descriptions; the kernel tears all of them down when a mount goes away, so a
// lock row can never legitimately outlive the mount that took it. In the
// single-node deployment there are also no other live mounts, so any rows
// present at startup are stale leftovers from a previous (possibly crashed)
// instance. Without this, a crash while holding a lock leaves a row under a dead
// session id that every future locker sees as a live foreign holder — wedging
// that file's locking permanently (EAGAIN forever). Multi-node will instead reap
// by session liveness rather than clearing wholesale (L2.8).
func (e *Engine) ClearAllLocks(ctx context.Context) error {
	return e.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM flock`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM plock`)
		return err
	})
}

// ClearLocksForSID deletes only the flock/plock rows owned by one session id —
// the multi-node-safe form of startup reaping (L2.8). At mount startup a node
// calls this with its OWN sid to clear its stale rows from a prior crashed
// incarnation without touching peers' live locks (which ClearAllLocks would
// wrongly nuke). The liveness reaper calls it with a dead peer's sid once the
// peer is confirmed gone (absent from the node-liveness set).
func (e *Engine) ClearLocksForSID(ctx context.Context, sid uint64) error {
	return e.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM flock WHERE sid=$1`, int64(sid)); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM plock WHERE sid=$1`, int64(sid))
		return err
	})
}

// LockSID exposes this engine's lock-session id so the daemon can reap its own
// stale rows at startup (ClearLocksForSID) and publish it to the node-liveness
// set for peer reaping.
func (e *Engine) LockSID() uint64 { return e.lockSID() }

// ListLockSIDs returns the distinct session ids that currently hold any flock or
// plock row. The liveness reaper diffs this against the live-node set to find
// dead sessions whose locks must be cleared.
func (e *Engine) ListLockSIDs(ctx context.Context) ([]uint64, error) {
	rows, err := e.db.QueryContext(ctx,
		`SELECT sid FROM flock UNION SELECT sid FROM plock`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uint64
	for rows.Next() {
		var sid int64
		if err := rows.Scan(&sid); err != nil {
			return nil, err
		}
		out = append(out, uint64(sid))
	}
	return out, rows.Err()
}

// ClearPlocks drops all byte-range (POSIX) locks held by owner on inode. The
// FUSE bridge calls this on flush (every close(2)), matching the POSIX rule that
// closing any fd releases the process's record locks on that file. flock locks
// are released separately via Flock(..., 'U') on release.
func (e *Engine) ClearPlocks(ctx context.Context, inode Ino, owner uint64) {
	if owner == 0 {
		return
	}
	_, _ = e.db.ExecContext(ctx,
		`DELETE FROM plock WHERE inode=$1 AND sid=$2 AND owner=$3`,
		int64(inode), int64(e.lockSID()), int64(owner))
}

// loadMyPlock returns owner's own byte-range records for inode.
func (e *Engine) loadMyPlock(ctx context.Context, q rowQuerier, inode Ino, owner uint64) ([]plockRec, error) {
	var blob []byte
	err := q.QueryRowContext(ctx,
		`SELECT records FROM plock WHERE inode=$1 AND sid=$2 AND owner=$3`,
		int64(inode), int64(e.lockSID()), int64(owner)).Scan(&blob)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodePlock(blob)
}

// loadOtherPlocks returns the flattened byte-range records of every owner OTHER
// than the given one (across all sessions), for conflict checking.
func (e *Engine) loadOtherPlocks(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, inode Ino, owner uint64) ([]plockRec, error) {
	sid := e.lockSID()
	rows, err := q.QueryContext(ctx,
		`SELECT sid, owner, records FROM plock WHERE inode=$1`, int64(inode))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []plockRec
	for rows.Next() {
		var rsid, rowner int64
		var blob []byte
		if err := rows.Scan(&rsid, &rowner, &blob); err != nil {
			return nil, err
		}
		if uint64(rsid) == sid && uint64(rowner) == owner {
			continue // skip my own records
		}
		recs, err := decodePlock(blob)
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
	}
	return out, rows.Err()
}

func decodePlock(blob []byte) ([]plockRec, error) {
	if len(blob) == 0 {
		return nil, nil
	}
	var recs []plockRec
	if err := json.Unmarshal(blob, &recs); err != nil {
		return nil, err
	}
	return recs, nil
}

// plockConflict returns the first record in others that conflicts with a [start,
// end] request of the given type. A write request conflicts with any overlap; a
// read request conflicts only with an overlapping write.
func plockConflict(others []plockRec, typ uint8, start, end uint64) *plockRec {
	for i := range others {
		r := others[i]
		if r.End < start || r.Start > end {
			continue // disjoint
		}
		if typ == uint8(syscall.F_WRLCK) || r.Typ == uint8(syscall.F_WRLCK) {
			return &others[i]
		}
	}
	return nil
}

// splitRange applies a lock/unlock of [start, end] to one owner's record list:
// it trims any overlapping portions of existing records and, for a lock request
// (not F_UNLCK), appends the new range. End is inclusive throughout.
func splitRange(recs []plockRec, typ uint8, start, end uint64, pid uint32) []plockRec {
	var out []plockRec
	for _, r := range recs {
		if r.End < start || r.Start > end {
			out = append(out, r) // untouched
			continue
		}
		if r.Start < start { // keep the head that precedes the new range
			out = append(out, plockRec{Typ: r.Typ, Start: r.Start, End: start - 1, Pid: r.Pid})
		}
		if r.End > end { // keep the tail that follows it
			out = append(out, plockRec{Typ: r.Typ, Start: end + 1, End: r.End, Pid: r.Pid})
		}
		// the overlapping middle is dropped (superseded by the new lock/unlock)
	}
	if typ != uint8(syscall.F_UNLCK) {
		out = append(out, plockRec{Typ: typ, Start: start, End: end, Pid: pid})
	}
	return out
}

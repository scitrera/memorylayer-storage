// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"database/sql"
	"time"
)

// fs_changelog retention. The changelog is the durable, ordered (BIGSERIAL lsn)
// backstop for the change feed (L2.8): every metadata mutation appends a row in
// its own transaction. Nothing trims it automatically, so without retention it
// grows unbounded and bloats the metadata DB. These methods let an operator (or
// the L2.6 daemon, once wired) reclaim entries that every consumer has already
// processed. Trim only below a checkpoint that all change-feed subscribers have
// durably passed, or the feed loses its replay backstop.

// ChangelogHorizon returns the highest lsn currently in fs_changelog (0 when
// empty). Capture this as a checkpoint before doing work you might want to
// replay, then TrimChangelog up to it once the work is durable.
func (e *Engine) ChangelogHorizon(ctx context.Context) (int64, error) {
	var lsn sql.NullInt64
	if err := e.db.QueryRowContext(ctx, `SELECT max(lsn) FROM fs_changelog`).Scan(&lsn); err != nil {
		return 0, err
	}
	return lsn.Int64, nil
}

// TrimChangelog deletes every entry with lsn <= upTo and returns the number
// removed. upTo <= 0 is a no-op. Pass the minimum LSN that all change-feed
// consumers have durably acknowledged.
func (e *Engine) TrimChangelog(ctx context.Context, upTo int64) (int64, error) {
	if upTo <= 0 {
		return 0, nil
	}
	res, err := e.db.ExecContext(ctx, `DELETE FROM fs_changelog WHERE lsn <= $1`, upTo)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// TrimChangelogKeepLast retains only the newest keep entries (by lsn), deleting
// the rest. keep <= 0 deletes nothing. Use this for a simple bounded-size log
// when there is no external consumer dictating the safe horizon.
func (e *Engine) TrimChangelogKeepLast(ctx context.Context, keep int64) (int64, error) {
	if keep <= 0 {
		return 0, nil
	}
	res, err := e.db.ExecContext(ctx,
		`DELETE FROM fs_changelog
		   WHERE lsn < (SELECT min(lsn) FROM (
		       SELECT lsn FROM fs_changelog ORDER BY lsn DESC LIMIT $1) t)`, keep)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// TrimChangelogOlderThan deletes entries whose timestamp is older than age,
// measured against the engine clock. age <= 0 deletes nothing.
func (e *Engine) TrimChangelogOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	if age <= 0 {
		return 0, nil
	}
	cutoff := e.now().Add(-age).UnixNano()
	res, err := e.db.ExecContext(ctx, `DELETE FROM fs_changelog WHERE ts < $1`, cutoff)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

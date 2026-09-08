// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fileio

import (
	"context"
	"fmt"
	"syscall"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/wal"
)

// ReplayResult summarizes a WAL recovery pass.
type ReplayResult struct {
	Applied int // records (re-)committed to the metadata engine
	Skipped int // records no longer applicable (already done, file gone, or fenced)
}

// ReplayWAL re-applies every logged write/truncate to the metadata engine on
// startup, recovering writes that were staged + logged but whose metadata
// commit was lost to a crash. It MUST run before the daemon serves, and after
// the ownership coordinator is registered so the fence (L2.8) governs replay.
//
// meta.Write/Truncate are idempotent (ON CONFLICT on the slice_id natural key;
// truncate is set-length), so re-applying an already-committed record is a
// harmless no-op. Records that are no longer applicable are SKIPPED, not forced:
//   - ENOENT/EINVAL: the target was deleted or is no longer a regular file.
//   - ESTALE (multi-node): the scope was taken over by a peer that holds a newer
//     generation — the conservative fenced-replay rule (docs/L2.8 §9): we do not
//     resurrect a write over a peer's authoritative state.
//
// Only a genuine I/O error (EIO) aborts recovery. On success the WAL is
// checkpointed so recovered segments are dropped.
func ReplayWAL(ctx context.Context, w *wal.WAL, e *meta.Engine) (ReplayResult, error) {
	var res ReplayResult
	err := w.Replay(func(rec wal.Record) error {
		var st syscall.Errno
		switch rec.Op {
		case wal.OpWrite:
			st = e.Write(ctx, meta.Ino(rec.Ino), rec.Indx, rec.Off,
				meta.Slice{Id: rec.SliceID, Size: rec.Size, Off: rec.SliceOff, Len: rec.SliceLen})
		case wal.OpTruncate:
			size := int64(uint64(rec.Indx)<<32 | uint64(rec.Off))
			st = e.Truncate(ctx, meta.Ino(rec.Ino), size)
		default:
			return fmt.Errorf("wal replay: unknown op %d (seq %d)", rec.Op, rec.Seq)
		}
		switch st {
		case 0:
			res.Applied++
		case syscall.ENOENT, syscall.EINVAL, syscall.ESTALE:
			res.Skipped++
		default:
			return fmt.Errorf("wal replay: ino %d seq %d: errno %v", rec.Ino, rec.Seq, st)
		}
		return nil
	})
	if err != nil {
		return res, err
	}
	// Everything logged is now durable in SQL: drop recovered segments.
	if err := w.Checkpoint(w.LastSeq()); err != nil {
		return res, fmt.Errorf("wal replay: checkpoint: %w", err)
	}
	return res, nil
}

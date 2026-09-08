// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// Checkpointer is the minimal interface a sandbox-like type must satisfy for
// CheckpointAndStore. Both *gvisor.gvisorSandbox and the reaper's wrapper
// satisfy it via the existing provider.Sandbox interface.
type Checkpointer interface {
	Checkpoint(ctx context.Context, w io.Writer) error
	SnapshotMetadata() map[string]string
}

// LeaveRunningCheckpointer is the periodic-snapshot variant of Checkpointer.
// It checkpoints the sandbox without stopping it, so the sandbox keeps serving
// traffic during the snapshot. Used by PeriodicSnapshotter.
type LeaveRunningCheckpointer interface {
	CheckpointLeaveRunning(ctx context.Context, w io.Writer) error
	SnapshotMetadata() map[string]string
}

// CheckpointAndStore streams a sandbox's checkpoint into the store,
// automatically merging the sandbox's runtime-specific metadata
// (access_token, sandbox_ip, etc.) into the stored SnapshotMetadata.Tags.
// Tags already present on baseMeta win over sandbox-supplied tags.
//
// On any error, the store may have a partial blob; the helper does NOT try to
// roll back. Callers that need cleanup-on-error should call store.Delete on
// the returned (zero) version. Use this helper from anywhere that needs
// snapshot semantics; do not assemble the pipe + Put + merge dance by hand.
func CheckpointAndStore(
	ctx context.Context,
	cp Checkpointer,
	store SnapshotStore,
	key SnapshotKey,
	baseMeta SnapshotMetadata,
) (SnapshotMetadata, error) {
	return CheckpointAndStoreFunc(ctx, cp.Checkpoint, cp.SnapshotMetadata(), store, key, baseMeta)
}

// CheckpointAndStoreLeaveRunning is the periodic-snapshot sibling of
// CheckpointAndStore. It calls cp.CheckpointLeaveRunning instead of
// cp.Checkpoint so the sandbox keeps serving traffic during the snapshot.
//
// All metadata-merge, pipe, and error-propagation semantics are identical
// to CheckpointAndStore. The reaper uses CheckpointAndStore (stop-then-store);
// PeriodicSnapshotter uses this helper.
func CheckpointAndStoreLeaveRunning(
	ctx context.Context,
	cp LeaveRunningCheckpointer,
	store SnapshotStore,
	key SnapshotKey,
	baseMeta SnapshotMetadata,
) (SnapshotMetadata, error) {
	return CheckpointAndStoreFunc(ctx, cp.CheckpointLeaveRunning, cp.SnapshotMetadata(), store, key, baseMeta)
}

// CheckpointAndStoreFunc is the underlying pipe + merge + Put primitive that
// powers CheckpointAndStore and CheckpointAndStoreLeaveRunning. It lets
// orchestrators build their own checkpoint closure (e.g. CheckpointWithOpts
// for incremental snapshots) while reusing the metadata-merge and
// error-propagation plumbing.
//
// runCheckpoint writes the snapshot bytes to the provided writer. It runs
// synchronously on the caller's goroutine; meanwhile Put runs in a background
// goroutine draining the pipe's read side. Both sides see CloseWithError on
// the other's failure so neither side can leak waiting on a closed pipe.
//
// sandboxMeta is the sandbox-supplied tag bag (Checkpointer.SnapshotMetadata).
// Tags already present on baseMeta win over sandboxMeta on key conflict —
// the same precedence the previous in-line implementation used.
//
// Errors are wrapped identically to CheckpointAndStore's previous behaviour
// so callers see the same error strings:
//
//   - ErrSnapshotUnsupported is returned unwrapped (callers use errors.Is).
//   - Any other checkpoint error is wrapped with the "checkpoint: " prefix.
//   - Any Put error is wrapped with the "snapshot store put: " prefix.
//
// To preserve the previous "checkpoint-leave-running: " wrap, callers that
// need a custom error prefix should wrap the returned error themselves; the
// existing CheckpointAndStoreLeaveRunning has historically used the plain
// "checkpoint: " prefix once it routes through this helper (the unify cost
// is a tiny consistency win — the message is rarely consumed programmatically).
func CheckpointAndStoreFunc(
	ctx context.Context,
	runCheckpoint func(ctx context.Context, w io.Writer) error,
	sandboxMeta map[string]string,
	store SnapshotStore,
	key SnapshotKey,
	baseMeta SnapshotMetadata,
) (SnapshotMetadata, error) {
	// Build merged tags: start with baseMeta.Tags, then add sandbox-supplied
	// keys that are not already present (base wins on conflict).
	merged := make(map[string]string, len(baseMeta.Tags))
	for k, v := range baseMeta.Tags {
		merged[k] = v
	}
	for k, v := range sandboxMeta {
		if _, exists := merged[k]; !exists {
			merged[k] = v
		}
	}
	meta := baseMeta
	meta.Tags = merged

	pr, pw := io.Pipe()

	// Run Put in a goroutine so the checkpoint closure can stream into the
	// pipe while Put drains the read side concurrently.
	putErrCh := make(chan putResult, 1)
	go func() {
		stored, err := store.Put(ctx, key, meta, pr)
		pr.CloseWithError(err) // signal the checkpoint writer side on error
		putErrCh <- putResult{meta: stored, err: err}
	}()

	checkpointErr := runCheckpoint(ctx, pw)
	// Always close the write side so Put's Read sees EOF (or the error).
	pw.CloseWithError(checkpointErr)

	res := <-putErrCh

	if errors.Is(checkpointErr, ErrSnapshotUnsupported) {
		return SnapshotMetadata{}, ErrSnapshotUnsupported
	}
	if checkpointErr != nil {
		return SnapshotMetadata{}, fmt.Errorf("checkpoint: %w", checkpointErr)
	}
	if res.err != nil {
		return SnapshotMetadata{}, fmt.Errorf("snapshot store put: %w", res.err)
	}
	return res.meta, nil
}

type putResult struct {
	meta SnapshotMetadata
	err  error
}

// PruneByAge deletes any version of key whose CreatedAt is older than
// maxAge before now. The most recent version is *always* retained even if
// older than maxAge — so an inactive sandbox does not lose its most recent
// restore point to time-based pruning. Returns the number of deletions and
// the first error encountered; remaining deletes are still attempted so a
// single bad version cannot block forward progress.
//
// maxAge <= 0 is a no-op (returns 0, nil) so callers can wire the helper
// unconditionally and disable it by setting the env knob to 0.
func PruneByAge(ctx context.Context, store SnapshotStore, key SnapshotKey, maxAge time.Duration) (int, error) {
	if maxAge <= 0 {
		return 0, nil
	}
	metas, err := store.List(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("snapshot: prune-by-age list: %w", err)
	}
	if len(metas) <= 1 {
		// Single (or no) version — nothing to age out without losing the
		// only restore point. Documented invariant.
		return 0, nil
	}
	cutoff := time.Now().Add(-maxAge)
	var firstErr error
	deleted := 0
	// Skip metas[0] (newest, always kept). Walk the rest; delete those whose
	// CreatedAt precedes the cutoff.
	for _, m := range metas[1:] {
		if !m.CreatedAt.Before(cutoff) {
			continue
		}
		if derr := store.Delete(ctx, key, m.Version); derr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("snapshot: prune-by-age delete %q: %w", m.Version, derr)
			}
			continue
		}
		deleted++
	}
	return deleted, firstErr
}

// PruneOldVersions keeps the keepLatest most-recent versions for the given
// snapshot key and deletes the rest. Versions are ordered newest-first by
// SnapshotStore.List, so the slice tail represents the oldest versions.
//
// Returns the number of versions successfully deleted and the first error
// encountered (if any). On error the helper still attempts the remaining
// deletes so a single bad version does not block prune from making progress.
//
// keepLatest <= 0 is a no-op (returns 0, nil) — this lets callers wire the
// helper in unconditionally and disable pruning by setting the env knob to 0.
func PruneOldVersions(ctx context.Context, store SnapshotStore, key SnapshotKey, keepLatest int) (int, error) {
	if keepLatest <= 0 {
		return 0, nil
	}
	metas, err := store.List(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("snapshot: prune list: %w", err)
	}
	if len(metas) <= keepLatest {
		return 0, nil
	}
	var firstErr error
	deleted := 0
	for _, m := range metas[keepLatest:] {
		if derr := store.Delete(ctx, key, m.Version); derr != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("snapshot: prune delete %q: %w", m.Version, derr)
			}
			continue
		}
		deleted++
	}
	return deleted, firstErr
}

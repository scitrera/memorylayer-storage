// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package pgindex

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// Postgres implementation of snapshot.PackAssocStore: context-scoped pack
// associations plus the reclamation lifecycle mark.
//
// Two tables and one column carry it (see Schema):
//
//   - pack_assoc(domain, pack_hash, context) — the association rows. A pack is
//     live while any row names it, so the GC live set is a query.
//   - packs.state — the lifecycle mark ('stored' | 'obliterating' |
//     'obliterated'). A pack with no packs row reads as 'stored', which is what
//     makes this a pure forward migration.
//   - pack_assoc_backfill(domain) — the marker that says a domain's associations
//     have been populated from pre-existing manifests.
//
// The two contracts callers depend on are enforced here: Lookup/LookupBatch join
// against packs.state so a reclaiming pack is never handed out (see pgindex.go),
// and Associate refuses a batch touching a non-stored pack.

var _ snapshot.PackAssocStore = (*Store)(nil)

// Associate idempotently records (pack, context) rows. The whole batch is
// validated and written in ONE transaction: if any named pack is not in the
// stored state the transaction rolls back and nothing is recorded, so the caller
// may treat its Put as cleanly failed and retry.
func (s *Store) Associate(ctx context.Context, domain string, assocs []snapshot.PackAssociation) (rerr error) {
	if len(assocs) == 0 {
		return nil
	}
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "associate", start, rerr) }()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		rerr = fmt.Errorf("pgindex: associate: begin: %w", err)
		return rerr
	}
	defer func() { _ = tx.Rollback() }()

	// Reject up front if any target pack is being reclaimed. Locking the packs
	// rows FOR SHARE keeps a concurrent MarkObliterating from moving one out of
	// 'stored' between this check and the insert.
	hashes := make([]string, 0, len(assocs))
	seen := make(map[string]struct{}, len(assocs))
	for _, a := range assocs {
		if _, dup := seen[a.PackHash]; dup {
			continue
		}
		seen[a.PackHash] = struct{}{}
		hashes = append(hashes, a.PackHash)
	}
	placeholders, args := inClause(domain, hashes)
	rows, err := tx.QueryContext(ctx,
		`SELECT pack_hash, state FROM packs
		  WHERE domain=$1 AND pack_hash IN (`+placeholders+`)
		    AND state <> 'stored'
		  FOR SHARE`, args...)
	if err != nil {
		rerr = fmt.Errorf("pgindex: associate: check pack state: %w", err)
		return rerr
	}
	var badPack, badState string
	found := false
	for rows.Next() {
		if err := rows.Scan(&badPack, &badState); err != nil {
			_ = rows.Close()
			rerr = fmt.Errorf("pgindex: associate: scan pack state: %w", err)
			return rerr
		}
		found = true
		break
	}
	if cerr := rows.Close(); cerr != nil {
		rerr = fmt.Errorf("pgindex: associate: close state rows: %w", cerr)
		return rerr
	}
	if found {
		rerr = fmt.Errorf("pgindex: associate pack %s in domain %s (state %s): %w",
			badPack, domain, badState, snapshot.ErrPackObliterating)
		return rerr
	}

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO pack_assoc (domain, pack_hash, context) VALUES ($1, $2, $3)
		 ON CONFLICT (domain, pack_hash, context) DO NOTHING`)
	if err != nil {
		rerr = fmt.Errorf("pgindex: associate: prepare: %w", err)
		return rerr
	}
	defer func() { _ = stmt.Close() }()
	for _, a := range assocs {
		if _, err := stmt.ExecContext(ctx, domain, a.PackHash, a.Context); err != nil {
			rerr = fmt.Errorf("pgindex: associate pack %s: %w", a.PackHash, err)
			return rerr
		}
	}
	if err := tx.Commit(); err != nil {
		rerr = fmt.Errorf("pgindex: associate: commit: %w", err)
		return rerr
	}
	return nil
}

// DropContext removes every association held by packContext and returns the
// distinct packs it referenced — the reclamation candidates. One statement, using
// RETURNING so the candidate list costs no extra round trip.
func (s *Store) DropContext(ctx context.Context, domain, packContext string) (_ []string, rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "drop_context", start, rerr) }()

	rows, err := s.db.QueryContext(ctx,
		`DELETE FROM pack_assoc WHERE domain=$1 AND context=$2 RETURNING pack_hash`,
		domain, packContext)
	if err != nil {
		rerr = fmt.Errorf("pgindex: drop context %q: %w", packContext, err)
		return nil, rerr
	}
	defer func() { _ = rows.Close() }()

	seen := make(map[string]struct{})
	var out []string
	for rows.Next() {
		var packHash string
		if err := rows.Scan(&packHash); err != nil {
			rerr = fmt.Errorf("pgindex: drop context: scan: %w", err)
			return nil, rerr
		}
		if _, dup := seen[packHash]; dup {
			continue
		}
		seen[packHash] = struct{}{}
		out = append(out, packHash)
	}
	if err := rows.Err(); err != nil {
		rerr = fmt.Errorf("pgindex: drop context: rows: %w", err)
		return nil, rerr
	}
	return out, nil
}

// HasAssociations reports, per pack, whether any association remains.
func (s *Store) HasAssociations(ctx context.Context, domain string, packHashes []string) (_ map[string]bool, rerr error) {
	out := make(map[string]bool, len(packHashes))
	if len(packHashes) == 0 {
		return out, nil
	}
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "has_associations", start, rerr) }()

	for _, h := range packHashes {
		out[h] = false
	}
	placeholders, args := inClause(domain, packHashes)
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT pack_hash FROM pack_assoc
		  WHERE domain=$1 AND pack_hash IN (`+placeholders+`)`, args...)
	if err != nil {
		rerr = fmt.Errorf("pgindex: has associations: %w", err)
		return nil, rerr
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var packHash string
		if err := rows.Scan(&packHash); err != nil {
			rerr = fmt.Errorf("pgindex: has associations: scan: %w", err)
			return nil, rerr
		}
		out[packHash] = true
	}
	if err := rows.Err(); err != nil {
		rerr = fmt.Errorf("pgindex: has associations: rows: %w", err)
		return nil, rerr
	}
	return out, nil
}

// WalkLivePacks streams every pack in domain holding at least one association.
// This is the GC live set: one indexed query instead of a walk over every
// manifest in the store.
func (s *Store) WalkLivePacks(ctx context.Context, domain string, yield func(packHash string) error) (rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "walk_live_packs", start, rerr) }()

	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT pack_hash FROM pack_assoc WHERE domain=$1`, domain)
	if err != nil {
		rerr = fmt.Errorf("pgindex: walk live packs: %w", err)
		return rerr
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var packHash string
		if err := rows.Scan(&packHash); err != nil {
			rerr = fmt.Errorf("pgindex: walk live packs: scan: %w", err)
			return rerr
		}
		if err := yield(packHash); err != nil {
			rerr = err
			return rerr
		}
	}
	if err := rows.Err(); err != nil {
		rerr = fmt.Errorf("pgindex: walk live packs: rows: %w", err)
		return rerr
	}
	return nil
}

// MarkObliterating moves a pack from stored to obliterating, creating the packs
// row if the pack predates storage accounting. It reports whether it won: false
// means another reclaimer already owns the pack (or it is already obliterated).
func (s *Store) MarkObliterating(ctx context.Context, domain, packHash string) (_ bool, rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "mark_obliterating", start, rerr) }()

	// The conflict target may or may not exist. DO UPDATE ... WHERE makes the
	// transition conditional on the CURRENT state being 'stored', so two
	// reclaimers racing the same pack produce exactly one winner.
	var marked string
	err := s.db.QueryRowContext(ctx,
		`INSERT INTO packs (domain, pack_hash, compressed_pack_bytes, state)
		 VALUES ($1, $2, 0, 'obliterating')
		 ON CONFLICT (domain, pack_hash)
		 DO UPDATE SET state = 'obliterating'
		 WHERE packs.state = 'stored'
		 RETURNING pack_hash`, domain, packHash).Scan(&marked)
	if err == sql.ErrNoRows {
		return false, nil // conflict row existed but was not 'stored'
	}
	if err != nil {
		rerr = fmt.Errorf("pgindex: mark obliterating %s: %w", packHash, err)
		return false, rerr
	}
	return true, nil
}

// ReleaseMark returns a pack from obliterating to stored.
func (s *Store) ReleaseMark(ctx context.Context, domain, packHash string) (rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "release_mark", start, rerr) }()

	if _, err := s.db.ExecContext(ctx,
		`UPDATE packs SET state='stored'
		  WHERE domain=$1 AND pack_hash=$2 AND state='obliterating'`,
		domain, packHash); err != nil {
		rerr = fmt.Errorf("pgindex: release mark %s: %w", packHash, err)
		return rerr
	}
	return nil
}

// CommitObliterated moves a pack from obliterating to obliterated, but only if it
// still holds no associations. The re-check and the transition are ONE statement,
// which is what removes the gap between deciding a pack is dead and deleting its
// bytes: an association that lands first makes this a no-op and the caller spares
// the pack.
func (s *Store) CommitObliterated(ctx context.Context, domain, packHash string) (_ bool, rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "commit_obliterated", start, rerr) }()

	var committed string
	err := s.db.QueryRowContext(ctx,
		`UPDATE packs SET state='obliterated'
		  WHERE domain=$1 AND pack_hash=$2 AND state='obliterating'
		    AND NOT EXISTS (
		        SELECT 1 FROM pack_assoc
		         WHERE pack_assoc.domain=packs.domain AND pack_assoc.pack_hash=packs.pack_hash)
		  RETURNING pack_hash`, domain, packHash).Scan(&committed)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		rerr = fmt.Errorf("pgindex: commit obliterated %s: %w", packHash, err)
		return false, rerr
	}
	return true, nil
}

// MarkBackfilled records that a domain's associations were populated from its
// existing manifests, unlocking the association-derived GC live set.
func (s *Store) MarkBackfilled(ctx context.Context, domain string) (rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "mark_backfilled", start, rerr) }()

	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO pack_assoc_backfill (domain) VALUES ($1)
		 ON CONFLICT (domain) DO UPDATE SET completed_at = now()`, domain); err != nil {
		rerr = fmt.Errorf("pgindex: mark backfilled %q: %w", domain, err)
		return rerr
	}
	return nil
}

// IsBackfilled reports whether a domain has been association-backfilled.
func (s *Store) IsBackfilled(ctx context.Context, domain string) (_ bool, rerr error) {
	start := time.Now()
	defer func() { _ = s.dp.RecordPG(ctx, "is_backfilled", start, rerr) }()

	var one int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM pack_assoc_backfill WHERE domain=$1`, domain).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		rerr = fmt.Errorf("pgindex: is backfilled %q: %w", domain, err)
		return false, rerr
	}
	return true, nil
}

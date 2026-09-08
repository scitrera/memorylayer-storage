// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package meta

import (
	"context"
	"database/sql"
	"fmt"
)

// Schema is the mlfs metadata DDL, idempotent.
//
//   - node / edge / symlink: the directory tree.
//   - slice_ref: the per-chunk slice list, with one row per write.
//     Copy-on-write copies slice rows; garbage collection is
//     mark-and-sweep over this table. No chunk_refcount table (by design).
//   - flock / plock: POSIX locks (table created in L2.1; lock ops land later).
//   - xattr: extended attributes ((inode,name)→value); also stores POSIX ACL
//     blobs (system.posix_acl_*). The kernel enforces these (mount EnableAcl /
//     CAP_POSIX_ACL, L2.8.7), reading/writing them through this generic store.
//   - ownership: per-scope write-affinity lease with a `generation` fence.
//   - fs_changelog: durable, ordered (BIGSERIAL lsn) change log; every write
//     appends here in the same transaction.
//   - mlfs_counter: monotonic counters (inode allocation).
const Schema = `
CREATE TABLE IF NOT EXISTS node (
    inode      BIGINT PRIMARY KEY,
    type       SMALLINT NOT NULL,
    flags      SMALLINT NOT NULL DEFAULT 0,
    mode       INTEGER  NOT NULL,
    uid        BIGINT   NOT NULL DEFAULT 0,
    gid        BIGINT   NOT NULL DEFAULT 0,
    atime      BIGINT   NOT NULL DEFAULT 0,
    mtime      BIGINT   NOT NULL DEFAULT 0,
    ctime      BIGINT   NOT NULL DEFAULT 0,
    atimensec  INTEGER  NOT NULL DEFAULT 0,
    mtimensec  INTEGER  NOT NULL DEFAULT 0,
    ctimensec  INTEGER  NOT NULL DEFAULT 0,
    nlink      BIGINT   NOT NULL DEFAULT 1,
    length     BIGINT   NOT NULL DEFAULT 0,
    rdev       BIGINT   NOT NULL DEFAULT 0,
    parent     BIGINT   NOT NULL DEFAULT 0,
    -- store_class is the file's compression class (0 = default/policy-decides,
    -- 1 = uncompressed for model/tensor data). Set at create by file type; read
    -- on every write to stamp the slice so the codec follows the file.
    store_class SMALLINT NOT NULL DEFAULT 0,
    -- content_hash is the whole-file sha256 as lowercase hex, no algorithm prefix
    -- (empty until the file is first flushed). It matches blobgw's
    -- ObjectInfo.ContentHash encoding byte-for-byte, so a bridged blob_ref's
    -- content_hash equals this value without re-hashing; it also closes the
    -- integrity gap of storing no whole-file digest. Written on flush/close.
    content_hash TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS edge (
    parent BIGINT NOT NULL,
    name   BYTEA  NOT NULL,
    inode  BIGINT NOT NULL,
    type   SMALLINT NOT NULL,
    PRIMARY KEY (parent, name)
);
CREATE INDEX IF NOT EXISTS edge_by_inode ON edge (inode);

CREATE TABLE IF NOT EXISTS symlink (
    inode  BIGINT PRIMARY KEY,
    target BYTEA  NOT NULL
);

CREATE TABLE IF NOT EXISTS slice_ref (
    id       BIGSERIAL PRIMARY KEY,
    inode    BIGINT  NOT NULL,
    indx     INTEGER NOT NULL,
    pos      INTEGER NOT NULL,   -- byte offset within the chunk
    slice_id BIGINT  NOT NULL,   -- casstore chunk handle (L2.2); opaque here
    size     INTEGER NOT NULL,   -- full source slice size
    soff     INTEGER NOT NULL,   -- read offset into the source slice
    slen     INTEGER NOT NULL,   -- bytes this slice contributes
    -- (inode, slice_id) is the natural key: a write produces one row per
    -- (file, slice), so re-applying it (WAL replay, L2.4) is idempotent.
    -- Keyed by inode rather than slice_id alone so copy-on-write (Clone) can
    -- reference the SAME physical slice from a second file.
    UNIQUE (inode, slice_id)
);
CREATE INDEX IF NOT EXISTS slice_ref_by_chunk ON slice_ref (inode, indx, id);

CREATE TABLE IF NOT EXISTS flock (
    id    BIGSERIAL PRIMARY KEY,
    inode BIGINT NOT NULL,
    sid   BIGINT NOT NULL,
    owner BIGINT NOT NULL,
    ltype SMALLINT NOT NULL,
    UNIQUE (inode, sid, owner)
);

CREATE TABLE IF NOT EXISTS plock (
    id      BIGSERIAL PRIMARY KEY,
    inode   BIGINT NOT NULL,
    sid     BIGINT NOT NULL,
    owner   BIGINT NOT NULL,
    records BYTEA  NOT NULL,
    UNIQUE (inode, sid, owner)
);

CREATE TABLE IF NOT EXISTS xattr (
    inode BIGINT NOT NULL,
    name  BYTEA  NOT NULL,
    value BYTEA  NOT NULL,
    PRIMARY KEY (inode, name)
);

CREATE TABLE IF NOT EXISTS ownership (
    scope_key             TEXT PRIMARY KEY,
    owner_node            TEXT   NOT NULL DEFAULT '',
    lease_expires_unix_ms BIGINT NOT NULL DEFAULT 0,
    generation            BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fs_changelog (
    lsn     BIGSERIAL PRIMARY KEY,
    ino     BIGINT NOT NULL,
    op      SMALLINT NOT NULL,
    payload JSONB,
    ts      BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS mlfs_counter (
    name  TEXT PRIMARY KEY,
    value BIGINT NOT NULL
);
`

// changelog op codes (compact, stable).
const (
	opMknod uint8 = iota + 1
	opMkdir
	opUnlink
	opRmdir
	opRename
	opSetattr
	opWrite
	opSymlink
	opLink
)

// Exported mirror of the changelog op codes: the fs_changelog wire contract,
// consumed by the change feed (L2.8.5) to decide what kernel cache to
// invalidate. Kept as aliases so internal call sites stay unchanged.
const (
	ChangeMknod   = opMknod
	ChangeMkdir   = opMkdir
	ChangeUnlink  = opUnlink
	ChangeRmdir   = opRmdir
	ChangeRename  = opRename
	ChangeSetattr = opSetattr
	ChangeWrite   = opWrite
	ChangeSymlink = opSymlink
	ChangeLink    = opLink
)

// Migrate applies the schema and seeds the root inode + inode counter. Safe to
// call repeatedly.
func (e *Engine) Migrate(ctx context.Context) error {
	if _, err := e.db.ExecContext(ctx, Schema); err != nil {
		return fmt.Errorf("mlfs meta: migrate schema: %w", err)
	}
	// Additive column for pre-existing node tables (CREATE TABLE IF NOT EXISTS
	// won't add it). Idempotent.
	if _, err := e.db.ExecContext(ctx,
		`ALTER TABLE node ADD COLUMN IF NOT EXISTS store_class SMALLINT NOT NULL DEFAULT 0`); err != nil {
		return fmt.Errorf("mlfs meta: migrate node.store_class: %w", err)
	}
	if _, err := e.db.ExecContext(ctx,
		`ALTER TABLE node ADD COLUMN IF NOT EXISTS content_hash TEXT NOT NULL DEFAULT ''`); err != nil {
		return fmt.Errorf("mlfs meta: migrate node.content_hash: %w", err)
	}
	return e.bootstrap(ctx)
}

// bootstrap seeds inode 1 (root dir) and the inode counter if absent.
func (e *Engine) bootstrap(ctx context.Context) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	now := e.now().Unix()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO node (inode, type, mode, uid, gid, atime, mtime, ctime, nlink, parent)
		 VALUES ($1, $2, $3, 0, 0, $4, $4, $4, 2, $1)
		 ON CONFLICT (inode) DO NOTHING`,
		RootInode, TypeDirectory, 0o777, now); err != nil {
		return fmt.Errorf("mlfs meta: seed root: %w", err)
	}
	// The inode counter tracks the 48-bit low part; start above the reserved
	// root inode.
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO mlfs_counter (name, value) VALUES ('nextinode', $1)
		 ON CONFLICT (name) DO NOTHING`, int64(RootInode)+1); err != nil {
		return fmt.Errorf("mlfs meta: seed inode counter: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO mlfs_counter (name, value) VALUES ('nextslice', 1)
		 ON CONFLICT (name) DO NOTHING`); err != nil {
		return fmt.Errorf("mlfs meta: seed slice counter: %w", err)
	}
	return tx.Commit()
}

// withTx runs fn in a transaction, rolling back on error.
func (e *Engine) withTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

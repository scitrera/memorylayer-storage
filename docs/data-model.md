# Data model

The persistent data model across the platform: the mlfs PostgreSQL filesystem
schema, the casstore on-disk manifest format and blob ID schemes, and blobgw's
Postgres index tables. Every table/column/field below is taken from the code
(`mlfs/internal/meta/schema.go`, `casstore/snapshot/store_chunked.go`,
`casstore/snapshot/store_compress.go`, `blobgw/pgindex/pgindex.go`,
`blobgw-edge/edge/pgpolicy.go`).

For design rationale see [architecture.md](architecture.md); for limitations see
[testing notes](testing.md).

---

## mlfs metadata schema (PostgreSQL)

`mlfs/internal/meta/schema.go`. The DDL is idempotent; `Migrate` applies it and
seeds the root inode + counters. The directory-tree tables
(node/edge/symlink/slice_ref/flock/plock/xattr) describe the filesystem; `ownership`
and `fs_changelog` are mlfs additions.

### Inode number layout

`Ino` is a 64-bit inode (`meta/types.go`, `meta/engine.go`):

```
 63          48 47                              0
┌──────────────┬─────────────────────────────────┐
│ 16-bit prefix│        48-bit counter            │
└──────────────┴─────────────────────────────────┘
   region<<8 | session         per-node, batched from mlfs_counter
```

The 16-bit prefix is `uint16(region)<<8 | random-8-bit-session` — an 8-bit
region id (`-region`, 0-255) in the high byte and a random per-mount session id
in the low byte, so concurrent nodes draw distinct prefixes. `RootInode` is the
fixed value `1`. `inoLowMask` is `(1<<48)-1`.

### `node` — one row per inode (POSIX attributes)

| Column | Type | Purpose |
|---|---|---|
| `inode` | BIGINT PK | the 64-bit inode number |
| `type` | SMALLINT | file type (1 file, 2 dir, 3 symlink, 4 fifo, 5 blockdev, 6 chardev, 7 socket) |
| `flags` | SMALLINT | inode flags (default 0) |
| `mode` | INTEGER | POSIX mode bits |
| `uid` / `gid` | BIGINT | owner uid / gid |
| `atime`/`mtime`/`ctime` | BIGINT | timestamps (seconds) |
| `atimensec`/`mtimensec`/`ctimensec` | INTEGER | nanosecond components |
| `nlink` | BIGINT | hard-link count |
| `length` | BIGINT | file size in bytes |
| `rdev` | BIGINT | device id (block/char devices) |
| `parent` | BIGINT | parent inode (root is its own parent) |

### `edge` — directory entries (the name→inode tree)

| Column | Type | Purpose |
|---|---|---|
| `parent` | BIGINT | directory inode |
| `name` | BYTEA | entry name (bytes, not text) |
| `inode` | BIGINT | target inode |
| `type` | SMALLINT | target type (denormalized for fast readdir) |

PK `(parent, name)`; index `edge_by_inode (inode)`. **An inode with no inbound
`edge` row is unreachable** — that is the open-unlinked / zombie signal (see
invariants).

### `symlink` — symlink targets

`inode` BIGINT PK, `target` BYTEA. One row per symlink inode.

### `slice_ref` — the per-write slice list (the heart of the data model)

One **row per write**. Copy-on-write is "copy
the slice rows"; GC is mark-and-sweep over this table.

| Column | Type | Purpose |
|---|---|---|
| `id` | BIGSERIAL PK | row id |
| `inode` | BIGINT | the file this slice belongs to |
| `indx` | INTEGER | chunk index within the file (chunk = 64 MiB, `ChunkSize`) |
| `pos` | INTEGER | byte offset within the chunk |
| `slice_id` | BIGINT | casstore chunk handle (opaque here; names the slice's bytes) |
| `size` | INTEGER | full source slice size |
| `soff` | INTEGER | read offset into the source slice |
| `slen` | INTEGER | bytes this slice contributes |

`UNIQUE (inode, slice_id)` is the natural key: a write produces one row per
(file, slice), so re-applying it (WAL replay) is idempotent. Keyed by `inode`
(not `slice_id` alone) so copy-on-write can reference the **same physical slice**
from a second file. Index `slice_ref_by_chunk (inode, indx, id)` orders slices
for read-overlay resolution (newer `id` wins).

### `flock` / `plock` — POSIX locks

`flock` (whole-file BSD locks): `id` BIGSERIAL PK, `inode`, `sid` (session id),
`owner`, `ltype` SMALLINT; `UNIQUE (inode, sid, owner)`.
`plock` (byte-range fcntl locks): `id` BIGSERIAL PK, `inode`, `sid`, `owner`,
`records` BYTEA (the serialized range list); `UNIQUE (inode, sid, owner)`.
Lock state never survives a mount teardown; the daemon clears all rows on start
and `mlfs-admin fsck -reap-locks` clears them offline (single-node).

### `xattr` — extended attributes

`inode` BIGINT, `name` BYTEA, `value` BYTEA; PK `(inode, name)`. Also stores
POSIX ACL blobs (`system.posix_acl_*`) as **passthrough** — they are persisted
but not enforced in permission checks (enforcement is L2.8).

### `ownership` — per-scope write-affinity lease

| Column | Type | Purpose |
|---|---|---|
| `scope_key` | TEXT PK | the ownership scope |
| `owner_node` | TEXT | current owner node id |
| `lease_expires_unix_ms` | BIGINT | lease expiry |
| `generation` | BIGINT | the fencing token (row-version) |

`generation` is the fence for future multi-node write-affinity; single-node
today.

### `fs_changelog` — durable, ordered change log

| Column | Type | Purpose |
|---|---|---|
| `lsn` | BIGSERIAL PK | monotonic log sequence number |
| `ino` | BIGINT | affected inode |
| `op` | SMALLINT | op code (1 mknod, 2 mkdir, 3 unlink, 4 rmdir, 5 rename, 6 setattr, 7 write, 8 symlink, 9 link) |
| `payload` | JSONB | op-specific detail |
| `ts` | BIGINT | timestamp |

Every metadata write appends here in the **same transaction** as the mutation
(the reason the engine uses raw SQL, not an ORM). Trimmed by age/LSN/count
during maintenance (`-changelog-retention`).

### `mlfs_counter` — monotonic counters

`name` TEXT PK, `value` BIGINT. Seeds: `nextinode` (the 48-bit low part, started
above `RootInode`) and `nextslice`. Nodes reserve batches of 1024 per
round-trip.

### Key invariants

- **slice_ref → chunk liveness.** There is no `chunk_refcount` table. The live
  slice set is `SELECT DISTINCT slice_id FROM slice_ref`. A `slice_id` becomes an
  orphan once its last referencing inode is gone; its casstore manifest (then
  its chunk/pack blobs) is reclaimed by GC once it is older than the safety
  window. Because copy-on-write shares `slice_id`s across inodes, a slice stays
  live until **every** reference is gone.
- **Open-unlinked / zombie inodes.** When `nlink` hits 0 while a file is still
  open, the node is sustained in memory and deleted on the last Close
  (per-mount, single-node). If the holder crashes first, the node row survives
  with no inbound `edge` and no live handle: a "zombie". `mlfs-admin fsck` reaps
  node rows that have `nlink=0`/no inbound edge (never `RootInode`), clearing
  `node`/`symlink`/`slice_ref`/`xattr`, which then lets GC reclaim the freed
  slices. Offline/single-node only.
- **Idempotent replay.** `UNIQUE (inode, slice_id)` makes WAL replay of a slice
  write idempotent.
- **GC↔write race window.** `fileio` writes a slice's casstore manifest *before*
  committing its `slice_ref` row, so a just-written slice momentarily looks like
  an orphan; the GC safety window prevents reclaiming it.

---

## casstore manifest format & blob IDs

`casstore/snapshot/store_chunked.go`. Each stored object is split into
content-defined chunks; a small JSON **manifest** (the chunk list in
original-stream order) is written as the object's blob, and the chunk bytes live
in pack blobs.

### Manifest (JSON, versioned)

`chunkManifest`:

| Field (JSON) | Type | Purpose |
|---|---|---|
| `format` | string | `"sandbox-chunked-v1"` or `"sandbox-chunked-v2"` |
| `total_size` | int64 | original blob size, before chunking |
| `tenant` | string | the dedup-domain prefix all chunk/pack blob IDs use |
| `chunks` | []chunkRef | every chunk, **in original-stream order** |

`chunkRef`:

| Field (JSON) | Type | v1 | v2 |
|---|---|---|---|
| `h` | string | sha256 hex of chunk content (set) | set |
| `p` | string | — | sha256 hex of the pack blob holding the chunk |
| `o` | int | — | byte offset of the chunk within the pack |
| `s` | int | chunk byte length (set) | set |

**Stream-order invariant.** `chunks` is ordered by original byte position, so
reassembly is a straight concatenation in slice order — Restore fetches each
referenced chunk/pack and streams the bytes back in `chunks` order.

### Blob ID schemes

- **v1 chunk blob:** `chunk-<domain>-<hash>` (the chunk lives at offset 0 of its
  own blob). Listing prefix `chunk-<domain>-`.
- **v2 pack blob:** `pack-<domain>-<packHash>` where `packHash` is the sha256 of
  the pack; the chunk is at byte `o` for length `s` inside it. Listing prefix
  `pack-<domain>-`.

`<domain>` is the dedup domain (`DedupDomain` / `tenant`). Distinct domains
never share blobs or index rows. v1 readers handle v2-absent fields; a v2
manifest can reference a v1 chunk by treating it as a pack-of-one. Packing is
controlled by `PackTargetBytes` (0 = default ~16 MiB; -1 = disable packing → v1).

### Pack compression codec header

`casstore/snapshot/store_compress.go`. Compression is applied to **pack blobs**
(not the whole object), so dedup keys on uncompressed chunk content. The codec
must be recoverable on read without a manifest change (a v2 manifest can
reference packs written by other Puts), so every pack this store writes carries a
**self-describing fixed-size header**:

```
┌────────────────────┬────────────┬──────────────────────┐
│ magic "CPK"+0x01 (4)│ codec (1)  │ codec-framed body …  │
└────────────────────┴────────────┴──────────────────────┘
```

- Magic = `{'C','P','K',1}` (4 bytes); header length = 5 bytes (magic + 1 codec
  byte).
- Codec byte: `0` none, `1` zstd, `2` gzip.
- A `none` codec still frames the header (uniform new packs) with a raw body.
- **Legacy packs** (written before pack compression existed) have no header —
  the reader detects a missing/short magic and returns the bytes untouched
  (backward compatible).
- The pack's identity (hash / blob ID) is computed over the **raw** bytes before
  compression, so compression never affects dedup or GC.

(Separately, the whole-blob `CompressingStore` records its algorithm in
`SnapshotMetadata.Tags["compression"]`; absence = uncompressed. That decorator
is distinct from the per-pack header above.)

---

## blobgw index tables (PostgreSQL)

`blobgw/pgindex/pgindex.go`. One `Store` implements both casstore's
`DedupStore` and the gateway's `RefStore`, so blobgw adds **no new datastore**.
The DDL is idempotent; `Migrate` applies it.

### `pack_manifest` — the chunk→pack dedup index (casstore `DedupStore`)

| Column | Type | Purpose |
|---|---|---|
| `domain` | TEXT | dedup domain |
| `chunk_hash` | TEXT | sha256 hex of the chunk content |
| `pack_hash` | TEXT | sha256 hex of the pack blob holding it |
| `offset_b` | BIGINT | byte offset of the chunk within the pack |
| `length_b` | BIGINT | chunk byte length |

PK `(domain, chunk_hash)`; index `pack_manifest_by_pack (domain, pack_hash)` (so
purging a reclaimed pack's rows is cheap). One row corresponds to a
`snapshot.ChunkLocation` / `PackRef`.

### `blob_ref` — the logical object index (gateway `RefStore`)

| Column | Type | Purpose |
|---|---|---|
| `domain` | TEXT | dedup + ref namespace |
| `ref` | TEXT | the logical object key (deterministic path) |
| `content_hash` | TEXT | whole-object content hash |
| `size_b` | BIGINT | object size |
| `content_type` | TEXT | MIME type |
| `version` | TEXT | object version |
| `pending` | BOOLEAN | true between MintRef and Finalize (stage-then-finalize) |
| `staging_key` | TEXT | the staged-upload key while pending |
| `created_at` | TIMESTAMPTZ | creation time |

PK `(domain, ref)`; index `blob_ref_by_prefix (domain, ref text_pattern_ops)`
for prefix-listing. `pending`/`staging_key` are set only between `MintRef` and
`Finalize` (the staging sweeper reclaims stale pending rows via
`ListStalePending`).

---

## blobgw-edge policy tables (PostgreSQL)

`blobgw-edge/edge/pgpolicy.go` (`-policy postgres`). `PgMigrate` applies the
idempotent DDL.

- **`edge_quota`** — per-tenant usage: `tenant` TEXT PK, `bytes_used` BIGINT,
  `object_count` BIGINT.
- **`edge_rate_bucket`** — per-key token bucket: `bucket_key` TEXT PK, `tokens`
  DOUBLE PRECISION, `last_ts` TIMESTAMPTZ.
- **`edge_revocation`** — the jti denylist: `jti` TEXT PK, `expires_at`
  TIMESTAMPTZ; index `edge_revocation_by_expiry (expires_at)`. Pruned on the GC
  cadence so revoked-but-expired tokens don't accumulate.

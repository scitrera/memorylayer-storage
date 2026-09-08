// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package meta implements the PostgreSQL filesystem metadata engine.
// Directory entries, file slices, locks, ownership leases and change records
// are updated transactionally. Each metadata write appends a change record
// in the same transaction, preserving the ordering observed by other nodes.
package meta

// Ino is a 64-bit inode number. mlfs partitions the space as a 16-bit
// region+session prefix and a 48-bit per-node counter (see inode.go).
type Ino uint64

// RootInode is the fixed inode of the filesystem root.
const RootInode Ino = 1

// ChunkSize is the logical chunk granularity: a file is a
// sequence of chunks, each addressed by index, each holding overlaid slices.
const ChunkSize = 1 << 26 // 64 MiB

// File types stored in filesystem metadata.
const (
	TypeFile      uint8 = 1
	TypeDirectory uint8 = 2
	TypeSymlink   uint8 = 3
	TypeFIFO      uint8 = 4
	TypeBlockDev  uint8 = 5
	TypeCharDev   uint8 = 6
	TypeSocket    uint8 = 7
)

// SetAttr mask bits select which Attr fields SetAttr applies.
const (
	SetMode  uint16 = 1 << 0
	SetUID   uint16 = 1 << 1
	SetGID   uint16 = 1 << 2
	SetSize  uint16 = 1 << 3
	SetAtime uint16 = 1 << 4
	SetMtime uint16 = 1 << 5
)

// Attr is a node's POSIX attributes.
type Attr struct {
	Typ       uint8
	Mode      uint16
	Uid       uint32
	Gid       uint32
	Atime     int64 // seconds
	Mtime     int64
	Ctime     int64
	Atimensec uint32
	Mtimensec uint32
	Ctimensec uint32
	Nlink     uint32
	Length    uint64
	Rdev      uint32
	Parent    Ino
	// StoreClass is the file's compression class (0 = default/policy-decides,
	// 1 = uncompressed for model/tensor data). Set at create by file type, read on
	// every write to stamp the slice so the codec follows the file. Mirrors
	// chunkstore.StoreClass numerically; meta stays decoupled from chunkstore (the
	// bridge maps between them).
	StoreClass uint8
	// ContentHash is the whole-file sha256 as lowercase hex, no algorithm prefix
	// (empty until the file is first flushed). It matches blobgw's
	// ObjectInfo.ContentHash encoding byte-for-byte (gateway hexSum), so the
	// mlfs↔VFS bridge can register a blob_ref whose content_hash equals this value
	// without re-hashing. Written by SetContentHash on flush/close; read here.
	ContentHash string
	Full        bool
}

// Slice is one write of data over a chunk: a (sliceId, size) source, read from
// [Off, Off+Len). Slices overlay in write order within a chunk.
type Slice struct {
	Id   uint64
	Size uint32
	Off  uint32
	Len  uint32
}

// Entry is a directory entry.
type Entry struct {
	Inode Ino
	Name  string
	Attr  *Attr
}

// Context carries the caller identity for ownership + (future) permission
// checks. A zero Context (root) is fine for internal callers.
type Context struct {
	Uid uint32
	Gid uint32
	Pid uint32
}

// Background returns a root-owned context.
func Background() *Context { return &Context{} }

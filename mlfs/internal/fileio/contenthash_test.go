// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fileio_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// hexSHA256 is the reference encoding the whole-file hash must match: lowercase
// hex sha256 of the bytes, no algorithm prefix (identical to blobgw's ContentHash
// / gateway hexSum).
func hexSHA256(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// TestHashFileKnownVector pins the encoding to a hardcoded vector so a future
// change to the algorithm/encoding (e.g. an accidental "sha256:" prefix) fails
// loudly, independent of crypto/sha256.
func TestHashFileKnownVector(t *testing.T) {
	// sha256("abc") — the canonical NIST vector.
	const wantABC = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	// sha256("") — the empty-string digest.
	const wantEmpty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	s := openStack(t)
	ctx := context.Background()

	ino := s.createFile(t, "abc")
	if _, err := s.f.Write(ctx, ino, 0, []byte("abc"), chunkstore.ClassDefault); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := s.f.HashFile(ctx, ino)
	if err != nil {
		t.Fatalf("HashFile: %v", err)
	}
	if got != wantABC {
		t.Fatalf("HashFile(abc) = %q, want %q", got, wantABC)
	}
	if got != hexSHA256([]byte("abc")) {
		t.Fatalf("HashFile disagrees with crypto/sha256")
	}

	empty := s.createFile(t, "empty")
	got, err = s.f.HashFile(ctx, empty)
	if err != nil {
		t.Fatalf("HashFile(empty): %v", err)
	}
	if got != wantEmpty {
		t.Fatalf("HashFile(empty) = %q, want %q", got, wantEmpty)
	}
}

// TestHashFileSequential verifies the digest of a plain sequential write equals
// crypto/sha256 of the same bytes, across sizes that touch the 1 MiB hash-stream
// buffer boundary and (via a large write) the 64 MiB chunk boundary.
func TestHashFileSequential(t *testing.T) {
	s := openStack(t)
	ctx := context.Background()
	for _, size := range []int{1, 4096, 1 << 20, (1 << 20) + 7, 70 << 20} {
		data := randomBytes(t, size)
		ino := s.createFile(t, "seq")
		if _, err := s.f.Write(ctx, ino, 0, data, chunkstore.ClassDefault); err != nil {
			t.Fatalf("write %d: %v", size, err)
		}
		got, err := s.f.HashFile(ctx, ino)
		if err != nil {
			t.Fatalf("HashFile %d: %v", size, err)
		}
		if want := hexSHA256(data); got != want {
			t.Fatalf("size %d: HashFile = %q, want %q", size, got, want)
		}
		// Clean up the fixed name so the next iteration's Create succeeds.
		if st := s.e.Unlink(ctx, meta.RootInode, "seq", meta.Background()); st != 0 {
			t.Fatalf("unlink: %v", st)
		}
	}
}

// TestHashFileRandomBackfill covers the recompute path's raison d'être: bytes
// written out of order (a later low-offset overwrite/backfill) must still yield
// the final-content hash. HashFile streams the ASSEMBLED file, so the digest
// equals sha256 of the final visible bytes regardless of write order.
func TestHashFileRandomBackfill(t *testing.T) {
	s := openStack(t)
	ctx := context.Background()
	ino := s.createFile(t, "backfill")

	// Write the tail first, then backfill the head — the "random" pattern the
	// incremental fast path cannot follow.
	final := randomBytes(t, 3<<20)
	if _, err := s.f.Write(ctx, ino, 1<<20, final[1<<20:], chunkstore.ClassDefault); err != nil {
		t.Fatalf("write tail: %v", err)
	}
	if _, err := s.f.Write(ctx, ino, 0, final[:1<<20], chunkstore.ClassDefault); err != nil {
		t.Fatalf("write head: %v", err)
	}
	// Overwrite a middle region (a true behind-watermark overwrite).
	over := randomBytes(t, 512<<10)
	copy(final[1<<20:], over)
	if _, err := s.f.Write(ctx, ino, 1<<20, over, chunkstore.ClassDefault); err != nil {
		t.Fatalf("overwrite middle: %v", err)
	}

	got, err := s.f.HashFile(ctx, ino)
	if err != nil {
		t.Fatalf("HashFile: %v", err)
	}
	if want := hexSHA256(final); got != want {
		t.Fatalf("random-backfill HashFile = %q, want %q", got, want)
	}
	// Belt-and-suspenders: the assembled read must equal our expected final bytes.
	if got2 := readAll(t, s, ino, len(final)); !bytes.Equal(got2, final) {
		t.Fatal("assembled content diverged from expected final bytes")
	}
}

// TestHashFileSparse verifies a sparse file (a hole between two writes) hashes as
// the final content WITH the hole read as zeros — the recompute path's zero-fill
// must be reflected, matching what a reader sees.
func TestHashFileSparse(t *testing.T) {
	s := openStack(t)
	ctx := context.Background()
	ino := s.createFile(t, "sparse")

	head := randomBytes(t, 4096)
	tail := randomBytes(t, 4096)
	if _, err := s.f.Write(ctx, ino, 0, head, chunkstore.ClassDefault); err != nil {
		t.Fatalf("write head: %v", err)
	}
	// Leave a 1 MiB hole, then write the tail at offset 1 MiB + 4096.
	tailOff := int64(4096 + (1 << 20))
	if _, err := s.f.Write(ctx, ino, tailOff, tail, chunkstore.ClassDefault); err != nil {
		t.Fatalf("write tail: %v", err)
	}

	// Expected final content: head || zeros(hole) || tail.
	want := make([]byte, int(tailOff)+len(tail))
	copy(want, head)
	copy(want[tailOff:], tail)

	got, err := s.f.HashFile(ctx, ino)
	if err != nil {
		t.Fatalf("HashFile: %v", err)
	}
	if w := hexSHA256(want); got != w {
		t.Fatalf("sparse HashFile = %q, want %q", got, w)
	}
}

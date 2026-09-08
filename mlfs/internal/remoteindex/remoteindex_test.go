// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package remoteindex_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/ctlproto"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/remoteindex"
)

// ---------------------------------------------------------------------------
// fakeRequester — in-process NATS-less "server"
// ---------------------------------------------------------------------------

// fakeRequester decodes ctlproto requests and dispatches them against an
// in-memory index, producing proper ctlproto response payloads. It records
// every (subject, data) pair so tests can assert call counts.
type fakeRequester struct {
	mu    sync.Mutex
	index map[string]map[string]ctlproto.PackRef // domain → chunkHash → PackRef

	calls []fakeCall // history of all Request() invocations

	// If errOnSubject is set, Request returns an error for that subject.
	errOnSubject string
	errMsg       string

	// If serverErrOnSubject is set, the response carries Error for that subject.
	serverErrOnSubject string
	serverErrMsg       string
}

type fakeCall struct {
	Subject string
	Data    []byte
}

func newFakeRequester() *fakeRequester {
	return &fakeRequester{
		index: make(map[string]map[string]ctlproto.PackRef),
	}
}

func (f *fakeRequester) Request(ctx context.Context, subject string, data []byte) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("fake: context done: %w", err)
	}

	f.calls = append(f.calls, fakeCall{Subject: subject, Data: data})

	if f.errOnSubject != "" && subject == f.errOnSubject {
		return nil, errors.New(f.errMsg)
	}

	// Route by subject suffix.
	switch {
	case isLookup(subject):
		return f.handleLookup(subject, data)
	case isRecord(subject):
		return f.handleRecord(subject, data)
	case isPurge(subject):
		return f.handlePurge(subject, data)
	default:
		return nil, fmt.Errorf("fake: unknown subject %q", subject)
	}
}

func isLookup(s string) bool { return strings.HasSuffix(s, ".index.lookup") }
func isRecord(s string) bool { return strings.HasSuffix(s, ".index.record") }
func isPurge(s string) bool  { return strings.HasSuffix(s, ".gc.purge") }

func (f *fakeRequester) handleLookup(subject string, data []byte) ([]byte, error) {
	if f.serverErrOnSubject != "" && subject == f.serverErrOnSubject {
		return json.Marshal(ctlproto.LookupBatchResponse{Error: f.serverErrMsg})
	}
	var req ctlproto.LookupBatchRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	locs := make(map[string]ctlproto.PackRef)
	if dm := f.index[req.Domain]; dm != nil {
		for _, h := range req.ChunkHashes {
			if ref, ok := dm[h]; ok {
				locs[h] = ref
			}
		}
	}
	return json.Marshal(ctlproto.LookupBatchResponse{Locations: locs})
}

func (f *fakeRequester) handleRecord(subject string, data []byte) ([]byte, error) {
	if f.serverErrOnSubject != "" && subject == f.serverErrOnSubject {
		return json.Marshal(ctlproto.RecordResponse{Error: f.serverErrMsg})
	}
	var req ctlproto.RecordRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	if f.index[req.Domain] == nil {
		f.index[req.Domain] = make(map[string]ctlproto.PackRef)
	}
	for _, loc := range req.Locations {
		// First-writer-wins: ignore if already present.
		if _, exists := f.index[req.Domain][loc.ChunkHash]; !exists {
			f.index[req.Domain][loc.ChunkHash] = loc.PackRef
		}
	}
	return json.Marshal(ctlproto.RecordResponse{})
}

func (f *fakeRequester) handlePurge(subject string, data []byte) ([]byte, error) {
	if f.serverErrOnSubject != "" && subject == f.serverErrOnSubject {
		return json.Marshal(ctlproto.PurgePacksResponse{Error: f.serverErrMsg})
	}
	var req ctlproto.PurgePacksRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	if dm := f.index[req.Domain]; dm != nil {
		// Remove all entries whose PackRef.PackHash matches.
		purgeSets := make(map[string]bool, len(req.PackHashes))
		for _, ph := range req.PackHashes {
			purgeSets[ph] = true
		}
		for ch, ref := range dm {
			if purgeSets[ref.PackHash] {
				delete(dm, ch)
			}
		}
	}
	return json.Marshal(ctlproto.PurgePacksResponse{})
}

func (f *fakeRequester) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// ---------------------------------------------------------------------------
// Compile-time interface assertion (also in production file; belt+suspenders)
// ---------------------------------------------------------------------------

var _ snapshot.DedupStore = (*remoteindex.RemoteDedupStore)(nil)

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestLookup_Miss(t *testing.T) {
	fake := newFakeRequester()
	store := remoteindex.NewRemoteDedupStore(fake)

	ref, ok, err := store.Lookup(context.Background(), "tenant1", "aabbcc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected miss, got hit with ref %+v", ref)
	}
}

func TestLookup_Hit(t *testing.T) {
	fake := newFakeRequester()
	// Pre-populate the fake index.
	fake.index["tenant1"] = map[string]ctlproto.PackRef{
		"deadbeef": {PackHash: "pack001", Offset: 128, Size: 512},
	}
	store := remoteindex.NewRemoteDedupStore(fake)

	ref, ok, err := store.Lookup(context.Background(), "tenant1", "deadbeef")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected hit, got miss")
	}
	if ref.PackHash != "pack001" || ref.Offset != 128 || ref.Size != 512 {
		t.Fatalf("unexpected ref: %+v", ref)
	}
}

func TestLookupBatch_MixedHitMiss(t *testing.T) {
	fake := newFakeRequester()
	fake.index["dom"] = map[string]ctlproto.PackRef{
		"hash1": {PackHash: "packA", Offset: 0, Size: 100},
		"hash3": {PackHash: "packA", Offset: 100, Size: 200},
	}
	store := remoteindex.NewRemoteDedupStore(fake)

	got, err := store.LookupBatch(context.Background(), "dom", []string{"hash1", "hash2", "hash3", "hash4"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(got), got)
	}
	if _, ok := got["hash1"]; !ok {
		t.Error("hash1 should be in result")
	}
	if _, ok := got["hash3"]; !ok {
		t.Error("hash3 should be in result")
	}
	if _, ok := got["hash2"]; ok {
		t.Error("hash2 should not be in result")
	}
}

func TestLookupBatch_LargerThanMaxSplitsRequests(t *testing.T) {
	fake := newFakeRequester()
	store := remoteindex.NewRemoteDedupStore(fake)

	// Build a slice just over two batches.
	total := ctlproto.MaxChunkHashesPerBatch + 500
	hashes := make([]string, total)
	for i := range hashes {
		hashes[i] = fmt.Sprintf("hash%08d", i)
	}

	_, err := store.LookupBatch(context.Background(), "dom", hashes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Must have issued exactly 2 requests.
	if n := fake.callCount(); n != 2 {
		t.Fatalf("expected 2 NATS requests for %d hashes, got %d", total, n)
	}
}

func TestLookupBatch_ExactlyMaxDoesNotSplit(t *testing.T) {
	fake := newFakeRequester()
	store := remoteindex.NewRemoteDedupStore(fake)

	hashes := make([]string, ctlproto.MaxChunkHashesPerBatch)
	for i := range hashes {
		hashes[i] = fmt.Sprintf("hash%08d", i)
	}

	_, err := store.LookupBatch(context.Background(), "dom", hashes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n := fake.callCount(); n != 1 {
		t.Fatalf("expected 1 request for exactly MaxChunkHashesPerBatch, got %d", n)
	}
}

func TestRecord_ThenLookup_RoundTrip(t *testing.T) {
	fake := newFakeRequester()
	store := remoteindex.NewRemoteDedupStore(fake)

	locs := []snapshot.ChunkLocation{
		{ChunkHash: "cafebabe", PackRef: snapshot.PackRef{PackHash: "packXYZ", Offset: 256, Size: 1024}},
		{ChunkHash: "deadd00d", PackRef: snapshot.PackRef{PackHash: "packXYZ", Offset: 1280, Size: 512}},
	}

	if err := store.Record(context.Background(), "dom2", locs); err != nil {
		t.Fatalf("Record error: %v", err)
	}

	got, err := store.LookupBatch(context.Background(), "dom2", []string{"cafebabe", "deadd00d", "nothere"})
	if err != nil {
		t.Fatalf("LookupBatch error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 results, got %d", len(got))
	}

	ref := got["cafebabe"]
	if ref.PackHash != "packXYZ" || ref.Offset != 256 || ref.Size != 1024 {
		t.Errorf("cafebabe ref mismatch: %+v", ref)
	}
	ref = got["deadd00d"]
	if ref.PackHash != "packXYZ" || ref.Offset != 1280 || ref.Size != 512 {
		t.Errorf("deadd00d ref mismatch: %+v", ref)
	}
}

func TestRecord_Batches(t *testing.T) {
	fake := newFakeRequester()
	store := remoteindex.NewRemoteDedupStore(fake)

	total := ctlproto.MaxChunkHashesPerBatch + 1
	locs := make([]snapshot.ChunkLocation, total)
	for i := range locs {
		locs[i] = snapshot.ChunkLocation{
			ChunkHash: fmt.Sprintf("hash%08d", i),
			PackRef:   snapshot.PackRef{PackHash: "packA", Offset: i * 64, Size: 64},
		}
	}

	if err := store.Record(context.Background(), "dom3", locs); err != nil {
		t.Fatalf("Record error: %v", err)
	}
	if n := fake.callCount(); n != 2 {
		t.Fatalf("expected 2 Record requests, got %d", n)
	}
}

func TestPurgePacks(t *testing.T) {
	fake := newFakeRequester()
	fake.index["pdom"] = map[string]ctlproto.PackRef{
		"chunk1": {PackHash: "pack1", Offset: 0, Size: 100},
		"chunk2": {PackHash: "pack2", Offset: 0, Size: 100},
		"chunk3": {PackHash: "pack1", Offset: 100, Size: 200},
	}
	store := remoteindex.NewRemoteDedupStore(fake)

	if err := store.PurgePacks(context.Background(), "pdom", []string{"pack1"}); err != nil {
		t.Fatalf("PurgePacks error: %v", err)
	}

	// pack1 entries should be gone; pack2 entry should survive.
	got, err := store.LookupBatch(context.Background(), "pdom", []string{"chunk1", "chunk2", "chunk3"})
	if err != nil {
		t.Fatalf("LookupBatch after purge error: %v", err)
	}
	if _, ok := got["chunk1"]; ok {
		t.Error("chunk1 should have been purged")
	}
	if _, ok := got["chunk3"]; ok {
		t.Error("chunk3 should have been purged")
	}
	if _, ok := got["chunk2"]; !ok {
		t.Error("chunk2 should still be present (different pack)")
	}
}

func TestServerErrorPropagation_Lookup(t *testing.T) {
	fake := newFakeRequester()
	subject := ctlproto.IndexLookupSubject("errdom")
	fake.serverErrOnSubject = subject
	fake.serverErrMsg = "index unavailable"
	store := remoteindex.NewRemoteDedupStore(fake)

	_, err := store.LookupBatch(context.Background(), "errdom", []string{"h1"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !contains(err.Error(), "index unavailable") {
		t.Fatalf("error should mention server message, got: %v", err)
	}
}

func TestServerErrorPropagation_Record(t *testing.T) {
	fake := newFakeRequester()
	subject := ctlproto.IndexRecordSubject("errdom")
	fake.serverErrOnSubject = subject
	fake.serverErrMsg = "record failed"
	store := remoteindex.NewRemoteDedupStore(fake)

	locs := []snapshot.ChunkLocation{{ChunkHash: "h1", PackRef: snapshot.PackRef{PackHash: "p1", Offset: 0, Size: 10}}}
	err := store.Record(context.Background(), "errdom", locs)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !contains(err.Error(), "record failed") {
		t.Fatalf("error should mention server message, got: %v", err)
	}
}

func TestServerErrorPropagation_PurgePacks(t *testing.T) {
	fake := newFakeRequester()
	subject := ctlproto.GCPurgeSubject("errdom")
	fake.serverErrOnSubject = subject
	fake.serverErrMsg = "purge failed"
	store := remoteindex.NewRemoteDedupStore(fake)

	err := store.PurgePacks(context.Background(), "errdom", []string{"pack1"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !contains(err.Error(), "purge failed") {
		t.Fatalf("error should mention server message, got: %v", err)
	}
}

func TestTransportErrorPropagation(t *testing.T) {
	fake := newFakeRequester()
	subject := ctlproto.IndexLookupSubject("tdom")
	fake.errOnSubject = subject
	fake.errMsg = "connection refused"
	store := remoteindex.NewRemoteDedupStore(fake)

	_, err := store.LookupBatch(context.Background(), "tdom", []string{"h1"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !contains(err.Error(), "connection refused") {
		t.Fatalf("error should mention transport error, got: %v", err)
	}
}

func TestContextTimeout(t *testing.T) {
	// slowRequester blocks until context is cancelled.
	slow := &slowRequester{}
	store := remoteindex.NewRemoteDedupStore(slow)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := store.LookupBatch(ctx, "dom", []string{"h1"})
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !contains(err.Error(), "context") {
		t.Fatalf("expected context error, got: %v", err)
	}
}

// slowRequester blocks until context is done.
type slowRequester struct{}

func (s *slowRequester) Request(ctx context.Context, _ string, _ []byte) ([]byte, error) {
	<-ctx.Done()
	return nil, fmt.Errorf("fake: context done: %w", ctx.Err())
}

func TestTypeMapping_IntFields(t *testing.T) {
	// Verify Offset/Size survive the int64 ↔ int round-trip through the fake.
	fake := newFakeRequester()
	store := remoteindex.NewRemoteDedupStore(fake)

	want := snapshot.ChunkLocation{
		ChunkHash: "typemap",
		PackRef:   snapshot.PackRef{PackHash: "packT", Offset: 1_000_000, Size: 2_000_000},
	}
	if err := store.Record(context.Background(), "typedom", []snapshot.ChunkLocation{want}); err != nil {
		t.Fatalf("Record: %v", err)
	}

	ref, ok, err := store.Lookup(context.Background(), "typedom", "typemap")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !ok {
		t.Fatal("expected hit")
	}
	if ref.Offset != want.Offset || ref.Size != want.Size || ref.PackHash != want.PackHash {
		t.Errorf("type mapping mismatch: got %+v, want %+v", ref, want.PackRef)
	}
}

func TestWithCodec_Option(t *testing.T) {
	// Confirm WithCodec option is accepted; default JSONCodec passes all tests above.
	fake := newFakeRequester()
	store := remoteindex.NewRemoteDedupStore(fake, remoteindex.WithCodec(ctlproto.DefaultCodec))
	_, _, err := store.Lookup(context.Background(), "dom", "hash")
	if err != nil {
		t.Fatalf("unexpected error with explicit codec: %v", err)
	}
}

func TestWithTimeout_Option(t *testing.T) {
	// Confirm WithTimeout option is accepted.
	fake := newFakeRequester()
	store := remoteindex.NewRemoteDedupStore(fake, remoteindex.WithTimeout(10*time.Second))
	_, _, err := store.Lookup(context.Background(), "dom", "hash")
	if err != nil {
		t.Fatalf("unexpected error with explicit timeout: %v", err)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && func() bool {
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	}())
}

// ---------------------------------------------------------------------------
// Empty / nil input — must return zero result and issue 0 NATS calls
// ---------------------------------------------------------------------------

func TestLookupBatch_EmptyInput(t *testing.T) {
	for _, hashes := range [][]string{nil, {}} {
		fake := newFakeRequester()
		store := remoteindex.NewRemoteDedupStore(fake)

		got, err := store.LookupBatch(context.Background(), "dom", hashes)
		if err != nil {
			t.Fatalf("LookupBatch(empty): unexpected error: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("LookupBatch(empty): expected empty result, got %v", got)
		}
		if n := fake.callCount(); n != 0 {
			t.Errorf("LookupBatch(empty): expected 0 NATS calls, got %d", n)
		}
	}
}

func TestRecord_EmptyInput(t *testing.T) {
	for _, locs := range [][]snapshot.ChunkLocation{nil, {}} {
		fake := newFakeRequester()
		store := remoteindex.NewRemoteDedupStore(fake)

		if err := store.Record(context.Background(), "dom", locs); err != nil {
			t.Fatalf("Record(empty): unexpected error: %v", err)
		}
		if n := fake.callCount(); n != 0 {
			t.Errorf("Record(empty): expected 0 NATS calls, got %d", n)
		}
	}
}

func TestPurgePacks_EmptyInput(t *testing.T) {
	for _, hashes := range [][]string{nil, {}} {
		fake := newFakeRequester()
		store := remoteindex.NewRemoteDedupStore(fake)

		if err := store.PurgePacks(context.Background(), "dom", hashes); err != nil {
			t.Fatalf("PurgePacks(empty): unexpected error: %v", err)
		}
		if n := fake.callCount(); n != 0 {
			t.Errorf("PurgePacks(empty): expected 0 NATS calls, got %d", n)
		}
	}
}

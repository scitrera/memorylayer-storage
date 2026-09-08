// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package chunkstore_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/ctlproto"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/s3http"
)

// remoteTestTenant is a valid NATS subject token (no '.', ' ', '*', '>').
const remoteTestTenant = "tenant-remote"

// ---------------------------------------------------------------------------
// fakeS3 — in-memory object store over httptest (PUT store, GET incl. Range).
// ---------------------------------------------------------------------------

type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte

	// getsRanged / getsFull count GET requests by whether they carried a Range
	// header, so a test can assert the reader took the RANGED path (#18) rather
	// than fetching whole packs. Only pack-blob GETs are interesting; the helper
	// packGetStats filters by key prefix.
	gets []recordedGet
}

type recordedGet struct {
	key    string
	ranged bool
}

func newFakeS3() *fakeS3 { return &fakeS3{objects: make(map[string][]byte)} }

func (s *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/")
	switch r.Method {
	case http.MethodPut:
		body := new(bytes.Buffer)
		if _, err := body.ReadFrom(r.Body); err != nil {
			http.Error(w, "read body", http.StatusInternalServerError)
			return
		}
		s.mu.Lock()
		s.objects[key] = body.Bytes()
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		s.mu.Lock()
		data, ok := s.objects[key]
		s.gets = append(s.gets, recordedGet{key: key, ranged: r.Header.Get("Range") != ""})
		s.mu.Unlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		// http.ServeContent honours the Range header (206), exercising
		// s3http.GetRange's partial-content path.
		http.ServeContent(w, r, key, time.Time{}, bytes.NewReader(data))
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// packGetStats returns, over GETs of pack blobs since the last reset, how many
// carried a Range header (ranged data fetch or header probe) vs none (whole-pack
// fetch). The casstore reader's framing probe is a tiny ranged GET too, so a
// whole-pack (full) GET is the unambiguous signal of the compressed fallback.
func (s *fakeS3) packGetStats() (ranged, full int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range s.gets {
		if !strings.Contains(g.key, "pack-") {
			continue
		}
		if g.ranged {
			ranged++
		} else {
			full++
		}
	}
	return ranged, full
}

func (s *fakeS3) resetGets() {
	s.mu.Lock()
	s.gets = nil
	s.mu.Unlock()
}

// ---------------------------------------------------------------------------
// combinedRequester — one in-process Requester serving BOTH the dedup-index
// RPCs (lookup/record/purge over an in-memory map, first-writer-wins) AND the
// presign RPC (URLs pointing at the httptest S3). It routes by subject suffix.
// ---------------------------------------------------------------------------

type combinedRequester struct {
	baseURL string
	codec   ctlproto.Codec

	mu    sync.Mutex
	index map[string]map[string]ctlproto.PackRef // domain → chunkHash → PackRef
	// assoc records the pack associations the write path reports, as
	// domain → context → set of pack hashes, so a test can assert the converged
	// path really tells blobgw which owner references which bytes.
	assoc map[string]map[string]map[string]bool
}

func newCombinedRequester(baseURL string) *combinedRequester {
	return &combinedRequester{
		baseURL: baseURL,
		codec:   ctlproto.DefaultCodec,
		index:   make(map[string]map[string]ctlproto.PackRef),
		assoc:   make(map[string]map[string]map[string]bool),
	}
}

func (f *combinedRequester) Request(ctx context.Context, subject string, data []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("combinedRequester: context done: %w", err)
	}
	switch {
	case strings.HasSuffix(subject, ".presign"):
		return f.handlePresign(data)
	case strings.HasSuffix(subject, ".index.lookup"):
		return f.handleLookup(data)
	case strings.HasSuffix(subject, ".index.record"):
		return f.handleRecord(data)
	case strings.HasSuffix(subject, ".gc.purge"):
		return f.handlePurge(data)
	case strings.HasSuffix(subject, ".assoc.record"):
		return f.handleAssociate(data)
	default:
		return nil, fmt.Errorf("combinedRequester: unknown subject %q", subject)
	}
}

func (f *combinedRequester) handlePresign(data []byte) ([]byte, error) {
	var req ctlproto.PresignBatchRequest
	if err := f.codec.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("combinedRequester: unmarshal presign: %w", err)
	}
	urls := make(map[string]string, len(req.Keys))
	for _, k := range req.Keys {
		// Path-escape the key so multi-segment keys map cleanly to a URL path
		// the fake S3 server decodes back to the same key.
		urls[k] = f.baseURL + "/" + (&url.URL{Path: k}).EscapedPath() + "?sig=fake-" + string(req.Op)
	}
	return f.codec.Marshal(ctlproto.PresignBatchResponse{
		URLs:          urls,
		ExpiresAtUnix: time.Now().Add(time.Hour).Unix(), // future expiry
	})
}

func (f *combinedRequester) handleLookup(data []byte) ([]byte, error) {
	var req ctlproto.LookupBatchRequest
	if err := f.codec.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("combinedRequester: unmarshal lookup: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	locs := make(map[string]ctlproto.PackRef)
	if dm := f.index[req.Domain]; dm != nil {
		for _, h := range req.ChunkHashes {
			if ref, ok := dm[h]; ok {
				locs[h] = ref
			}
		}
	}
	return f.codec.Marshal(ctlproto.LookupBatchResponse{Locations: locs})
}

func (f *combinedRequester) handleRecord(data []byte) ([]byte, error) {
	var req ctlproto.RecordRequest
	if err := f.codec.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("combinedRequester: unmarshal record: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.index[req.Domain] == nil {
		f.index[req.Domain] = make(map[string]ctlproto.PackRef)
	}
	for _, loc := range req.Locations {
		// First-writer-wins: ignore if already present.
		if _, exists := f.index[req.Domain][loc.ChunkHash]; !exists {
			f.index[req.Domain][loc.ChunkHash] = loc.PackRef
		}
	}
	return f.codec.Marshal(ctlproto.RecordResponse{})
}

func (f *combinedRequester) handlePurge(data []byte) ([]byte, error) {
	var req ctlproto.PurgePacksRequest
	if err := f.codec.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("combinedRequester: unmarshal purge: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if dm := f.index[req.Domain]; dm != nil {
		purge := make(map[string]bool, len(req.PackHashes))
		for _, ph := range req.PackHashes {
			purge[ph] = true
		}
		for ch, ref := range dm {
			if purge[ref.PackHash] {
				delete(dm, ch)
			}
		}
	}
	return f.codec.Marshal(ctlproto.PurgePacksResponse{})
}

func (f *combinedRequester) handleAssociate(data []byte) ([]byte, error) {
	var req ctlproto.AssociateRequest
	if err := f.codec.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("combinedRequester: unmarshal associate: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.assoc[req.Domain] == nil {
		f.assoc[req.Domain] = make(map[string]map[string]bool)
	}
	for _, a := range req.Associations {
		if f.assoc[req.Domain][a.Context] == nil {
			f.assoc[req.Domain][a.Context] = make(map[string]bool)
		}
		f.assoc[req.Domain][a.Context][a.PackHash] = true
	}
	return f.codec.Marshal(ctlproto.AssociateResponse{})
}

// associatedPacks returns the pack hashes recorded for one owner context.
func (f *combinedRequester) associatedPacks(domain, context string) map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]bool)
	for p := range f.assoc[domain][context] {
		out[p] = true
	}
	return out
}

// chunkCount returns the number of distinct chunk hashes recorded in the remote
// index for the tenant — the physical-dedup signal the round-trip test asserts.
func (f *combinedRequester) chunkCount(domain string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.index[domain])
}

// ---------------------------------------------------------------------------
// newRemoteStore wires the converged node stack against the fakes and returns
// the chunkstore.Remote under test plus the requester (for index assertions).
// ---------------------------------------------------------------------------

func newRemoteStore(t *testing.T) (*chunkstore.Remote, *combinedRequester) {
	t.Helper()
	r, fr, _ := newRemoteStoreWith(t, nil)
	return r, fr
}

// newRemoteStoreWith wires the converged node stack with an explicit compression
// policy (nil = the RemoteConfig default, i.e. uncompressed) and also returns the
// in-memory fakeS3 so a test can inspect the GET access pattern (ranged vs
// whole-pack).
func newRemoteStoreWith(t *testing.T, compression snapshot.CompressionPolicy) (*chunkstore.Remote, *combinedRequester, *fakeS3) {
	t.Helper()
	return newRemoteStoreFramed(t, compression, snapshot.PackCompressPerChunk)
}

// newRemoteStoreFramed is newRemoteStoreWith with an explicit pack-compression
// framing, so a test can pin the per-chunk default or the whole-pack rollback.
func newRemoteStoreFramed(t *testing.T, compression snapshot.CompressionPolicy, framing snapshot.PackCompressionMode) (*chunkstore.Remote, *combinedRequester, *fakeS3) {
	t.Helper()

	s3 := newFakeS3()
	srv := httptest.NewServer(s3)
	t.Cleanup(srv.Close)

	fr := newCombinedRequester(srv.URL)

	manifests, err := snapshot.NewLocalStore(t.TempDir() + "/manifests")
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	r, err := chunkstore.NewRemote(chunkstore.RemoteConfig{
		Tenant:          remoteTestTenant,
		Requester:       fr,
		HTTP:            s3http.NewClient(s3http.WithHTTPClient(srv.Client())),
		Manifests:       manifests,
		Compression:     compression,
		PackCompression: framing,
	})
	if err != nil {
		t.Fatalf("NewRemote: %v", err)
	}
	return r, fr, s3
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

func TestNewRemote_Validation(t *testing.T) {
	httpc := s3http.NewClient()
	manifests, err := snapshot.NewLocalStore(t.TempDir() + "/m")
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	fr := newCombinedRequester("http://example")

	cases := []struct {
		name string
		cfg  chunkstore.RemoteConfig
	}{
		{"empty tenant", chunkstore.RemoteConfig{Requester: fr, HTTP: httpc, Manifests: manifests}},
		{"nil requester", chunkstore.RemoteConfig{Tenant: remoteTestTenant, HTTP: httpc, Manifests: manifests}},
		{"nil http", chunkstore.RemoteConfig{Tenant: remoteTestTenant, Requester: fr, Manifests: manifests}},
		{"nil manifests", chunkstore.RemoteConfig{Tenant: remoteTestTenant, Requester: fr, HTTP: httpc}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := chunkstore.NewRemote(tc.cfg); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// End-to-end node stack: Write/Read/ReadAt/Exists round-trips, plus dedup via
// the remote index.
// ---------------------------------------------------------------------------

func TestNewRemote_RoundTrip(t *testing.T) {
	r, _ := newRemoteStore(t)
	ctx := context.Background()

	// Use a payload large enough to survive zstd compression as real bytes and
	// to exercise the pack path; deterministic content keeps the test stable.
	payload := bytes.Repeat([]byte("the quick brown fox jumps over 0123456789 "), 4096)

	const sliceID = uint64(42)
	if err := r.Store.Write(ctx, sliceID, payload, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := r.Store.Read(ctx, sliceID)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("Read round-trip mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	// ReadAt at an offset returns the right sub-bytes.
	const off = int64(1000)
	buf := make([]byte, 256)
	n, err := r.Store.ReadAt(ctx, sliceID, off, buf)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("ReadAt: read %d bytes, want %d", n, len(buf))
	}
	if want := payload[off : off+int64(len(buf))]; !bytes.Equal(buf, want) {
		t.Fatalf("ReadAt content mismatch at off %d", off)
	}

	// Exists true after write.
	exists, err := r.Store.Exists(ctx, sliceID)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Fatal("Exists: want true after write")
	}

	// Exists false for an unwritten slice.
	exists, err = r.Store.Exists(ctx, 9999)
	if err != nil {
		t.Fatalf("Exists(absent): %v", err)
	}
	if exists {
		t.Fatal("Exists(absent): want false")
	}
}

// TestNewRemote_DefaultUncompressedEngagesRangedReads proves PART B: with the
// remote default (uncompressed), a model-weight-like slice reads back through
// casstore's #18 RANGED path — the reader issues ranged S3 GETs and NEVER a
// whole-pack fetch. A whole-pack GET (no Range header) is the unambiguous signal
// of the compressed-pack fallback; its absence here confirms ranged reads engage.
func TestNewRemote_DefaultUncompressedEngagesRangedReads(t *testing.T) {
	r, _, s3 := newRemoteStoreWith(t, nil) // nil = remote default: uncompressed
	ctx := context.Background()

	// Highly compressible content: if this were stored compressed (the old
	// default), the reader would HAVE to fall back to whole-pack fetches. Under
	// the uncompressed default it stays range-sliceable, so the test is a sharp
	// discriminator.
	payload := bytes.Repeat([]byte("model-weight-ish-block-of-bytes-"), 200000) // ~6.4 MiB

	const sliceID = uint64(7)
	if err := r.Store.Write(ctx, sliceID, payload, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}

	s3.resetGets()
	got, err := r.Store.Read(ctx, sliceID)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: got %d want %d", len(got), len(payload))
	}

	ranged, full := s3.packGetStats()
	if full != 0 {
		t.Fatalf("uncompressed default took the whole-pack fallback: %d full GET(s) (expected 0) — ranged reads did not engage", full)
	}
	if ranged == 0 {
		t.Fatal("expected at least one RANGED pack GET on the uncompressed default read path")
	}
	t.Logf("uncompressed default: %d ranged pack GET(s), %d whole-pack GET(s)", ranged, full)
}

// TestNewRemote_CompressedOptInUsesWholePack proves the opt-in knob: passing a
// zstd policy via RemoteConfig.Compression stores compressible slices compressed,
// which the reader can only restore via a WHOLE-pack fetch + decompress. This is
// the documented tension — compression trades ranged reads for smaller bytes.
// TestNewRemote_CompressedOptInIsRangeReadable is the mlfs-side proof of the
// per-chunk pack framing (A1), on the remote path the cold-load cost lives on:
// with zstd opted in, a compressed pack is still served by RANGED GETs and never
// falls back to fetching the whole pack. Before per-chunk framing this same read
// pulled the entire pack, which is the behaviour this test previously asserted
// (TECH_DEBT #18).
func TestNewRemote_CompressedOptInIsRangeReadable(t *testing.T) {
	r, _, s3 := newRemoteStoreWith(t, snapshot.NewContentTypePolicy(snapshot.CompressZstd))
	ctx := context.Background()

	payload := bytes.Repeat([]byte("model-weight-ish-block-of-bytes-"), 200000)

	const sliceID = uint64(8)
	if err := r.Store.Write(ctx, sliceID, payload, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}

	s3.resetGets()
	got, err := r.Store.Read(ctx, sliceID)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: got %d want %d", len(got), len(payload))
	}

	ranged, full := s3.packGetStats()
	if full != 0 {
		t.Fatalf("compressed pack fell back to %d whole-pack GET(s); per-chunk framing must keep it range-readable", full)
	}
	if ranged == 0 {
		t.Fatal("expected ranged pack GETs for a per-chunk-framed compressed pack")
	}
	t.Logf("zstd opt-in: %d ranged pack GET(s), 0 whole-pack GETs", ranged)
}

func TestNewRemote_DedupViaRemoteIndex(t *testing.T) {
	r, fr := newRemoteStore(t)
	ctx := context.Background()

	payload := bytes.Repeat([]byte("dedup-me-across-distinct-slice-ids-9876 "), 4096)

	// First write of the content under slice id 1.
	if err := r.Store.Write(ctx, 1, payload, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write slice 1: %v", err)
	}
	first := fr.chunkCount(remoteTestTenant)
	if first == 0 {
		t.Fatal("expected the first write to record chunks in the remote index")
	}

	// Write IDENTICAL content under a DIFFERENT slice id. The global dedup
	// index must report every chunk as already-present, so no NEW physical
	// chunk is recorded — the recorded chunk count is unchanged.
	if err := r.Store.Write(ctx, 2, payload, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write slice 2 (identical content): %v", err)
	}
	second := fr.chunkCount(remoteTestTenant)
	if second != first {
		t.Fatalf("dedup failed: chunk count grew from %d to %d on identical content", first, second)
	}

	// Both slice ids must read back the identical bytes.
	for _, id := range []uint64{1, 2} {
		got, err := r.Store.Read(ctx, id)
		if err != nil {
			t.Fatalf("Read slice %d: %v", id, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("Read slice %d mismatch", id)
		}
	}
}

// TestNewRemote_PackFramingRollbackUsesWholePack proves the -pack-framing knob is
// real end to end: selecting the whole-pack rollback on the remote path restores
// the pre-A1 behaviour (a compressed pack is fetched whole and decompressed),
// while still round-tripping byte-identical content.
func TestNewRemote_PackFramingRollbackUsesWholePack(t *testing.T) {
	r, _, s3 := newRemoteStoreFramed(t,
		snapshot.NewContentTypePolicy(snapshot.CompressZstd),
		snapshot.PackCompressWholePack)
	ctx := context.Background()

	payload := bytes.Repeat([]byte("model-weight-ish-block-of-bytes-"), 200000)

	const sliceID = uint64(9)
	if err := r.Store.Write(ctx, sliceID, payload, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}

	s3.resetGets()
	got, err := r.Store.Read(ctx, sliceID)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: got %d want %d", len(got), len(payload))
	}

	_, full := s3.packGetStats()
	if full == 0 {
		t.Fatal("whole-pack framing must fetch the pack whole; got no full pack GET")
	}
	t.Logf("whole-pack rollback: %d whole-pack GET(s)", full)
}

// TestNewRemote_AssociatesPacksOverControlPlane proves the converged path reports
// its pack associations to blobgw: a slice write records, under the slice's own
// owner context, every pack the slice references. That is what later lets
// reclamation scope itself to one slice instead of walking the store.
func TestNewRemote_AssociatesPacksOverControlPlane(t *testing.T) {
	r, fr := newRemoteStore(t)
	ctx := context.Background()

	const sliceID = uint64(77)
	payload := bytes.Repeat([]byte("associated-slice-block-"), 100000)
	if err := r.Store.Write(ctx, sliceID, payload, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// chunkstore keys a slice as "s/<id>", which is the OwnerKey the association
	// context defaults to.
	packs := fr.associatedPacks(remoteTestTenant, "s/77")
	if len(packs) == 0 {
		t.Fatal("slice write recorded no pack associations over the control plane")
	}
	t.Logf("slice 77 associated %d pack(s) under context s/77", len(packs))

	// A second, identical slice dedups into the same packs and must associate
	// them under ITS OWN context — otherwise reclaiming the first slice would
	// take bytes the second still needs.
	if err := r.Store.Write(ctx, uint64(78), payload, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write second: %v", err)
	}
	second := fr.associatedPacks(remoteTestTenant, "s/78")
	if len(second) == 0 {
		t.Fatal("the deduping slice recorded no associations of its own")
	}
	for p := range packs {
		if !second[p] {
			t.Fatalf("deduped slice did not associate shared pack %s under its own context", p)
		}
	}
}

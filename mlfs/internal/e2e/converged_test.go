// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package e2e

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/scitrera/memorylayer-storage/blobgw/controlplane"
	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/remoteindex"
	"github.com/scitrera/memorylayer-storage/mlfs/internal/s3http"
)

// e2eTenant is a valid NATS subject token (no '.', ' ', '*', '>') used as both
// the casstore dedup domain and the ctlproto subject tenant.
const e2eTenant = "tenant-e2e"

// ---------------------------------------------------------------------------
// Embedded NATS — in-process server with no TCP listener (same pattern as
// blobgw/controlplane/server_test.go's startNATS).
// ---------------------------------------------------------------------------

func startNATS(t *testing.T) *nats.Conn {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{DontListen: true})
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatalf("nats not ready")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	conn, err := nats.Connect("", nats.InProcessServer(srv))
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	t.Cleanup(conn.Close)
	return conn
}

// ---------------------------------------------------------------------------
// fakeS3 — in-memory object store over httptest. The presigned URLs minted by
// the REAL blobgw presign handler use path-style addressing
// (http://host:port/<bucket>/<key>), so the request path includes the bucket
// segment; we key objects by the full trimmed path. PUT stores, GET serves
// (honouring Range → 206 via http.ServeContent), HEAD returns Content-Length.
// objectCount supports the dedup assertion (no new physical object on identical
// content).
// ---------------------------------------------------------------------------

type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
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
		s.mu.Unlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		// http.ServeContent honours the Range header (206), exercising
		// s3http.GetRange's partial-content path.
		http.ServeContent(w, r, key, time.Time{}, bytes.NewReader(data))
	case http.MethodHead:
		s.mu.Lock()
		data, ok := s.objects[key]
		s.mu.Unlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		http.ServeContent(w, r, key, time.Time{}, bytes.NewReader(data))
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// objectCount returns the number of distinct objects physically stored in the
// fake S3 — the dedup signal: identical content must not create a new object.
func (s *fakeS3) objectCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objects)
}

// quietLogger discards log output so handler diagnostics don't spam the test.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ---------------------------------------------------------------------------
// harness wires the FULL converged stack against the in-process fakes:
//
//	mlfs node (chunkstore.NewRemote: remoteindex.NatsRequester + s3http +
//	           casstore LocalStore manifests)
//	        │  ctlproto over embedded NATS
//	        ▼
//	blobgw control plane (controlplane.NewServer: snapshot.MemoryDedupStore +
//	           tenantbind.StaticKeysProvider) → presigned URLs → httptest S3
//
// Nothing external is touched.
// ---------------------------------------------------------------------------

type harness struct {
	remote *chunkstore.Remote
	s3     *fakeS3
	index  *snapshot.MemoryDedupStore
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	// 1. Embedded NATS.
	conn := startNATS(t)

	// 2. httptest S3.
	s3 := newFakeS3()
	httpSrv := httptest.NewServer(s3)
	t.Cleanup(httpSrv.Close)

	// 3. REAL blobgw control plane: hermetic in-memory dedup index + a static
	//    tenantbind provider whose endpoint is the httptest S3. Prefix is empty
	//    so presigned object keys map straight to /<bucket>/<key>.
	index := snapshot.NewMemoryDedupStore()
	resolver := tenantbind.NewMapResolver()
	resolver.Set(e2eTenant, tenantbind.Descriptor{
		Endpoint:      httpSrv.URL,
		Region:        "us-east-1",
		Bucket:        "test-bucket",
		Prefix:        "",
		CredentialRef: "t",
	})
	secrets := tenantbind.NewMapSecretStore()
	secrets.Set("t", "AKIDEXAMPLE", "secretkeyexample")
	provider := tenantbind.NewStaticKeysProvider(resolver, secrets)

	srv, err := controlplane.NewServer(conn, index, provider, controlplane.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("controlplane.NewServer: %v", err)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("controlplane.Server.Start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	// 4. REAL mlfs node data path. The NatsRequester feeds BOTH the remote dedup
	//    index and the presign client (multiplexed by subject). The httptest
	//    server's client is injected into s3http so its TLS/transport matches.
	req := remoteindex.NewNatsRequester(conn, 5*time.Second)
	httpc := s3http.NewClient(s3http.WithHTTPClient(httpSrv.Client()))
	manifests, err := snapshot.NewLocalStore(t.TempDir() + "/manifests")
	if err != nil {
		t.Fatalf("snapshot.NewLocalStore: %v", err)
	}
	remote, err := chunkstore.NewRemote(chunkstore.RemoteConfig{
		Tenant:          e2eTenant,
		PackTargetBytes: 0, // casstore default pack target
		Requester:       req,
		HTTP:            httpc,
		Manifests:       manifests,
		Logger:          quietLogger(),
	})
	if err != nil {
		t.Fatalf("chunkstore.NewRemote: %v", err)
	}

	return &harness{remote: remote, s3: s3, index: index}
}

// ---------------------------------------------------------------------------
// TestConverged_WriteReadRangedDedup is the end-to-end proof.
//
// Flow exercised (all over the REAL components, no fakes for the control plane
// or node logic):
//
//	Write  → casstore chunks → presign PUT (NATS→blobgw→presigned URL) →
//	         httptest S3 stores object(s); index Record reaches blobgw's
//	         MemoryDedupStore.
//	Read   → presign GET → httptest S3 serves → casstore reassembles → equal.
//	ReadAt → ranged presign GET (Range → 206) → correct sub-slice.
//	Dedup  → second Write of IDENTICAL bytes under a different id → blobgw index
//	         reports every chunk present → NO new httptest S3 object.
//
// ---------------------------------------------------------------------------
func TestConverged_WriteReadRangedDedup(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// A few MB of deterministic content: large enough to span ≥2 casstore packs
	// for a meaningful pack/dedup exercise, yet stable across runs.
	big := bytes.Repeat([]byte("converged-storage-stack-e2e-0123456789-ABCDEFGHIJ "), 80_000) // ~4 MB
	small := []byte("a small slice that fits comfortably in a single chunk")

	// --- Write (big) ---------------------------------------------------------
	const idBig = uint64(1)
	if err := h.remote.Store.Write(ctx, idBig, big, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write(big): %v", err)
	}
	objAfterBig := h.s3.objectCount()
	if objAfterBig == 0 {
		t.Fatal("Write(big): expected ≥1 object stored in httptest S3 via presigned PUT")
	}
	idxAfterBig := h.index.Len(e2eTenant)
	if idxAfterBig == 0 {
		t.Fatal("Write(big): expected blobgw MemoryDedupStore to gain chunk entries via NATS Record")
	}

	// --- Read (big) ----------------------------------------------------------
	got, err := h.remote.Store.Read(ctx, idBig)
	if err != nil {
		t.Fatalf("Read(big): %v", err)
	}
	if !bytes.Equal(got, big) {
		t.Fatalf("Read(big) round-trip mismatch: got %d bytes, want %d", len(got), len(big))
	}

	// --- Ranged read (big) ---------------------------------------------------
	const off = int64(1_500_000)
	buf := make([]byte, 4096)
	n, err := h.remote.Store.ReadAt(ctx, idBig, off, buf)
	if err != nil {
		t.Fatalf("ReadAt(big): %v", err)
	}
	if n != len(buf) {
		t.Fatalf("ReadAt(big): read %d bytes, want %d", n, len(buf))
	}
	if want := big[off : off+int64(len(buf))]; !bytes.Equal(buf, want) {
		t.Fatalf("ReadAt(big): content mismatch at off %d", off)
	}

	// --- Small slice round-trips through the same real stack -----------------
	const idSmall = uint64(2)
	if err := h.remote.Store.Write(ctx, idSmall, small, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write(small): %v", err)
	}
	gotSmall, err := h.remote.Store.Read(ctx, idSmall)
	if err != nil {
		t.Fatalf("Read(small): %v", err)
	}
	if !bytes.Equal(gotSmall, small) {
		t.Fatalf("Read(small) mismatch: got %q", gotSmall)
	}

	// --- Dedup across the REAL blobgw index ----------------------------------
	// Snapshot physical-object count, then write IDENTICAL big bytes under a new
	// slice id. blobgw's index must report every chunk already present over the
	// NATS round-trip, so the node uploads NO new object.
	objBeforeDup := h.s3.objectCount()
	idxBeforeDup := h.index.Len(e2eTenant)

	const idDup = uint64(3)
	if err := h.remote.Store.Write(ctx, idDup, big, chunkstore.ClassDefault); err != nil {
		t.Fatalf("Write(dup, identical bytes): %v", err)
	}

	if objAfter := h.s3.objectCount(); objAfter != objBeforeDup {
		t.Fatalf("dedup failed: httptest S3 object count grew from %d to %d on identical content",
			objBeforeDup, objAfter)
	}
	if idxAfter := h.index.Len(e2eTenant); idxAfter != idxBeforeDup {
		t.Fatalf("dedup failed: blobgw index grew from %d to %d on identical content",
			idxBeforeDup, idxAfter)
	}

	// The deduped slice must still read back identical bytes (it shares the
	// already-stored chunks via the global index).
	gotDup, err := h.remote.Store.Read(ctx, idDup)
	if err != nil {
		t.Fatalf("Read(dup): %v", err)
	}
	if !bytes.Equal(gotDup, big) {
		t.Fatalf("Read(dup) mismatch: got %d bytes, want %d", len(gotDup), len(big))
	}
}

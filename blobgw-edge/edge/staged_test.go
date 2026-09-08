// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package edge_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/blobgw-edge/capability"
	"github.com/scitrera/memorylayer-storage/blobgw-edge/edge"
	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
)

// mintStagedCap mints a STAGE or FINALIZE capability directly through the minter
// (the JSON mint helper does not expose content_hash), bound to (tenant, subject,
// op, ref) with the optional content-type/max-size (stage) or content-hash
// (finalize) that binds the upload.
func (h *harness) mintStagedCap(t *testing.T, tenant, subject string, op capability.Op, ref, contentType, contentHash string, maxSize int64) string {
	t.Helper()
	tok, _, err := h.minter.Mint(capability.Claims{
		Tenant: tenant, Subject: subject, Op: op, Ref: ref,
		ContentType: contentType, ContentHash: contentHash, MaxSize: maxSize,
	}, time.Minute)
	if err != nil {
		t.Fatalf("mint %s cap: %v", op, err)
	}
	return tok
}

// postStaged calls POST /staged/{ref} with a STAGE capability and decodes the
// StagedUpload response (or returns the status on failure).
func (h *harness) postStaged(t *testing.T, ref, token string) (gateway.StagedUpload, int) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, h.srv.URL+"/staged/"+ref+"?cap="+token, nil)
	resp, err := h.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("POST /staged: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return gateway.StagedUpload{}, resp.StatusCode
	}
	var out gateway.StagedUpload
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode StagedUpload: %v", err)
	}
	return out, resp.StatusCode
}

// postFinalize calls POST /finalize/{ref} with a FINALIZE capability and decodes
// the ObjectInfo response (or returns the status on failure).
func (h *harness) postFinalize(t *testing.T, ref, token string) (gateway.ObjectInfo, int) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, h.srv.URL+"/finalize/"+ref+"?cap="+token, nil)
	resp, err := h.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("POST /finalize: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return gateway.ObjectInfo{}, resp.StatusCode
	}
	var out gateway.ObjectInfo
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode ObjectInfo: %v", err)
	}
	return out, resp.StatusCode
}

// TestEdge_StageFinalizeRoundTrip drives the full external staged path: mint a
// STAGE cap → POST /staged returns a presigned StagedUpload; simulate the client
// PUT to the (in-memory) staging target; mint a FINALIZE cap → POST /finalize
// returns ObjectInfo carrying Size + ContentHash; then a fresh GET returns the
// exact bytes.
func TestEdge_StageFinalizeRoundTrip(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	const ref = "staged/report.pdf"
	const ct = "application/pdf"
	payload := randomBytes(t, 256*1024)
	wantHash := hex.EncodeToString(sha256Sum(payload))

	// 1. Stage: the edge mints a presigned upload bound to (tenant, ref).
	stageTok := h.mintStagedCap(t, "acme", "alice", capability.OpStage, ref, ct, "", 0)
	staged, st := h.postStaged(t, ref, stageTok)
	if st != http.StatusOK {
		t.Fatalf("POST /staged status %d", st)
	}
	if staged.Ref != ref || staged.UploadURL == "" || staged.StagingKey == "" {
		t.Fatalf("StagedUpload incomplete: %+v", staged)
	}

	// 2. Client uploads raw bytes to the presigned target (simulated in-memory).
	h.ltr.Staging.PutStaged(staged.StagingKey, payload)

	// 3. Finalize: the edge chunks+dedups the staged bytes and returns ObjectInfo.
	finTok := h.mintStagedCap(t, "acme", "alice", capability.OpFinalize, ref, "", "", 0)
	info, st := h.postFinalize(t, ref, finTok)
	if st != http.StatusOK {
		t.Fatalf("POST /finalize status %d", st)
	}
	if info.Size != int64(len(payload)) {
		t.Errorf("finalized size %d, want %d", info.Size, len(payload))
	}
	if info.ContentHash != wantHash {
		t.Errorf("finalized hash %q, want %q", info.ContentHash, wantHash)
	}

	// 4. The object is now readable via a GET capability.
	getTok, _ := h.mintCap(t, "acme", "alice", "GET", ref, "", 0)
	resp, err := h.srv.Client().Get(h.blobURL(ref, getTok))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, payload) {
		t.Errorf("download mismatch: %d vs %d bytes", len(got), len(payload))
	}
}

// TestEdge_StageFinalizeMintJSON proves the JSON mint endpoint accepts op=stage
// and op=finalize and returns the {token, capability_url} shape pointing at the
// new routes.
func TestEdge_StageFinalizeMintJSON(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	mint := func(op, ref string) (string, string, int) {
		body, _ := json.Marshal(map[string]any{"op": op, "ref": ref, "ttl_seconds": 600})
		req, _ := http.NewRequest(http.MethodPost, h.srv.URL+"/capabilities", bytes.NewReader(body))
		req.Header.Set("X-Auth-Tenant-ID", "acme")
		req.Header.Set("X-Scitrera-User", "alice")
		resp, err := h.srv.Client().Do(req)
		if err != nil {
			t.Fatalf("mint %s: %v", op, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return "", "", resp.StatusCode
		}
		var out struct {
			Token         string `json:"token"`
			CapabilityURL string `json:"capability_url"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return out.Token, out.CapabilityURL, resp.StatusCode
	}
	if tok, url, st := mint("STAGE", "s/obj"); st != http.StatusOK || tok == "" ||
		url != "/staged/s/obj?cap="+tok {
		t.Errorf("stage mint: st=%d url=%q", st, url)
	}
	if tok, url, st := mint("FINALIZE", "s/obj"); st != http.StatusOK || tok == "" ||
		url != "/finalize/s/obj?cap="+tok {
		t.Errorf("finalize mint: st=%d url=%q", st, url)
	}
}

// TestEdge_FinalizeContentHashMismatch proves a FINALIZE capability carrying a
// ContentHash binds the staged bytes: finalizing bytes that hash differently is
// rejected with 403 and leaves no readable ref (delete + deny, mirroring the
// direct PUT path).
func TestEdge_FinalizeContentHashMismatch(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	const ref = "staged/mismatch.bin"
	const ct = "application/octet-stream"
	payload := randomBytes(t, 4096)
	wrongHash := hex.EncodeToString(sha256Sum(randomBytes(t, 4096)))

	stageTok := h.mintStagedCap(t, "acme", "alice", capability.OpStage, ref, ct, "", 0)
	staged, st := h.postStaged(t, ref, stageTok)
	if st != http.StatusOK {
		t.Fatalf("POST /staged status %d", st)
	}
	h.ltr.Staging.PutStaged(staged.StagingKey, payload)

	// Finalize cap demands a hash the staged bytes do not have.
	finTok := h.mintStagedCap(t, "acme", "alice", capability.OpFinalize, ref, "", wrongHash, 0)
	_, st = h.postFinalize(t, ref, finTok)
	if st != http.StatusForbidden {
		t.Fatalf("hash-mismatch finalize: got %d, want 403", st)
	}

	// The rejected finalize must not leave a readable ref behind.
	getTok, _ := h.mintCap(t, "acme", "alice", "GET", ref, "", 0)
	resp, err := h.srv.Client().Get(h.blobURL(ref, getTok))
	if err != nil {
		t.Fatalf("GET after mismatch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("ref after rejected finalize: got %d, want 404", resp.StatusCode)
	}
}

// TestEdge_StageArmsIntegrityGate proves a STAGE capability's ContentHash flows
// into the gateway finalize integrity gate (via MintRefWithOptions ExpectedHash):
// staging then uploading bytes that disagree makes Finalize itself reject with
// ErrIntegrity, surfaced as 409, even when the FINALIZE cap carries no hash of
// its own. This is the server-side gate, complementing the edge's own post-
// finalize ContentHash-binding check.
func TestEdge_StageArmsIntegrityGate(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	const ref = "staged/armed.bin"
	const ct = "application/octet-stream"
	want := randomBytes(t, 4096)
	wantHash := hex.EncodeToString(sha256Sum(want))

	// STAGE cap carries the expected hash → arms the gateway gate at mint.
	stageTok := h.mintStagedCap(t, "acme", "alice", capability.OpStage, ref, ct, wantHash, 0)
	staged, st := h.postStaged(t, ref, stageTok)
	if st != http.StatusOK {
		t.Fatalf("POST /staged status %d", st)
	}
	// Upload DIFFERENT bytes than the armed hash.
	h.ltr.Staging.PutStaged(staged.StagingKey, randomBytes(t, 4096))

	// FINALIZE cap without its own hash: the gateway's armed gate must still reject.
	finTok := h.mintStagedCap(t, "acme", "alice", capability.OpFinalize, ref, "", "", 0)
	if _, st := h.postFinalize(t, ref, finTok); st != http.StatusConflict {
		t.Errorf("armed-gate finalize: got %d, want 409", st)
	}
}

// TestEdge_FinalizeUnknownRef proves finalizing a ref that was never minted for
// staging surfaces the gateway's NotFound sentinel as 404 (a ref with no pending
// staging row simply does not exist), distinct from the 409 the NotStaged/
// Integrity sentinels map to.
func TestEdge_FinalizeUnknownRef(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	const ref = "staged/never.bin"
	finTok := h.mintStagedCap(t, "acme", "alice", capability.OpFinalize, ref, "", "", 0)
	_, st := h.postFinalize(t, ref, finTok)
	if st != http.StatusNotFound {
		t.Errorf("finalize unknown ref: got %d, want 404", st)
	}
}

// TestEdge_StagedFinalizeRejectsCrossOpCrossRef proves the shared verification
// path binds the logical op + ref: a GET cap cannot drive /staged, a STAGE cap
// cannot drive /finalize, and a STAGE cap for one ref cannot stage another.
func TestEdge_StagedFinalizeRejectsCrossOpCrossRef(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})

	// A GET cap used on /staged → 403 (op not permitted).
	getTok, _ := h.mintCap(t, "acme", "alice", "GET", "x", "", 0)
	if _, st := h.postStaged(t, "x", getTok); st != http.StatusForbidden {
		t.Errorf("GET cap on /staged: got %d, want 403", st)
	}

	// A STAGE cap used on /finalize → 403 (op not permitted).
	stageTok := h.mintStagedCap(t, "acme", "alice", capability.OpStage, "x", "", "", 0)
	if _, st := h.postFinalize(t, "x", stageTok); st != http.StatusForbidden {
		t.Errorf("STAGE cap on /finalize: got %d, want 403", st)
	}

	// A STAGE cap for ref "x" used against ref "y" → 403 (ref not permitted).
	if _, st := h.postStaged(t, "y", stageTok); st != http.StatusForbidden {
		t.Errorf("STAGE cap cross-ref: got %d, want 403", st)
	}

	// A FINALIZE cap for ref "x" used against ref "y" → 403 (ref not permitted).
	finTok := h.mintStagedCap(t, "acme", "alice", capability.OpFinalize, "x", "", "", 0)
	if _, st := h.postFinalize(t, "y", finTok); st != http.StatusForbidden {
		t.Errorf("FINALIZE cap cross-ref: got %d, want 403", st)
	}
}

// TestEdge_StageMaxSizeGuard proves a STAGE mint applies the edge max-size
// ceiling exactly like a PUT mint: a max_size above the edge limit is rejected at
// mint (403).
func TestEdge_StageMaxSizeGuard(t *testing.T) {
	h := newHarness(t, edge.Config{MaxObjectSize: 1024}, edge.Deps{})
	body, _ := json.Marshal(map[string]any{"op": "STAGE", "ref": "big", "max_size": 4096, "ttl_seconds": 600})
	req, _ := http.NewRequest(http.MethodPost, h.srv.URL+"/capabilities", bytes.NewReader(body))
	req.Header.Set("X-Auth-Tenant-ID", "acme")
	req.Header.Set("X-Scitrera-User", "alice")
	resp, err := h.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("oversize STAGE mint: got %d, want 403", resp.StatusCode)
	}
}

// TestEdge_StagingGCReclaims is the StagingGC wiring smoke test: a minted-but-
// never-finalized staged upload leaves a pending ref row + staged bytes; a
// per-tenant gateway.StagingGC over the SHARED ref + staging stores (the exact
// pieces main.go wires) reclaims them once past the TTL, and leaves a still-fresh
// pending upload alone.
func TestEdge_StagingGCReclaims(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	const tenant = "acme"
	const ref = "staged/orphan.bin"

	// Mint + stage but never finalize → a pending row + staged bytes leak.
	stageTok := h.mintStagedCap(t, tenant, "alice", capability.OpStage, ref, "application/octet-stream", "", 0)
	staged, st := h.postStaged(t, ref, stageTok)
	if st != http.StatusOK {
		t.Fatalf("POST /staged status %d", st)
	}
	h.ltr.Staging.PutStaged(staged.StagingKey, randomBytes(t, 4096))

	// The pending row exists in the tenant's domain before the sweep.
	if _, ok, err := h.ltr.Refs.Get(context.Background(), tenant, ref); err != nil || !ok {
		t.Fatalf("expected pending ref before GC: ok=%v err=%v", ok, err)
	}

	// Sweep with a zero TTL so the just-created pending row is immediately stale,
	// over the SAME shared stores main.go feeds NewStagingGC (domain=tenant).
	sgc := gateway.NewStagingGC(h.ltr.Refs, h.ltr.Staging, tenant, 0, nil)
	res, err := sgc.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("StagingGC.RunOnce: %v", err)
	}
	if res.SlotsReclaimed != 1 {
		t.Errorf("SlotsReclaimed = %d, want 1", res.SlotsReclaimed)
	}
	if _, ok, _ := h.ltr.Refs.Get(context.Background(), tenant, ref); ok {
		t.Error("pending ref should be reclaimed after GC")
	}

	// A fresh pending upload (TTL not yet elapsed) must survive a sweep with a long
	// grace window.
	stageTok2 := h.mintStagedCap(t, tenant, "alice", capability.OpStage, "staged/fresh.bin", "application/octet-stream", "", 0)
	if _, st := h.postStaged(t, "staged/fresh.bin", stageTok2); st != http.StatusOK {
		t.Fatalf("second POST /staged status %d", st)
	}
	fresh := gateway.NewStagingGC(h.ltr.Refs, h.ltr.Staging, tenant, time.Hour, nil)
	res2, err := fresh.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("StagingGC.RunOnce (fresh): %v", err)
	}
	if res2.SlotsReclaimed != 0 {
		t.Errorf("fresh pending reclaimed prematurely: %d", res2.SlotsReclaimed)
	}
}

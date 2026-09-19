// SPDX-License-Identifier: AGPL-3.0-only
package edge_test

import (
	"bytes"
	"encoding/hex"
	"github.com/scitrera/memorylayer-storage/blobgw-edge/capability"
	"github.com/scitrera/memorylayer-storage/blobgw-edge/edge"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestReadCapabilityBindsResolvedContent(t *testing.T) {
	h := newHarness(t, edge.Config{}, edge.Deps{})
	const ref = "documents/doc/pages/page.png"
	original := []byte("original page")
	put := func(data []byte) {
		tok, _ := h.mintCap(t, "acme", "alice", "PUT", ref, "", 0)
		req, _ := http.NewRequest("PUT", h.blobURL(ref, tok), bytes.NewReader(data))
		res, err := h.srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != 201 {
			t.Fatalf("put: %d", res.StatusCode)
		}
	}
	put(original)
	for _, op := range []capability.Op{capability.OpGet, capability.OpHead} {
		tok, _, err := h.minter.Mint(capability.Claims{Tenant: "acme", Subject: "alice", Op: op, Ref: ref, ContentHash: hex.EncodeToString(sha256Sum(original))}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		check := func(want int) {
			req, _ := http.NewRequest(string(op), h.blobURL(ref, tok), nil)
			res, err := h.srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, _ := io.ReadAll(res.Body)
			if res.StatusCode != want {
				t.Fatalf("%s: got %d want %d: %s", op, res.StatusCode, want, body)
			}
			if want == 412 && bytes.Contains(body, []byte("replacement page")) {
				t.Fatal("replacement leaked")
			}
		}
		check(200)
		put([]byte("replacement page"))
		check(412)
		put(original)
	}
}

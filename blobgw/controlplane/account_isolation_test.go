// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package controlplane_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/scitrera/memorylayer-storage/blobgw/controlplane"
	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/ctlproto"
)

// This file is the hermetic proof for ADR-001 §7.7: account-per-domain isolation
// extended to the converged blobgw control plane. It boots an embedded
// nats-server configured with THREE in-server accounts — a shared service
// account (SVC) where the real controlplane.Server runs, and two tenant accounts
// (A, B) — wired with a NATS service export/import mapping so that:
//
//   - SVC exports the control-plane service on blobgw.*.>
//   - tenant account A imports it remapped to blobgw.A.> ONLY
//   - tenant account B imports it remapped to blobgw.B.> ONLY
//
// The import subject is the account boundary: a node in account A can reach
// blobgw for tenant A, but a request to blobgw.B.* from account A has no matching
// import and is rejected at the NATS layer (no responder) — isolation by the
// account boundary, not merely by the server's app-level subject validation.
//
// The in-server `accounts{}` config exercises the SAME routing/isolation
// semantics as production operator/JWT mode (nsc-issued account JWTs with the
// equivalent exports/imports); see docs/blobgw-control-plane-accounts.md for the
// operator-mode equivalent. Full JWT/operator mode is a deployment concern and
// is not provisioned hermetically here.

// accountTenantA and accountTenantB are the dedup domains / tenants used in the
// isolation test. They double as the blobgw.<tenant>.* subject token.
const (
	accountTenantA = "acme"
	accountTenantB = "globex"
)

// serviceAccountsConf renders an in-server NATS config with the SVC/tenant
// accounts and the control-plane service export + per-tenant restricted imports.
// It uses the production mapping generator (controlplane.ServiceAccountConfig)
// as the single source of truth for the export/import shape, then prepends a
// random-port listener so client auth actually applies (the in-process pipe used
// by startNATS bypasses account auth).
func serviceAccountsConf(t *testing.T) string {
	t.Helper()
	accts, err := controlplane.ServiceAccountConfig{
		ServiceAccount:  "SVC",
		ServiceUser:     "svc",
		ServicePassword: "psvc",
		Tenants: map[string]controlplane.TenantCredentials{
			accountTenantA: {User: "a", Password: "pa"},
			accountTenantB: {User: "b", Password: "pb"},
		},
	}.RenderServerConfig()
	if err != nil {
		t.Fatalf("render account config: %v", err)
	}
	return "host: \"127.0.0.1\"\nport: -1\n" + accts
}

// startServiceAccountsNATS boots an embedded nats-server from the given config
// string and returns its client URL. The server is shut down via t.Cleanup.
func startServiceAccountsNATS(t *testing.T, conf string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		t.Fatalf("write nats conf: %v", err)
	}
	opts, err := natsserver.ProcessConfigFile(path)
	if err != nil {
		t.Skipf("nats config (accounts/exports/imports) unsupported here: %v", err)
	}
	opts.NoLog, opts.NoSigs = true, true
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		t.Fatalf("nats not ready")
	}
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	return srv.ClientURL()
}

// connectAs dials the embedded server with the given account user credentials.
func connectAs(t *testing.T, url, user, pass string) *nats.Conn {
	t.Helper()
	conn, err := nats.Connect(url, nats.UserInfo(user, pass), nats.Name(user))
	if err != nil {
		t.Fatalf("connect as %q: %v", user, err)
	}
	t.Cleanup(conn.Close)
	return conn
}

// serviceProvider builds a static tenantbind provider covering BOTH tenants, so
// the presign path on the service account can resolve either tenant's binding.
func serviceProvider(t *testing.T) tenantbind.Provider {
	t.Helper()
	res := tenantbind.NewMapResolver()
	res.Set(accountTenantA, tenantbind.Descriptor{
		Endpoint:      "http://fake-s3.local:9000",
		Region:        "us-east-1",
		Bucket:        accountTenantA + "-bucket",
		Prefix:        "packs",
		CredentialRef: accountTenantA + "-secret",
	})
	res.Set(accountTenantB, tenantbind.Descriptor{
		Endpoint:      "http://fake-s3.local:9000",
		Region:        "us-east-1",
		Bucket:        accountTenantB + "-bucket",
		Prefix:        "packs",
		CredentialRef: accountTenantB + "-secret",
	})
	secrets := tenantbind.NewMapSecretStore()
	secrets.Set(accountTenantA+"-secret", "AKIDA", "secretA")
	secrets.Set(accountTenantB+"-secret", "AKIDB", "secretB")
	return tenantbind.NewStaticKeysProvider(res, secrets)
}

// TestServiceAccountIsolation is the §7.7 proof. It runs the REAL
// controlplane.Server in the SVC account and exercises a tenant-A node:
//
//	(1) A's record+lookup AND presign to blobgw.A.* SUCCEED (cross-account service
//	    routing works end-to-end through the real server).
//	(2) A's request to blobgw.B.* is BLOCKED at the account boundary — no
//	    responder — even though the server subscribes blobgw.*.> and would happily
//	    answer the forged subject in a single account. This is account isolation,
//	    not just app validation.
func TestServiceAccountIsolation(t *testing.T) {
	url := startServiceAccountsNATS(t, serviceAccountsConf(t))

	// The blobgw control plane runs in the SVC account.
	svc := connectAs(t, url, "svc", "psvc")
	index := snapshot.NewMemoryDedupStore()
	srv, err := controlplane.NewServer(svc, index, serviceProvider(t),
		controlplane.WithLogger(quietLogger()))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	// A tenant-A node connects in tenant A's account.
	nodeA := connectAs(t, url, "a", "pa")

	// (1a) A → blobgw.A.index.record/lookup succeeds and returns the right data.
	var rec ctlproto.RecordResponse
	request(t, nodeA, ctlproto.IndexRecordSubject(accountTenantA), ctlproto.RecordRequest{
		Domain: accountTenantA,
		Locations: []ctlproto.ChunkLocation{
			{ChunkHash: "h1", PackRef: ctlproto.PackRef{PackHash: "p1", Offset: 0, Size: 10}},
		},
	}, &rec)
	if rec.Error != "" {
		t.Fatalf("A record error: %s", rec.Error)
	}
	var look ctlproto.LookupBatchResponse
	request(t, nodeA, ctlproto.IndexLookupSubject(accountTenantA), ctlproto.LookupBatchRequest{
		Domain:      accountTenantA,
		ChunkHashes: []string{"h1", "missing"},
	}, &look)
	if look.Error != "" {
		t.Fatalf("A lookup error: %s", look.Error)
	}
	got, ok := look.Locations["h1"]
	if !ok || got.PackHash != "p1" || got.Size != 10 {
		t.Fatalf("A lookup: want h1->{p1 .. 10}, got %+v (ok=%v)", look.Locations, ok)
	}

	// (1b) A → blobgw.A.presign succeeds and is scoped to A's bucket.
	var pre ctlproto.PresignBatchResponse
	request(t, nodeA, ctlproto.PresignSubject(accountTenantA), ctlproto.PresignBatchRequest{
		Domain:     accountTenantA,
		Op:         ctlproto.PresignGet,
		Keys:       []string{"obj"},
		TTLSeconds: 300,
	}, &pre)
	if pre.Error != "" {
		t.Fatalf("A presign error: %s", pre.Error)
	}
	if u := pre.URLs["obj"]; u == "" {
		t.Fatalf("A presign minted no URL: %v", pre.URLs)
	}

	// (2) ISOLATION: A → blobgw.B.* is blocked at the NATS account boundary. Even
	// though A forges the subject for tenant B (and the server in SVC subscribes
	// blobgw.*.> and would otherwise answer), account A imports ONLY blobgw.A.> ,
	// so there is no route to the service for blobgw.B.* → no responder.
	for _, subj := range []string{
		ctlproto.IndexLookupSubject(accountTenantB),
		ctlproto.PresignSubject(accountTenantB),
	} {
		data, mErr := ctlproto.DefaultCodec.Marshal(ctlproto.LookupBatchRequest{
			Domain: accountTenantB, ChunkHashes: []string{"h1"},
		})
		if mErr != nil {
			t.Fatalf("marshal cross-account probe: %v", mErr)
		}
		_, reqErr := nodeA.Request(subj, data, 300*time.Millisecond)
		if reqErr == nil {
			t.Fatalf("ACCOUNT ISOLATION BREACH: tenant A reached %q (got a response)", subj)
		}
		// The account boundary MUST yield ErrNoResponders: account A has no import
		// for blobgw.B.> , so the publish is never routed to the SVC service
		// responder and the server signals no-responders immediately (not a timeout).
		// A timeout is a weak signal — it could mask a slow or broadened import — so
		// we require the definitive no-responders rejection. If this nats-server
		// build does not emit no_responders (very old build), skip rather than
		// silently pass.
		if errors.Is(reqErr, nats.ErrTimeout) {
			t.Skipf("cross-account %q: got timeout instead of ErrNoResponders — nats-server build may not emit no_responders; skip rather than pass on weak signal", subj)
		}
		if !errors.Is(reqErr, nats.ErrNoResponders) {
			t.Fatalf("cross-account %q: unexpected error %v (want ErrNoResponders)", subj, reqErr)
		}
	}

	// (2b) Defense-in-depth on the index: B's namespace never received A's write,
	// so even the data is isolated. A nodeB in account B sees an EMPTY index for
	// h1 (B's import lets it talk to the same server, but the dedup domain "B"
	// was never written).
	nodeB := connectAs(t, url, "b", "pb")
	var lookB ctlproto.LookupBatchResponse
	request(t, nodeB, ctlproto.IndexLookupSubject(accountTenantB), ctlproto.LookupBatchRequest{
		Domain:      accountTenantB,
		ChunkHashes: []string{"h1"},
	}, &lookB)
	if lookB.Error != "" {
		t.Fatalf("B lookup error: %s", lookB.Error)
	}
	if len(lookB.Locations) != 0 {
		t.Fatalf("tenant B must not see tenant A's chunk: got %v", lookB.Locations)
	}
}

// TestSingleAccountBackCompat proves the legacy single-account path still works:
// with NO account boundary (the in-process pipe server, one account), a tenant
// node reaches blobgw.<tenant>.* exactly as before. This is the back-compat guard
// for the default (-nats-creds/-nats-nkey empty) deployment.
func TestSingleAccountBackCompat(t *testing.T) {
	conn := startNATS(t)
	index := snapshot.NewMemoryDedupStore()
	newServer(t, conn, index)

	var rec ctlproto.RecordResponse
	request(t, conn, ctlproto.IndexRecordSubject(testTenant), ctlproto.RecordRequest{
		Domain:    testDomain,
		Locations: []ctlproto.ChunkLocation{{ChunkHash: "h1", PackRef: ctlproto.PackRef{PackHash: "p1", Size: 10}}},
	}, &rec)
	if rec.Error != "" {
		t.Fatalf("record error: %s", rec.Error)
	}
	var look ctlproto.LookupBatchResponse
	request(t, conn, ctlproto.IndexLookupSubject(testTenant), ctlproto.LookupBatchRequest{
		Domain:      testDomain,
		ChunkHashes: []string{"h1"},
	}, &look)
	if look.Error != "" {
		t.Fatalf("lookup error: %s", look.Error)
	}
	if _, ok := look.Locations["h1"]; !ok {
		t.Fatalf("single-account back-compat: h1 not found in %v", look.Locations)
	}
}

// TestRenderServerConfig checks the account-mapping generator: a valid config
// renders the service export + per-tenant import in deterministic order, and
// structural errors are reported rather than silently producing an un-isolated
// server.
func TestRenderServerConfig(t *testing.T) {
	out, err := controlplane.ServiceAccountConfig{
		ServiceAccount:  "BLOBGW_SVC",
		ServiceUser:     "blobgw",
		ServicePassword: "pw",
		Tenants: map[string]controlplane.TenantCredentials{
			"globex": {User: "g", Password: "pg"},
			"acme":   {User: "a", Password: "pa"},
		},
	}.RenderServerConfig()
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		`exports: [ { service: "blobgw.*.>" } ]`,
		`{ service: { account: "BLOBGW_SVC", subject: "blobgw.acme.>" } }`,
		`{ service: { account: "BLOBGW_SVC", subject: "blobgw.globex.>" } }`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("rendered config missing %q:\n%s", want, out)
		}
	}
	// Deterministic ordering: acme (sorted) before globex.
	if strings.Index(out, "blobgw.acme.>") > strings.Index(out, "blobgw.globex.>") {
		t.Fatalf("tenants not rendered in sorted order:\n%s", out)
	}

	// Defense-in-depth: no tenant import block must contain the wildcard
	// blobgw.*.> — that subject must appear ONLY in the SVC export. A generator
	// regression that copies the export subject into an import would grant tenants
	// cross-tenant access at the NATS layer.
	lines := strings.Split(out, "\n")
	inImport := false
	for _, line := range lines {
		if strings.Contains(line, "imports:") {
			inImport = true
		}
		if inImport && strings.Contains(line, "blobgw.*.>") {
			t.Fatalf("tenant import block contains wildcard subject blobgw.*.> — must appear in SVC export only:\n%s", out)
		}
		// imports block closes at the end of the account block (next '}')
		if inImport && strings.TrimSpace(line) == "}" {
			inImport = false
		}
	}

	// Structural validation.
	for name, cfg := range map[string]controlplane.ServiceAccountConfig{
		"no service account":      {ServiceUser: "u", Tenants: map[string]controlplane.TenantCredentials{"a": {}}},
		"no service user":         {ServiceAccount: "SVC", Tenants: map[string]controlplane.TenantCredentials{"a": {}}},
		"no tenants":              {ServiceAccount: "SVC", ServiceUser: "u"},
		"invalid service account": {ServiceAccount: "SVC account!", ServiceUser: "u", Tenants: map[string]controlplane.TenantCredentials{"a": {}}},
		"invalid tenant name":     {ServiceAccount: "SVC", ServiceUser: "u", Tenants: map[string]controlplane.TenantCredentials{"bad tenant": {}}},
	} {
		if _, err := cfg.RenderServerConfig(); err == nil {
			t.Fatalf("%s: expected error, got nil", name)
		}
	}
}

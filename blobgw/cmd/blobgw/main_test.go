// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/scitrera/memorylayer-storage/blobgw/controlplane"
	"github.com/scitrera/memorylayer-storage/blobgw/tenantconfig"
	"github.com/scitrera/memorylayer-storage/ctlproto"
)

// TestBuild_DefaultsHTTPOnly proves the zero-flag default still assembles the
// HTTP data path (memory backends) with no control-plane involvement — the
// existing behavior must be unchanged when -control-plane is not passed.
func TestBuild_DefaultsHTTPOnly(t *testing.T) {
	o := defaultOptions(t.TempDir())
	b, err := build(context.Background(), o)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer b.cleanup()
	if b.gw == nil || b.gc == nil || b.ready == nil || b.dedup == nil {
		t.Fatalf("build returned incomplete components: %+v", b)
	}
	if err := b.ready(context.Background()); err != nil {
		t.Fatalf("readiness probe (memory backends): %v", err)
	}
}

// TestControlPlaneSmoke boots an embedded NATS server on a real ephemeral port,
// assembles the daemon's shared stores via build() (memory index), loads the
// per-tenant provider from a tenant-config file exactly as startControlPlane
// does, starts a controlplane.Server over that shared index, and exercises one
// record+lookup RPC end-to-end. This is the smoke test that the wired control
// plane Starts and answers an index request.
func TestControlPlaneSmoke(t *testing.T) {
	url := startNATSServer(t)

	// tenant-config + secrets on disk, as the operator would provide.
	configPath := writeFile(t, "tenants.json", `{
  "tenants": {
    "acme": {
      "endpoint": "https://s3.example.com",
      "region": "us-east-1",
      "bucket": "tenant-acme",
      "prefix": "packs",
      "credentialRef": "acme"
    }
  }
}`)
	secretsPath := writeFile(t, "secrets.json", `{"acme":{"accessKey":"AK","secretKey":"SK"}}`)

	o := defaultOptions(t.TempDir())
	b, err := build(context.Background(), o) // memory index/refs/staging
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer b.cleanup()

	provider, err := tenantconfig.LoadProvider(configPath, secretsPath, time.Minute)
	if err != nil {
		t.Fatalf("LoadProvider: %v", err)
	}

	nc, err := nats.Connect(url, nats.Name("blobgw-test"))
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	defer nc.Close()

	cp, err := controlplane.NewServer(nc, b.dedup, provider)
	if err != nil {
		t.Fatalf("new control-plane server: %v", err)
	}
	if err := cp.Start(context.Background()); err != nil {
		t.Fatalf("start control-plane server: %v", err)
	}
	defer func() { _ = cp.Close() }()

	// Record one chunk location, then look it up — the index RPC round-trip.
	codec := ctlproto.DefaultCodec
	recReq, _ := codec.Marshal(ctlproto.RecordRequest{
		Domain: "acme",
		Locations: []ctlproto.ChunkLocation{
			{ChunkHash: "h1", PackRef: ctlproto.PackRef{PackHash: "p1", Offset: 0, Size: 10}},
		},
	})
	recMsg, err := nc.Request(ctlproto.IndexRecordSubject("acme"), recReq, 3*time.Second)
	if err != nil {
		t.Fatalf("record request: %v", err)
	}
	var recResp ctlproto.RecordResponse
	if err := codec.Unmarshal(recMsg.Data, &recResp); err != nil {
		t.Fatalf("unmarshal record resp: %v", err)
	}
	if recResp.Error != "" {
		t.Fatalf("record error: %s", recResp.Error)
	}

	lookReq, _ := codec.Marshal(ctlproto.LookupBatchRequest{
		Domain:      "acme",
		ChunkHashes: []string{"h1", "missing"},
	})
	lookMsg, err := nc.Request(ctlproto.IndexLookupSubject("acme"), lookReq, 3*time.Second)
	if err != nil {
		t.Fatalf("lookup request: %v", err)
	}
	var lookResp ctlproto.LookupBatchResponse
	if err := codec.Unmarshal(lookMsg.Data, &lookResp); err != nil {
		t.Fatalf("unmarshal lookup resp: %v", err)
	}
	if lookResp.Error != "" {
		t.Fatalf("lookup error: %s", lookResp.Error)
	}
	got, ok := lookResp.Locations["h1"]
	if !ok {
		t.Fatalf("h1 not found in %v", lookResp.Locations)
	}
	if got.PackHash != "p1" || got.Size != 10 {
		t.Fatalf("h1 = %+v, want {p1 ... 10}", got)
	}
	if _, ok := lookResp.Locations["missing"]; ok {
		t.Fatalf("missing hash should be absent")
	}
}

// defaultOptions returns options equivalent to the zero-flag daemon default
// (local/memory/memory), with a temp data dir so build's local stores land in a
// test-scoped directory.
func defaultOptions(dataDir string) options {
	return options{
		domain:             "default",
		compression:        "zstd",
		backend:            "local",
		dataDir:            dataDir,
		index:              "memory",
		staging:            "memory",
		gcSafetyWind:       time.Hour,
		credentialCacheTTL: tenantconfig.DefaultCacheTTL,
		presignMaxTTL:      15 * time.Minute,
	}
}

// startNATSServer boots an embedded NATS server on an ephemeral TCP port and
// returns its client URL. It is cleaned up via t.Cleanup.
func startNATSServer(t *testing.T) string {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1})
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
	return srv.ClientURL()
}

// writeFile writes content to a temp file under t.TempDir and returns its path.
func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

// TestLoadManifestDSNs covers the {tenant: dsn} Secret parser that routes
// converged mlfs -remote tenants' GC manifest reads to PostgreSQL. An empty path
// is the all-S3 posture (nil map); a malformed file or empty DSN must fail loudly
// rather than silently demote a converged tenant to the S3 fallback (which would
// make GC read an empty live set and reclaim live chunks after the safety window).
func TestLoadManifestDSNs(t *testing.T) {
	t.Run("empty path is all-S3 (nil)", func(t *testing.T) {
		m, err := loadManifestDSNs("")
		if err != nil {
			t.Fatalf("empty path: unexpected error %v", err)
		}
		if m != nil {
			t.Fatalf("empty path should yield nil map, got %v", m)
		}
	})

	t.Run("valid map parses", func(t *testing.T) {
		path := writeFile(t, "dsns.json", `{"t1":"postgres://h/db1","t2":"postgres://h/db2"}`)
		m, err := loadManifestDSNs(path)
		if err != nil {
			t.Fatalf("valid map: %v", err)
		}
		if m["t1"] != "postgres://h/db1" || m["t2"] != "postgres://h/db2" {
			t.Fatalf("parsed map mismatch: %v", m)
		}
	})

	t.Run("env vars in DSN are expanded", func(t *testing.T) {
		t.Setenv("MFTEST_PASS", "s3cr3t")
		path := writeFile(t, "expand.json", `{"t1":"postgres://u:${MFTEST_PASS}@h/db"}`)
		m, err := loadManifestDSNs(path)
		if err != nil {
			t.Fatalf("expand: %v", err)
		}
		if m["t1"] != "postgres://u:s3cr3t@h/db" {
			t.Fatalf("env not expanded: %q", m["t1"])
		}
	})

	t.Run("DSN expanding to empty is rejected", func(t *testing.T) {
		// An unset placeholder expands to "" and must fail loud, not silently
		// demote a converged tenant to the empty-live-set S3 path.
		path := writeFile(t, "expand-empty.json", `{"t1":"${MFTEST_DEFINITELY_UNSET_VAR}"}`)
		if _, err := loadManifestDSNs(path); err == nil {
			t.Fatal("DSN expanding to empty should be rejected")
		}
	})

	t.Run("missing file errors", func(t *testing.T) {
		if _, err := loadManifestDSNs(filepath.Join(t.TempDir(), "nope.json")); err == nil {
			t.Fatal("missing file should error")
		}
	})

	t.Run("malformed JSON errors", func(t *testing.T) {
		path := writeFile(t, "bad.json", `{not json`)
		if _, err := loadManifestDSNs(path); err == nil {
			t.Fatal("malformed JSON should error")
		}
	})

	t.Run("empty DSN rejected", func(t *testing.T) {
		path := writeFile(t, "emptydsn.json", `{"t1":""}`)
		if _, err := loadManifestDSNs(path); err == nil {
			t.Fatal("empty DSN should be rejected so a converged tenant is never silently demoted to S3")
		}
	})

	t.Run("empty tenant key rejected", func(t *testing.T) {
		path := writeFile(t, "emptykey.json", `{"":"postgres://h/db"}`)
		if _, err := loadManifestDSNs(path); err == nil {
			t.Fatal("empty tenant key should be rejected")
		}
	})
}

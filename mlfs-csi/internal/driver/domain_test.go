// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package driver

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateDomain(t *testing.T) {
	ok := []string{"acme", "tenant-1", "a_b", "ABC123", "x"}
	for _, d := range ok {
		if err := validateDomain(d); err != nil {
			t.Errorf("validateDomain(%q) = %v, want nil", d, err)
		}
	}
	bad := []string{"", "has.dot", "wild*", "deep>", "with/slash", "space bar"}
	for _, d := range bad {
		if err := validateDomain(d); err == nil {
			t.Errorf("validateDomain(%q) = nil, want error", d)
		}
	}
	long := make([]byte, 64)
	for i := range long {
		long[i] = 'a'
	}
	if err := validateDomain(string(long)); err == nil {
		t.Errorf("validateDomain(64 chars) = nil, want length error")
	}
}

func TestDomainDirKeyStableAndDistinct(t *testing.T) {
	a1 := domainDirKey("acme")
	a2 := domainDirKey("acme")
	b := domainDirKey("beta")
	if a1 != a2 {
		t.Errorf("domainDirKey not stable: %q != %q", a1, a2)
	}
	if a1 == b {
		t.Errorf("domainDirKey collision for distinct domains: %q", a1)
	}
}

func TestDomainSpecFromRequest(t *testing.T) {
	// No domain ==> legacy path (nil, nil).
	spec, err := domainSpecFromRequest("node-1", "", map[string]string{}, map[string]string{})
	if err != nil || spec != nil {
		t.Fatalf("legacy: got (%v, %v), want (nil, nil)", spec, err)
	}

	volCtx := map[string]string{paramDomain: "acme", paramNATSURL: "nats://r3:4222", paramRegion: "7"}
	secrets := map[string]string{secretNATSCreds: "CREDS", secretMetaDSN: "postgres://pg/acme"}
	spec, err = domainSpecFromRequest("node-1", "nats://default:4222", volCtx, secrets)
	if err != nil {
		t.Fatalf("full spec: %v", err)
	}
	if spec.Domain != "acme" || spec.NATSURL != "nats://r3:4222" || spec.Region != 7 ||
		spec.Creds != "CREDS" || spec.MetaDSN != "postgres://pg/acme" || spec.NodeID != "node-1" {
		t.Fatalf("unexpected spec: %+v", spec)
	}

	// Domain may come from the (per-namespace) secret instead of a param, so one
	// StorageClass can serve many tenants (§7.2).
	spec, err = domainSpecFromRequest("node-1", "nats://default:4222",
		map[string]string{},
		map[string]string{secretDomain: "acme", secretNATSCreds: "C", secretMetaDSN: "dsn"})
	if err != nil || spec == nil || spec.Domain != "acme" {
		t.Fatalf("domain-from-secret: spec=%+v err=%v", spec, err)
	}
	// Param wins over secret when both are present.
	spec, err = domainSpecFromRequest("node-1", "nats://default:4222",
		map[string]string{paramDomain: "fromparam"},
		map[string]string{secretDomain: "fromsecret", secretNATSCreds: "C", secretMetaDSN: "dsn"})
	if err != nil || spec.Domain != "fromparam" {
		t.Fatalf("param should win: spec=%+v err=%v", spec, err)
	}

	// natsUrl backfilled from the default when the param is absent.
	spec, err = domainSpecFromRequest("n", "nats://default:4222",
		map[string]string{paramDomain: "acme"}, secrets)
	if err != nil || spec.NATSURL != "nats://default:4222" {
		t.Fatalf("default nats backfill: spec=%+v err=%v", spec, err)
	}

	// Missing creds / DSN are hard errors.
	if _, err := domainSpecFromRequest("n", "nats://d", map[string]string{paramDomain: "acme"},
		map[string]string{secretMetaDSN: "dsn"}); err == nil {
		t.Errorf("missing creds: want error")
	}
	if _, err := domainSpecFromRequest("n", "nats://d", map[string]string{paramDomain: "acme"},
		map[string]string{secretNATSCreds: "c"}); err == nil {
		t.Errorf("missing dsn: want error")
	}
	// Missing nats url (no param, no default) is an error.
	if _, err := domainSpecFromRequest("n", "", map[string]string{paramDomain: "acme"}, secrets); err == nil {
		t.Errorf("missing nats url: want error")
	}
	// Invalid domain token rejected.
	if _, err := domainSpecFromRequest("n", "nats://d", map[string]string{paramDomain: "bad.dom"}, secrets); err == nil {
		t.Errorf("invalid domain: want error")
	}
}

func TestSubdirPermsFromContext(t *testing.T) {
	// Empty map => all nil fields (no change).
	p, err := subdirPermsFromContext(map[string]string{})
	if err != nil {
		t.Fatalf("empty: unexpected error: %v", err)
	}
	if p.Mode != nil || p.UID != nil || p.GID != nil {
		t.Errorf("empty: got %+v, want all nil", p)
	}

	// dirMode "0777" => Mode set to 0o777 perms.
	p, err = subdirPermsFromContext(map[string]string{paramDirMode: "0777"})
	if err != nil {
		t.Fatalf("dirMode 0777: unexpected error: %v", err)
	}
	if p.Mode == nil || p.Mode.Perm() != 0o777 {
		t.Errorf("dirMode 0777: got Mode=%v, want perm 0777", p.Mode)
	}

	// dirMode "2775" => Mode has os.ModeSetgid and 0o775 perms.
	p, err = subdirPermsFromContext(map[string]string{paramDirMode: "2775"})
	if err != nil {
		t.Fatalf("dirMode 2775: unexpected error: %v", err)
	}
	if p.Mode == nil || p.Mode.Perm() != 0o775 || (*p.Mode)&os.ModeSetgid == 0 {
		t.Errorf("dirMode 2775: got Mode=%v, want setgid + perm 0775", p.Mode)
	}

	// uid/gid set.
	p, err = subdirPermsFromContext(map[string]string{paramUID: "1000", paramGID: "1000"})
	if err != nil {
		t.Fatalf("uid/gid: unexpected error: %v", err)
	}
	if p.UID == nil || *p.UID != 1000 || p.GID == nil || *p.GID != 1000 {
		t.Errorf("uid/gid: got UID=%v GID=%v, want both 1000", p.UID, p.GID)
	}

	// Invalid dirMode (not a number).
	if _, err := subdirPermsFromContext(map[string]string{paramDirMode: "abc"}); err == nil {
		t.Errorf("dirMode abc: want error")
	}

	// Invalid dirMode (exceeds 0o7777).
	if _, err := subdirPermsFromContext(map[string]string{paramDirMode: "99999"}); err == nil {
		t.Errorf("dirMode 99999: want error")
	}

	// Invalid uid (negative).
	if _, err := subdirPermsFromContext(map[string]string{paramUID: "-1"}); err == nil {
		t.Errorf("uid -1: want error")
	}
}

func TestSubdirPermsApply(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "subdir")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	mode := os.FileMode(0o777)
	p := SubdirPerms{Mode: &mode}
	if err := p.apply(sub); err != nil {
		t.Fatalf("apply: %v", err)
	}

	info, err := os.Stat(sub)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o777 {
		t.Errorf("got perm %o, want 0777", info.Mode().Perm())
	}
	// Skip uid/gid chown assertion: non-root test runners can't chown.
}

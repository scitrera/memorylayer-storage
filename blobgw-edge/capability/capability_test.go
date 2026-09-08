// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package capability

import (
	"errors"
	"testing"
	"time"
)

func fixedClock(t time.Time) func() time.Time { return func() time.Time { return t } }

func newPair(t *testing.T, audience string) (*Minter, *Verifier) {
	t.Helper()
	pub, priv, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	m := NewMinter("k1", priv, audience)
	v := NewVerifier(audience)
	v.AddKey("k1", pub)
	return m, v
}

func sampleClaims() Claims {
	return Claims{Tenant: "t1", Subject: "u1", Op: OpGet, Ref: "docs/a.png"}
}

func TestMintVerifyRoundTrip(t *testing.T) {
	m, v := newPair(t, "edge-1")
	now := time.Unix(1_700_000_000, 0).UTC()
	m.SetClock(fixedClock(now))
	v.SetClock(fixedClock(now.Add(time.Minute)))

	token, claims, err := m.Mint(sampleClaims(), 15*time.Minute)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	got, err := v.Verify(token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.Tenant != "t1" || got.Op != OpGet || got.Ref != "docs/a.png" {
		t.Errorf("claims mismatch: %+v", got)
	}
	if got.JTI == "" || got.JTI != claims.JTI {
		t.Errorf("jti not stable: %q vs %q", got.JTI, claims.JTI)
	}
	if got.Audience != "edge-1" {
		t.Errorf("audience: %q", got.Audience)
	}
}

// TestStageFinalizeRoundTrip proves the STAGE/FINALIZE ops round-trip through
// mint+verify carrying the fields that bind the upload: a STAGE cap carries the
// ContentType/MaxSize the presign is bounded by, and a FINALIZE cap carries the
// ContentHash the finalized bytes must match.
func TestStageFinalizeRoundTrip(t *testing.T) {
	m, v := newPair(t, "edge-1")
	now := time.Unix(1_700_000_000, 0).UTC()
	m.SetClock(fixedClock(now))
	v.SetClock(fixedClock(now.Add(time.Minute)))

	stageTok, _, err := m.Mint(Claims{
		Tenant: "t1", Subject: "u1", Op: OpStage, Ref: "docs/a.png",
		ContentType: "image/png", MaxSize: 4096,
	}, 15*time.Minute)
	if err != nil {
		t.Fatalf("Mint stage: %v", err)
	}
	gotStage, err := v.Verify(stageTok)
	if err != nil {
		t.Fatalf("Verify stage: %v", err)
	}
	if gotStage.Op != OpStage || gotStage.Ref != "docs/a.png" ||
		gotStage.ContentType != "image/png" || gotStage.MaxSize != 4096 {
		t.Errorf("stage claims mismatch: %+v", gotStage)
	}

	finTok, _, err := m.Mint(Claims{
		Tenant: "t1", Subject: "u1", Op: OpFinalize, Ref: "docs/a.png",
		ContentHash: "deadbeef",
	}, 15*time.Minute)
	if err != nil {
		t.Fatalf("Mint finalize: %v", err)
	}
	gotFin, err := v.Verify(finTok)
	if err != nil {
		t.Fatalf("Verify finalize: %v", err)
	}
	if gotFin.Op != OpFinalize || gotFin.Ref != "docs/a.png" || gotFin.ContentHash != "deadbeef" {
		t.Errorf("finalize claims mismatch: %+v", gotFin)
	}
}

func TestExpired(t *testing.T) {
	m, v := newPair(t, "e")
	now := time.Unix(1_700_000_000, 0).UTC()
	m.SetClock(fixedClock(now))
	token, _, _ := m.Mint(sampleClaims(), time.Minute)
	v.SetClock(fixedClock(now.Add(2 * time.Minute)))
	if _, err := v.Verify(token); !errors.Is(err, ErrExpired) {
		t.Errorf("expected ErrExpired, got %v", err)
	}
}

func TestNotYetValid(t *testing.T) {
	m, v := newPair(t, "e")
	now := time.Unix(1_700_000_000, 0).UTC()
	m.SetClock(fixedClock(now))
	c := sampleClaims()
	c.NotBefore = now.Add(10 * time.Minute).Unix()
	token, _, _ := m.Mint(c, time.Hour)
	v.SetClock(fixedClock(now))
	if _, err := v.Verify(token); !errors.Is(err, ErrNotYetValid) {
		t.Errorf("expected ErrNotYetValid, got %v", err)
	}
}

func TestTamperedPayloadAndSignature(t *testing.T) {
	m, v := newPair(t, "e")
	token, _, _ := m.Mint(sampleClaims(), time.Hour)

	// Flip a character in the payload segment.
	bad := []byte(token)
	bad[0] ^= 0x01
	if _, err := v.Verify(string(bad)); err == nil {
		t.Error("tampered payload should fail")
	}

	// Flip a character in the signature segment.
	dot := 0
	for i, ch := range token {
		if ch == '.' {
			dot = i
			break
		}
	}
	bad2 := []byte(token)
	bad2[dot+1] ^= 0x01
	if _, err := v.Verify(string(bad2)); !errors.Is(err, ErrBadSignature) && err == nil {
		t.Error("tampered signature should fail")
	}
}

func TestUnknownKey(t *testing.T) {
	m, _ := newPair(t, "e")
	token, _, _ := m.Mint(sampleClaims(), time.Hour)
	other := NewVerifier("e") // no keys registered
	if _, err := other.Verify(token); !errors.Is(err, ErrUnknownKey) {
		t.Errorf("expected ErrUnknownKey, got %v", err)
	}
}

func TestWrongAudience(t *testing.T) {
	// Token minted for edge-A; a verifier for edge-B holding the SAME public
	// key rejects on audience (the signature itself is valid).
	pub, priv, _ := GenerateKey()
	m := NewMinter("k1", priv, "edge-A")
	token, _, _ := m.Mint(sampleClaims(), time.Hour)
	v := NewVerifier("edge-B")
	v.AddKey("k1", pub)
	if _, err := v.Verify(token); !errors.Is(err, ErrBadAudience) {
		t.Errorf("expected ErrBadAudience, got %v", err)
	}
}

func TestMalformedAndVersion(t *testing.T) {
	_, v := newPair(t, "e")
	for _, bad := range []string{"", "noseparator", ".", "a.", ".b", "@@@.@@@"} {
		if _, err := v.Verify(bad); err == nil {
			t.Errorf("malformed %q should fail", bad)
		}
	}
}

func TestKeyRotation(t *testing.T) {
	// Token minted under k1; verifier knows k1 and k2 → still verifies.
	pub1, priv1, _ := GenerateKey()
	pub2, _, _ := GenerateKey()
	m := NewMinter("k1", priv1, "e")
	token, _, _ := m.Mint(sampleClaims(), time.Hour)
	v := NewVerifier("e")
	v.AddKey("k2", pub2)
	v.AddKey("k1", pub1)
	if _, err := v.Verify(token); err != nil {
		t.Errorf("rotation verify failed: %v", err)
	}
}

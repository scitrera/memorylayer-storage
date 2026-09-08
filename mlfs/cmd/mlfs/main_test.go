// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package main

import "testing"

// TestDataPathOwnsNATS pins the dedicated-vs-reuse decision for the -remote data
// path: on EXTERNAL NATS it always opens its OWN connection (so parallel uploads
// don't starve coordination's lease/heartbeat — the mount-flap fix), regardless
// of coordination; with coordination disabled it owns one; only embedded NATS +
// coordination reuses the coordination connection.
func TestDataPathOwnsNATS(t *testing.T) {
	cases := []struct {
		natsURL      string
		coordEnabled bool
		wantOwn      bool
	}{
		{"nats://r3:4222", true, true},  // external + coordination: DEDICATED (the fix)
		{"nats://r3:4222", false, true}, // external + no coordination: own
		{"", true, false},               // embedded + coordination: reuse
		{"", false, true},               // embedded + no coordination: own
	}
	for _, c := range cases {
		if got := dataPathOwnsNATS(c.natsURL, c.coordEnabled); got != c.wantOwn {
			t.Errorf("dataPathOwnsNATS(%q, %v) = %v, want %v", c.natsURL, c.coordEnabled, got, c.wantOwn)
		}
	}
}

// TestSplitCSV guards the CSV flag parser used for NATS route lists.
func TestSplitCSV(t *testing.T) {
	if got := splitCSV(""); got != nil {
		t.Fatalf("empty input: want nil, got %#v", got)
	}
	got := splitCSV(" a , ,b,c ")
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("len: want %d, got %d (%#v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("index %d: want %q, got %q", i, want[i], got[i])
		}
	}
}

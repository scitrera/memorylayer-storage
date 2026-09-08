// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package coord

import (
	"context"
	"syscall"
	"testing"
	"time"
)

// TestHandoffTransfersOwnership: nodeA owns a scope; once past the hysteresis
// floor, nodeB's write triggers a handoff — nodeA releases, nodeB acquires and
// writes successfully (no ESTALE).
func TestHandoffTransfersOwnership(t *testing.T) {
	_, a, b, oa, ob, _ := twoNATSNodes(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Short hysteresis so the test doesn't wait; start both responders.
	oa.minHold, ob.minHold = 5*time.Millisecond, 5*time.Millisecond
	if err := oa.StartHandoffResponder(ctx); err != nil {
		t.Fatalf("A responder: %v", err)
	}
	if err := ob.StartHandoffResponder(ctx); err != nil {
		t.Fatalf("B responder: %v", err)
	}

	f, _ := setupFile(t, a)           // nodeA now owns scope ino:<s1>
	time.Sleep(20 * time.Millisecond) // exceed minHold

	// nodeB writes the same file: Ensure sees nodeA live → requests handoff →
	// nodeA releases → nodeB acquires and the write succeeds.
	if st := b.Write(ctx, f, 0, 0, ds(42)); st != 0 {
		t.Fatalf("nodeB write after handoff: got %v, want success", st)
	}
	// nodeA must have relinquished the scope.
	scope, _ := a.ScopeOf(ctx, f)
	oa.mu.Lock()
	_, stillHeld := oa.held[scope]
	oa.mu.Unlock()
	if stillHeld {
		t.Fatal("nodeA still holds the scope after handoff")
	}
}

// TestHandoffHysteresisDeclines: within the min-hold window, the owner declines
// the handoff and the requester is fenced (ESTALE) rather than thrashing it.
func TestHandoffHysteresisDeclines(t *testing.T) {
	_, a, b, oa, ob, _ := twoNATSNodes(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Long hysteresis: nodeA will refuse to hand off for the duration of the test.
	oa.minHold = time.Hour
	if err := oa.StartHandoffResponder(ctx); err != nil {
		t.Fatalf("A responder: %v", err)
	}
	if err := ob.StartHandoffResponder(ctx); err != nil {
		t.Fatalf("B responder: %v", err)
	}

	f, _ := setupFile(t, a) // nodeA owns the scope, just acquired

	if st := b.Write(ctx, f, 0, 0, ds(43)); st != syscall.ESTALE {
		t.Fatalf("nodeB write under hysteresis: got %v, want ESTALE", st)
	}
	// nodeA keeps writing its scope.
	if st := a.Write(ctx, f, 0, 0, ds(44)); st != 0 {
		t.Fatalf("nodeA write (owner): %v", st)
	}
}

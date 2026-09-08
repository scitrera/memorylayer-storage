// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package coord

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

// fakeScanner is a controllable ChangelogScanner backed by an in-memory slice.
type fakeScanner struct {
	mu      sync.Mutex
	entries []meta.ChangeEntry
}

func (f *fakeScanner) add(e meta.ChangeEntry) {
	f.mu.Lock()
	f.entries = append(f.entries, e)
	f.mu.Unlock()
}

func (f *fakeScanner) ScanChangelog(_ context.Context, after int64, fn func(meta.ChangeEntry) error) error {
	f.mu.Lock()
	snap := append([]meta.ChangeEntry(nil), f.entries...)
	f.mu.Unlock()
	for _, e := range snap {
		if e.LSN > after {
			if err := fn(e); err != nil {
				return err
			}
		}
	}
	return nil
}

// TestChangefeedPublishSubscribe: a leader publisher fans changelog tail entries
// out over NATS and a subscriber receives them.
func TestChangefeedPublishSubscribe(t *testing.T) {
	n := embeddedNATS(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cf := NewChangefeed(n)
	got := make(chan ChangeMsg, 8)
	if err := cf.Subscribe(ctx, func(m ChangeMsg) { got <- m }); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := n.Conn().Flush(); err != nil { // ensure SUB interest is registered before any PUB
		t.Fatalf("flush: %v", err)
	}

	// Seed a pre-existing entry so the publisher's startup drain advances its
	// cursor past it (boot does not replay history), then start publishing.
	fs := &fakeScanner{}
	fs.add(meta.ChangeEntry{LSN: 1, Ino: 1, Op: meta.ChangeWrite, Payload: []byte(`{}`)})
	go cf.RunPublisher(ctx, fs, func() bool { return true }, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond) // let the startup drain complete

	// A new change after boot must be fanned out.
	fs.add(meta.ChangeEntry{LSN: 2, Ino: 42, Op: meta.ChangeWrite, Payload: []byte(`{}`)})

	select {
	case m := <-got:
		if m.Ino != 42 || m.Op != meta.ChangeWrite || m.LSN != 2 {
			t.Fatalf("unexpected change: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive published change")
	}
}

// TestChangefeedLeaderGated: a non-leader publisher stays silent.
func TestChangefeedLeaderGated(t *testing.T) {
	n := embeddedNATS(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cf := NewChangefeed(n)
	got := make(chan ChangeMsg, 8)
	if err := cf.Subscribe(ctx, func(m ChangeMsg) { got <- m }); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	fs := &fakeScanner{}
	go cf.RunPublisher(ctx, fs, func() bool { return false }, 10*time.Millisecond) // never leader

	fs.add(meta.ChangeEntry{LSN: 1, Ino: 7, Op: meta.ChangeWrite, Payload: []byte(`{}`)})

	select {
	case m := <-got:
		t.Fatalf("non-leader should not publish, got %+v", m)
	case <-time.After(300 * time.Millisecond):
		// expected: silence
	}
}

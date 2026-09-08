// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// NewVersion returns a new, monotonically-sortable version string of the form
//
//	<RFC3339Nano>-<6-char-random-hex>
//
// The RFC3339Nano prefix ensures lexicographic order equals time order, which
// is the property GetLatest and List rely on for cheaply finding the newest
// version. The random suffix prevents collisions if two Puts race within the
// same nanosecond.
func NewVersion() string {
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	buf := make([]byte, 3) // 3 bytes → 6 hex chars
	_, _ = rand.Read(buf)
	return ts + "-" + hex.EncodeToString(buf)
}

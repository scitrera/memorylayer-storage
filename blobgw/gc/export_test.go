// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package gc

import "time"

// SafetyWindowForTest exposes the effective safety window NewRunner settled on,
// so a test can assert the floor/clamp logic (ADR §4 I3/HC1) without reaching
// into the unexported field.
func (r *Runner) SafetyWindowForTest() time.Duration { return r.safetyWindow }

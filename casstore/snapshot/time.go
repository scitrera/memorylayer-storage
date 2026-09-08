// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package snapshot

import "time"

// timeNow is the clock used throughout the package. Replaced in tests to
// produce deterministic timestamps.
var timeNow = time.Now

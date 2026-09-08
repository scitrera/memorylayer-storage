// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package driver

import "testing"

// TestVolumeResponseEchoesNodeParams pins the contract that VolumeContext is the
// only channel StorageClass parameters reach the node: every node-consumed param
// (the tenancy set plus dirMode/uid/gid) must be echoed, and empty values dropped.
// A regression here silently strips a param so NodePublishVolume never sees it —
// exactly the bug where dirMode was set on the class but never stamped the subdir.
func TestVolumeResponseEchoesNodeParams(t *testing.T) {
	params := map[string]string{
		paramDomain:  "acme",
		paramNATSURL: "nats://r3:4222",
		paramRegion:  "7",
		paramDirMode: "0777",
		paramUID:     "1000",
		paramGID:     "1000",
		"ignored":    "should-not-echo",
	}
	resp := volumeResponse("vol1", 0, params)
	ctx := resp.GetVolume().GetVolumeContext()

	if ctx[ctxSubPath] != "vol1" {
		t.Errorf("subPath = %q, want %q", ctx[ctxSubPath], "vol1")
	}
	for _, k := range []string{paramDomain, paramNATSURL, paramRegion, paramDirMode, paramUID, paramGID} {
		if ctx[k] != params[k] {
			t.Errorf("VolumeContext[%q] = %q, want %q (param dropped from echo)", k, ctx[k], params[k])
		}
	}
	if _, ok := ctx["ignored"]; ok {
		t.Errorf("non-parameter key %q leaked into VolumeContext", "ignored")
	}
}

// TestVolumeResponseDropsEmptyParams confirms unset params are not echoed as
// empty strings (so NodePublishVolume's "absent => no change / legacy" logic
// still fires instead of parsing "").
func TestVolumeResponseDropsEmptyParams(t *testing.T) {
	resp := volumeResponse("vol2", 0, map[string]string{paramDomain: "acme"})
	ctx := resp.GetVolume().GetVolumeContext()
	for _, k := range []string{paramNATSURL, paramRegion, paramDirMode, paramUID, paramGID} {
		if _, ok := ctx[k]; ok {
			t.Errorf("empty param %q was echoed into VolumeContext, want absent", k)
		}
	}
}

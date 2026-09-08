// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package driver

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// failBindMounter mounts/probes like memMounter but always fails BindMount, to
// exercise the post-Acquire failure path in NodePublishVolume.
type failBindMounter struct{ *memMounter }

func (failBindMounter) BindMount(_, _ string, _ bool) error { return errors.New("bind failed") }

// TestNodePublishKeepsDomainOnBindFailure pins the non-destructive-on-failure
// behavior: a bind-mount failure must NOT tear the domain mount down — the
// kubelet retries NodePublish and must reuse the live mount (destroying it on
// every transient glitch causes a permanent destroy→recreate churn). The hold is
// reclaimed by NodeUnpublishVolume on the proper CSI lifecycle.
func TestNodePublishKeepsDomainOnBindFailure(t *testing.T) {
	mm := newMemMounter()
	fl := &fakeLauncher{mounter: mm}
	mgr := NewMountManager(t.TempDir(), fl, mm)
	d, err := New(Config{Endpoint: "unix:///x", NodeID: "node-1", DefaultNATSURL: "nats://d"},
		failBindMounter{mm}, mgr)
	if err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(t.TempDir(), "pod")
	req := &csi.NodePublishVolumeRequest{
		VolumeId:   "vol1",
		TargetPath: target,
		VolumeCapability: &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
		},
		VolumeContext: map[string]string{paramDomain: "acme", ctxSubPath: "vol1"},
		Secrets:       map[string]string{secretNATSCreds: "C", secretMetaDSN: "dsn"},
	}

	if _, err := d.NodePublishVolume(context.Background(), req); err == nil {
		t.Fatalf("expected bind-mount failure")
	}

	// The domain mount is KEPT (not torn down), so a retried publish reuses it and
	// the mount-pod isn't destroyed/recreated. A Release for the target still finds
	// the holder — proving the mount survived the failed publish; teardown happens
	// only when NodeUnpublishVolume releases the last ref.
	startsBefore := fl.starts
	released, err := mgr.Release(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if !released {
		t.Errorf("domain mount was torn down on a bind failure — would churn the mount pod")
	}
	if fl.starts != startsBefore {
		t.Errorf("mount relaunched during the failed publish: starts %d -> %d", startsBefore, fl.starts)
	}
}

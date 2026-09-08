// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package driver_test

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/kubernetes-csi/csi-test/v5/pkg/sanity"

	"github.com/scitrera/memorylayer-storage/mlfs-csi/internal/driver"
)

// fakeMounter records bind mounts in memory so the sanity suite runs without
// CAP_SYS_ADMIN. The driver's real bind-mount behavior is covered by the
// production linuxMounter (exercised under root / in kind).
type fakeMounter struct {
	mu      sync.Mutex
	mounted map[string]bool
}

func newFakeMounter() *fakeMounter { return &fakeMounter{mounted: map[string]bool{}} }

func (m *fakeMounter) BindMount(_, target string, _ bool) error {
	m.mu.Lock()
	m.mounted[target] = true
	m.mu.Unlock()
	return nil
}

func (m *fakeMounter) Unmount(target string) error {
	m.mu.Lock()
	delete(m.mounted, target)
	m.mu.Unlock()
	return nil
}

func (m *fakeMounter) IsMounted(target string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mounted[target], nil
}

func (m *fakeMounter) IsMountedLive(target string) (bool, error) { return m.IsMounted(target) }

// TestCSISanity runs the kubernetes-csi sanity suite against the driver over a
// real unix socket, validating CSI spec compliance end to end.
func TestCSISanity(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "csi.sock")
	endpoint := "unix://" + sock
	root := t.TempDir() // stands in for the node's shared mlfs mount

	d, err := driver.New(driver.Config{Endpoint: endpoint, NodeID: "node-1", Root: root}, newFakeMounter(), nil)
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}
	go func() { _ = d.Run() }()
	defer d.Stop()

	cfg := sanity.NewTestConfig()
	cfg.Address = endpoint
	cfg.TargetPath = filepath.Join(t.TempDir(), "target")
	cfg.StagingPath = filepath.Join(t.TempDir(), "staging")
	sanity.Test(t, cfg)
}

// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package driver implements the Kubernetes CSI driver for the mlfs filesystem
// (L2.11). It uses the simple, robust subdir-over-shared-mount model: each node
// runs one shared mlfs FUSE mount (the DaemonSet "shared cache" mode), a volume
// is a subdirectory under that mount, and NodePublishVolume bind-mounts the
// subdir into the pod. No per-pod mount processes, no mount-pod orchestration —
// the multi-node correctness (ownership leases / generation fence) lives in mlfs
// itself (L2.8), so many nodes can share one filesystem safely.
package driver

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
)

// DriverName is the CSI plugin name (must match the CSIDriver object + the
// StorageClass provisioner).
const DriverName = "mlfs.csi.scitrera.com"

// version is overridable at build time via -ldflags.
var version = "0.1.0"

// volumesSubdir is where volume subdirectories live under the shared mlfs root,
// keeping CSI-managed state out of the way of user data. metaSubdir holds
// per-volume bookkeeping (requested capacity) separate from user data.
const (
	volumesSubdir = "csi-volumes"
	metaSubdir    = ".csi-meta"
)

// Config configures a Driver.
type Config struct {
	Endpoint string // gRPC listen endpoint, e.g. unix:///csi/csi.sock
	NodeID   string // this node's name (from spec.nodeName)
	Root     string // legacy single shared mlfs mount (optional); the no-domain publish path + controller meta
	// DefaultNATSURL backfills a PVC's NATS URL when the StorageClass omits the
	// natsUrl parameter (multi-domain only).
	DefaultNATSURL string
}

// Driver is the CSI Identity + Controller + Node service for mlfs. Unimplemented
// embeds give forward-compatible "unimplemented" for RPCs we don't advertise.
type Driver struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer

	cfg     Config
	mounter Mounter
	// mgr multiplexes per-domain mlfs mounts on this node. nil ==> multi-domain
	// disabled; only the legacy single-shared-mount (-mlfs-root) path is served.
	mgr *MountManager
	srv *grpc.Server
}

// New builds a Driver. mounter abstracts bind-mount/unmount so the CSI sanity
// suite can run without root (inject a fake); production passes NewLinuxMounter.
// mgr enables per-PVC multi-domain mounting (§4); pass nil to run legacy
// single-shared-mount only. At least one of cfg.Root or mgr must be set.
func New(cfg Config, mounter Mounter, mgr *MountManager) (*Driver, error) {
	if cfg.Root == "" && mgr == nil {
		return nil, fmt.Errorf("mlfs-csi: -mlfs-root (legacy shared mount) or -domains-base (multi-domain) is required")
	}
	if mounter == nil {
		return nil, fmt.Errorf("mlfs-csi: nil mounter")
	}
	return &Driver{cfg: cfg, mounter: mounter, mgr: mgr}, nil
}

// volumePath is the on-mount directory backing a volume id.
func (d *Driver) volumePath(volumeID string) string {
	return joinClean(d.cfg.Root, volumesSubdir, volumeID)
}

// metaPath is the bookkeeping file recording a volume's requested capacity.
func (d *Driver) metaPath(volumeID string) string {
	return joinClean(d.cfg.Root, metaSubdir, volumeID)
}

// Run serves the gRPC API until the listener closes. The endpoint scheme/addr is
// parsed from cfg.Endpoint (unix:// or tcp://).
func (d *Driver) Run() error {
	scheme, addr, err := parseEndpoint(d.cfg.Endpoint)
	if err != nil {
		return err
	}
	if scheme == "unix" {
		// A stale socket from a previous run blocks Listen; remove it.
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale socket %s: %w", addr, err)
		}
	}
	lis, err := net.Listen(scheme, addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", d.cfg.Endpoint, err)
	}
	d.srv = grpc.NewServer()
	csi.RegisterIdentityServer(d.srv, d)
	csi.RegisterControllerServer(d.srv, d)
	csi.RegisterNodeServer(d.srv, d)
	return d.srv.Serve(lis)
}

// Stop gracefully stops the gRPC server.
func (d *Driver) Stop() {
	if d.srv != nil {
		d.srv.GracefulStop()
	}
}

// parseEndpoint splits "unix:///path" or "tcp://host:port" into (scheme, addr).
func parseEndpoint(ep string) (string, string, error) {
	if strings.HasPrefix(ep, "unix://") {
		return "unix", strings.TrimPrefix(ep, "unix://"), nil
	}
	if strings.HasPrefix(ep, "tcp://") {
		return "tcp", strings.TrimPrefix(ep, "tcp://"), nil
	}
	return "", "", fmt.Errorf("mlfs-csi: invalid endpoint %q (want unix:// or tcp://)", ep)
}

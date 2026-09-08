// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package driver

import (
	"context"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NodePublishVolume bind-mounts the volume's subdirectory (under the shared mlfs
// mount) onto the pod's target path. Idempotent: a target already mounted is a
// no-op success.
func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	id := req.GetVolumeId()
	target := req.GetTargetPath()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}
	if req.GetVolumeCapability().GetBlock() != nil {
		return nil, status.Error(codes.InvalidArgument, "block volumes are not supported")
	}

	// Honor an explicit subPath from the volume context (static provisioning may
	// point at a pre-existing directory); default to the volume id.
	sub := id
	if v := req.GetVolumeContext()[ctxSubPath]; v != "" {
		sub = v
	}

	// Resolve the volume's backing directory. When the PVC selects a domain
	// (multi-tenant path), ensure that domain's mlfs mount is up on this node and
	// root the subdir under it; otherwise fall back to the legacy single shared
	// mount. §4.1/§4.2.
	source, err := d.resolveSource(ctx, sub, req.GetVolumeContext(), req.GetSecrets(), target)
	if err != nil {
		return nil, err
	}
	// NON-DESTRUCTIVE on failure: once the domain mount is up (Acquire succeeded),
	// a subsequent error here must NOT tear it down — the kubelet RETRIES
	// NodePublish, and destroying the mount pod on every transient glitch (e.g. a
	// brief `stale file handle` right after mount) turns it into a permanent
	// destroy→recreate churn that never recovers. We keep the per-domain mount
	// (the target's ref) and just return the error; the retry reuses the live
	// mount and self-heals, and NodeUnpublishVolume releases the ref + tears the
	// mount down when the volume is actually removed (the CO always calls it,
	// even after a failed publish). So Acquire's ref is reclaimed on the proper
	// CSI lifecycle, not on a transient publish error.

	// Ensure the backing dir exists (covers statically-provisioned volumes whose
	// CreateVolume never ran on this driver, and the lazy per-domain layout).
	if err := os.MkdirAll(source, 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "ensure source dir: %v", err)
	}

	// Stamp optional per-volume mode/ownership (dirMode/uid/gid StorageClass
	// parameters, echoed into the VolumeContext) so a non-root pod can write to
	// the root-created subdir. Unset => left as created. Chmod/Chown run as root
	// (the node plugin) and, on the mlfs path, persist to the domain meta.
	perms, err := subdirPermsFromContext(req.GetVolumeContext())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := perms.apply(source); err != nil {
		return nil, status.Errorf(codes.Internal, "apply subdir perms: %v", err)
	}

	if err := os.MkdirAll(target, 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "create target dir: %v", err)
	}

	mounted, err := d.mounter.IsMounted(target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check mount: %v", err)
	}
	if mounted {
		return &csi.NodePublishVolumeResponse{}, nil // idempotent
	}
	if err := d.mounter.BindMount(source, target, req.GetReadonly()); err != nil {
		return nil, status.Errorf(codes.Internal, "bind mount %s -> %s: %v", source, target, err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// resolveSource determines the on-node directory backing a volume. With a
// domain selected (multi-tenant), it lazily ensures that domain's mlfs mount is
// up on this node (ref-counting target as a holder) and roots the subdir under
// it; with no domain it uses the legacy single shared mount. The legacy path is
// retained on purpose for the shared model-cache use case (§8). The mount hold
// taken for target is released by NodeUnpublishVolume, NOT on a publish error
// (see NodePublishVolume — the mount is kept across retries).
func (d *Driver) resolveSource(ctx context.Context, sub string, volCtx, secrets map[string]string, target string) (source string, err error) {
	if d.mgr == nil {
		return d.volumePath(sub), nil // multi-domain disabled: legacy only
	}
	spec, err := domainSpecFromRequest(d.cfg.NodeID, d.cfg.DefaultNATSURL, volCtx, secrets)
	if err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}
	if spec == nil {
		if d.cfg.Root == "" {
			return "", status.Error(codes.InvalidArgument, "volume has no domain and no legacy -mlfs-root is configured")
		}
		return d.volumePath(sub), nil
	}
	mountDir, err := d.mgr.Acquire(ctx, *spec, target)
	if err != nil {
		return "", status.Errorf(codes.Internal, "ensure domain %q mount: %v", spec.Domain, err)
	}
	return joinClean(mountDir, volumesSubdir, sub), nil
}

// NodeUnpublishVolume unmounts the bind mount, removes the target, and releases
// the volume's hold on its domain mount (tearing the domain's mlfs down when its
// last PVC on this node leaves). Idempotent.
func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	target := req.GetTargetPath()
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	mounted, err := d.mounter.IsMounted(target)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check mount: %v", err)
	}
	if mounted {
		if err := d.mounter.Unmount(target); err != nil {
			return nil, status.Errorf(codes.Internal, "unmount %s: %v", target, err)
		}
	}
	if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "remove target %s: %v", target, err)
	}
	// Release the per-domain mount hold AFTER the bind mount is gone, so the
	// domain's mlfs is not torn down while the pod still has it mounted. A no-op
	// for legacy (non-domain) volumes the manager never tracked.
	if d.mgr != nil {
		if _, err := d.mgr.Release(ctx, target); err != nil {
			return nil, status.Errorf(codes.Internal, "release domain mount: %v", err)
		}
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetCapabilities: the driver publishes directly (no STAGE_UNSTAGE stage).
func (d *Driver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

// NodeGetInfo returns this node's id (used by the CO for topology/placement).
func (d *Driver) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: d.cfg.NodeID}, nil
}

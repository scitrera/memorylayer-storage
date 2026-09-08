// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package driver

import (
	"context"
	"os"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateVolume provisions a volume as a subdirectory under the shared mlfs
// mount. Idempotent: re-creating an existing name returns the same volume.
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}
	caps := req.GetVolumeCapabilities()
	if len(caps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	if err := validateCaps(caps); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// Capacity bookkeeping (and the legacy subdir) live under cfg.Root. Refuse
	// rather than silently resolving metaPath/volumePath to a relative path under
	// the process CWD when the controller was started without -mlfs-root.
	if d.cfg.Root == "" {
		return nil, status.Error(codes.FailedPrecondition,
			"mlfs-csi controller requires -mlfs-root for volume metadata")
	}

	// Pass the non-secret tenancy parameters (domain, nats-url, region) through to
	// the PV's volumeAttributes so they arrive at NodePublishVolume, which selects
	// and brings up the right per-domain mlfs mount on the node (§4.1). Sensitive
	// material (NATS creds, meta DSN) travels separately as node-publish secrets.
	params := req.GetParameters()
	domain := params[paramDomain]

	// mlfs has no per-subdir quota yet, so capacity is advisory (0 = unbounded);
	// the subdir shares the filesystem's capacity. We still record it so a repeat
	// CreateVolume with the SAME name but a DIFFERENT capacity is rejected with
	// AlreadyExists, per the CSI idempotency contract. The capacity meta is
	// controller-local bookkeeping (it does not require a domain mount).
	capacity := req.GetCapacityRange().GetRequiredBytes()

	if _, err := os.Stat(d.metaPath(name)); err == nil {
		prior, perr := d.readCapacity(name)
		if perr == nil && prior != capacity {
			return nil, status.Errorf(codes.AlreadyExists,
				"volume %q already exists with capacity %d (requested %d)", name, prior, capacity)
		}
		capacity = priorOr(prior, perr, capacity)
		return volumeResponse(name, capacity, params), nil
	}

	// Legacy single-shared-mount volumes are created eagerly under the controller's
	// own mount; domain volumes are created lazily node-side at publish (the
	// controller has no per-domain mount), so we only mkdir for the legacy case.
	if domain == "" {
		if err := os.MkdirAll(d.volumePath(name), 0o755); err != nil {
			return nil, status.Errorf(codes.Internal, "create volume dir: %v", err)
		}
	}
	if err := d.writeCapacity(name, capacity); err != nil {
		return nil, status.Errorf(codes.Internal, "record volume metadata: %v", err)
	}
	return volumeResponse(name, capacity, params), nil
}

// volumeResponse builds the CreateVolume reply, echoing the non-secret tenancy
// parameters into VolumeContext alongside the subPath so NodePublishVolume can
// reconstruct the DomainSpec and stamp the subdir mode/ownership. VolumeContext
// is the ONLY channel StorageClass parameters reach the node (it becomes the PV's
// volumeAttributes), so every node-consumed param — including dirMode/uid/gid —
// MUST be echoed here or it silently never arrives.
func volumeResponse(name string, capacity int64, params map[string]string) *csi.CreateVolumeResponse {
	ctx := map[string]string{ctxSubPath: name}
	for _, k := range []string{paramDomain, paramNATSURL, paramRegion, paramDirMode, paramUID, paramGID} {
		if v := params[k]; v != "" {
			ctx[k] = v
		}
	}
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      name,
			CapacityBytes: capacity,
			VolumeContext: ctx,
		},
	}
}

func priorOr(prior int64, err error, fallback int64) int64 {
	if err != nil {
		return fallback
	}
	return prior
}

func (d *Driver) writeCapacity(name string, capacity int64) error {
	if err := os.MkdirAll(joinClean(d.cfg.Root, metaSubdir), 0o755); err != nil {
		return err
	}
	return os.WriteFile(d.metaPath(name), []byte(strconv.FormatInt(capacity, 10)), 0o644)
}

func (d *Driver) readCapacity(name string) (int64, error) {
	b, err := os.ReadFile(d.metaPath(name))
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
}

// DeleteVolume removes the volume's controller-side subdirectory (legacy shared
// mount) and its capacity meta. Idempotent. NOTE: for domain volumes the data
// lives in a per-domain mount the controller does not hold, so the RemoveAll
// below is a no-op for them and byte reclamation of the domain subdir is
// deferred to mlfs GC / a node-side delete sweep (see docs/TECH_DEBT.md). The
// DeleteVolumeRequest carries no parameters, so the controller cannot reach the
// domain mount here even if it wanted to.
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	id := req.GetVolumeId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	if d.cfg.Root == "" {
		return nil, status.Error(codes.FailedPrecondition,
			"mlfs-csi controller requires -mlfs-root for volume metadata")
	}
	if err := os.RemoveAll(d.volumePath(id)); err != nil {
		return nil, status.Errorf(codes.Internal, "delete volume dir: %v", err)
	}
	if err := os.Remove(d.metaPath(id)); err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "delete volume metadata: %v", err)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerGetCapabilities advertises dynamic provisioning.
func (d *Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	rpc := func(t csi.ControllerServiceCapability_RPC_Type) *csi.ControllerServiceCapability {
		return &csi.ControllerServiceCapability{Type: &csi.ControllerServiceCapability_Rpc{
			Rpc: &csi.ControllerServiceCapability_RPC{Type: t},
		}}
	}
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			rpc(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
		},
	}, nil
}

// ValidateVolumeCapabilities confirms the requested capabilities are supported
// by an existing volume.
func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	id := req.GetVolumeId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	caps := req.GetVolumeCapabilities()
	if len(caps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	if _, err := os.Stat(d.volumePath(id)); os.IsNotExist(err) {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", id)
	}
	if err := validateCaps(caps); err != nil {
		// Unsupported caps → return a response with no Confirmed field set.
		return &csi.ValidateVolumeCapabilitiesResponse{Message: err.Error()}, nil
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: caps},
	}, nil
}

// validateCaps rejects block volumes (mlfs is a filesystem) and unsupported
// access modes. mlfs is a shared filesystem, so multi-node multi-writer is
// supported in addition to the single-node modes.
func validateCaps(caps []*csi.VolumeCapability) error {
	for _, c := range caps {
		if c.GetBlock() != nil {
			return status.Error(codes.InvalidArgument, "block volumes are not supported (mlfs is a filesystem)")
		}
		switch c.GetAccessMode().GetMode() {
		case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
			csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
			csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
		default:
			return status.Errorf(codes.InvalidArgument, "unsupported access mode %v", c.GetAccessMode().GetMode())
		}
	}
	return nil
}

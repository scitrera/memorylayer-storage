// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package driver

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// GetPluginInfo returns the driver name + version.
func (d *Driver) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: DriverName, VendorVersion: version}, nil
}

// GetPluginCapabilities advertises that the driver runs a controller service
// (dynamic provisioning via CreateVolume/DeleteVolume).
func (d *Driver) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{
				Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
			}}},
		},
	}, nil
}

// Probe reports readiness.
func (d *Driver) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

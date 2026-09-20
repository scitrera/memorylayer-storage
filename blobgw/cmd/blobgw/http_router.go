// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only
package main

import (
	"context"
	"fmt"

	"github.com/scitrera/memorylayer-storage/blobgw/gateway"
	"github.com/scitrera/memorylayer-storage/blobgw/tenantconfig"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// buildHTTPTenantRouter shares the edge's binding, namespace and object layout
// without requiring a NATS control-plane connection for HTTP-only deployments.
func buildHTTPTenantRouter(ctx context.Context, o options, b *built) (*gateway.TenantRouter, error) {
	mode, err := tenantconfig.ParseCredentialMode(o.credentialMode)
	if err != nil {
		return nil, err
	}
	ttl := o.credentialCacheTTL
	if ttl <= 0 {
		ttl = tenantconfig.DefaultCacheTTL
	}
	provider, err := tenantconfig.LoadProviderModeWatched(ctx, mode, o.tenantConfig, o.tenantSecretsFile, ttl)
	if err != nil {
		return nil, fmt.Errorf("load tenant provider: %w", err)
	}
	dsns, err := loadManifestDSNs(o.manifestDSNFile)
	if err != nil {
		return nil, err
	}
	framing, err := snapshot.ParsePackCompressionMode(o.packFraming)
	if err != nil {
		return nil, err
	}
	return gateway.NewTenantRouter(nil, nil, b.dedup, b.refs, b.staging, o.packSize,
		gateway.WithBindingProvider(provider, dsns), gateway.WithPackCompression(framing),
		gateway.WithBackingMeter(b.metrics.Meter())), nil
}

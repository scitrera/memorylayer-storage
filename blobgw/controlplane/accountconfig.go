// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package controlplane

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// validIdentifier matches names that are safe to interpolate as bare NATS
// config identifiers (account names, tenant names). Anything outside
// [A-Za-z0-9_-] would produce malformed config or silently broaden routing.
var validIdentifier = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// ServiceAccountConfig describes the NATS account-per-domain mapping that
// realizes ADR-001 §7.7 for the converged blobgw control plane: a shared service
// account that EXPORTS the control-plane service on blobgw.*.> , and one account
// per tenant that IMPORTS that service remapped to its own blobgw.<tenant>.>
// prefix ONLY. The per-tenant import subject is the isolation boundary — a node
// in tenant A's account can reach blobgw for tenant A and nothing else.
//
// This is the single source of truth for the mapping SHAPE. RenderServerConfig
// emits an in-server `accounts{}` block (for static deployments and hermetic
// tests); the operator/JWT (nsc) equivalent is documented in
// docs/blobgw-control-plane-accounts.md and produces identical routing.
type ServiceAccountConfig struct {
	// ServiceAccount is the shared account name blobgw connects to (e.g.
	// "BLOBGW_SVC"). It exports the control-plane service.
	ServiceAccount string
	// ServiceUser / ServicePassword are the blobgw connection credentials in the
	// service account (static config). In production these are an issued JWT/creds
	// file selected via cmd/blobgw -nats-creds.
	ServiceUser     string
	ServicePassword string
	// Tenants maps each tenant account name to its client user credentials. Each
	// tenant account imports blobgw.<tenant>.> from the service account, where
	// <tenant> is the map key.
	Tenants map[string]TenantCredentials
}

// TenantCredentials are a tenant account's static client credentials.
type TenantCredentials struct {
	User     string
	Password string
}

// serviceExportSubject is the wildcard the service account exports: it covers
// every control-plane op for every tenant (index lookup/record, presign,
// gc.purge — all live under blobgw.<tenant>.*). Tenant accounts narrow it to
// their own prefix on import.
const serviceExportSubject = "blobgw.*.>"

// tenantImportSubject is the per-tenant import subject: the account boundary that
// scopes a tenant account to its own control-plane prefix.
func tenantImportSubject(tenant string) string {
	return "blobgw." + tenant + ".>"
}

// RenderServerConfig renders an in-server NATS `accounts{}` configuration string
// (nats-server conf format) realizing the service-export / per-tenant-import
// mapping. The caller supplies the surrounding server options (host/port/
// jetstream); this returns only the `accounts { ... }` block so it can be
// embedded in a full nats.conf or written to a temp file and parsed via
// natsserver.ProcessConfigFile.
//
// It returns an error if the config is structurally invalid (no service account,
// no service user, or no tenants) so a misconfiguration fails loudly rather than
// silently producing an un-isolated server.
func (c ServiceAccountConfig) RenderServerConfig() (string, error) {
	if c.ServiceAccount == "" {
		return "", fmt.Errorf("controlplane: ServiceAccount is required")
	}
	if c.ServiceUser == "" {
		return "", fmt.Errorf("controlplane: ServiceUser is required")
	}
	if len(c.Tenants) == 0 {
		return "", fmt.Errorf("controlplane: at least one tenant is required")
	}
	if !validIdentifier.MatchString(c.ServiceAccount) {
		return "", fmt.Errorf("controlplane: ServiceAccount %q contains invalid characters (must match [A-Za-z0-9_-]+)", c.ServiceAccount)
	}
	for t := range c.Tenants {
		if !validIdentifier.MatchString(t) {
			return "", fmt.Errorf("controlplane: tenant name %q contains invalid characters (must match [A-Za-z0-9_-]+)", t)
		}
	}

	var b strings.Builder
	b.WriteString("accounts {\n")
	fmt.Fprintf(&b, "  %s {\n", c.ServiceAccount)
	fmt.Fprintf(&b, "    users: [ { user: %q, password: %q } ]\n", c.ServiceUser, c.ServicePassword)
	fmt.Fprintf(&b, "    exports: [ { service: %q } ]\n", serviceExportSubject)
	b.WriteString("  }\n")

	// Deterministic ordering keeps the rendered config stable (testable, diffable).
	tenants := make([]string, 0, len(c.Tenants))
	for t := range c.Tenants {
		tenants = append(tenants, t)
	}
	sort.Strings(tenants)
	for _, tenant := range tenants {
		creds := c.Tenants[tenant]
		fmt.Fprintf(&b, "  %s {\n", tenant)
		fmt.Fprintf(&b, "    users: [ { user: %q, password: %q } ]\n", creds.User, creds.Password)
		fmt.Fprintf(&b, "    imports: [ { service: { account: %q, subject: %q } } ]\n",
			c.ServiceAccount, tenantImportSubject(tenant))
		b.WriteString("  }\n")
	}
	b.WriteString("}\n")
	return b.String(), nil
}

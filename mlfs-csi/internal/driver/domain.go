// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package driver

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// This file carries the per-PVC tenancy inputs that make one CSI node plugin
// serve many mlfs domains on a shared node (see
// docs/MLFS_SANDBOX_MULTITENANCY_GAP.md §4). A PVC selects its domain (and the
// refs needed to reach that domain's mlfs) through StorageClass `parameters`
// (non-secret: domain, nats-url, region) which the external-provisioner echoes
// into the PV's volumeAttributes — and thus into NodePublishVolume's
// VolumeContext — plus CSI node-publish secrets (sensitive: the NATS account
// creds + meta DSN). S3 stays a blobgw concern; the node never holds S3 creds.

// Non-secret StorageClass parameter / VolumeContext keys.
const (
	// paramDomain selects the mlfs dedup domain (tenant token). Empty ==> the
	// legacy single-shared-mount path (-mlfs-root), preserved for the shared
	// model-cache use case (§8).
	paramDomain  = "domain"
	paramNATSURL = "natsUrl"
	paramRegion  = "region"
	// ctxSubPath is the in-mount subdirectory backing the volume; set by
	// CreateVolume, honored by NodePublishVolume (static provisioning may point
	// it at a pre-existing dir).
	ctxSubPath = "subPath"
	// paramDirMode / paramUID / paramGID optionally stamp the POSIX mode and
	// ownership on the volume's backing subdir at NodePublishVolume, so a non-root
	// pod can write to the otherwise root-created (0755) subdir. mlfs isolation is
	// domain-scoped (its domain root is seeded 0o777), so a class serving non-root
	// workloads typically sets dirMode: "0777". All optional; unset leaves the
	// subdir as created (root:root, MkdirAll mode).
	paramDirMode = "dirMode"
	paramUID     = "uid"
	paramGID     = "gid"
)

// CSI node-publish secret keys (delivered in NodePublishVolumeRequest.Secrets,
// never persisted to disk except the creds file the manager writes 0600).
const (
	secretNATSCreds = "nats-creds" // NATS account credentials file CONTENT
	secretMetaDSN   = "meta-dsn"   // PostgreSQL DSN for this domain's meta engine
	// secretDomain optionally carries the domain in the (per-namespace) secret.
	// This lets ONE StorageClass serve many tenants without duplication (§7.2):
	// the StorageClass templates node-publish-secret-name/-namespace per PVC
	// namespace, and each tenant's secret names its own domain. A non-secret
	// `domain` StorageClass parameter (volCtx) still takes precedence when set.
	secretDomain = "domain"
)

// DomainSpec is everything the node needs to bring up (or attach to) a single
// domain's mlfs mount. It is assembled per NodePublishVolume from the PVC's
// VolumeContext + Secrets and lives only in memory — the meta DSN is never
// written to the manager's on-disk state.
type DomainSpec struct {
	Domain  string // tenant / dedup-domain token (NATS subject token)
	MetaDSN string // PG DSN (sensitive; in-memory only)
	NATSURL string // shared R3 NATS URL (account-per-domain isolation)
	Creds   string // NATS account creds file CONTENT (sensitive)
	Region  uint8  // this node's region id (inode prefix)
	NodeID  string // node identity, used to derive the mlfs coordination id
}

// domainSpecFromRequest builds a DomainSpec from a publish request's
// VolumeContext (non-secret params) and Secrets (sensitive material).
// defaultNATSURL backfills natsUrl when the StorageClass omits it. Returns
// (nil, nil) when no domain is set — the caller falls back to the legacy
// single-shared-mount path.
func domainSpecFromRequest(nodeID, defaultNATSURL string, volCtx, secrets map[string]string) (*DomainSpec, error) {
	// Domain comes from the StorageClass parameter when set, else from the
	// (per-namespace) secret; absent both ==> the legacy single-mount path.
	domain := strings.TrimSpace(volCtx[paramDomain])
	if domain == "" {
		domain = strings.TrimSpace(secrets[secretDomain])
	}
	if domain == "" {
		return nil, nil
	}
	if err := validateDomain(domain); err != nil {
		return nil, err
	}

	natsURL := strings.TrimSpace(volCtx[paramNATSURL])
	if natsURL == "" {
		natsURL = defaultNATSURL
	}
	if natsURL == "" {
		return nil, fmt.Errorf("domain %q requires a NATS URL (StorageClass parameter %q or driver -default-nats-url)", domain, paramNATSURL)
	}

	var region uint8
	if r := strings.TrimSpace(volCtx[paramRegion]); r != "" {
		n, err := strconv.ParseUint(r, 10, 8)
		if err != nil {
			return nil, fmt.Errorf("invalid %q %q: must be 0-255", paramRegion, r)
		}
		region = uint8(n)
	}

	creds := secrets[secretNATSCreds]
	if strings.TrimSpace(creds) == "" {
		return nil, fmt.Errorf("domain %q requires NATS account creds (CSI node-publish secret key %q)", domain, secretNATSCreds)
	}
	metaDSN := strings.TrimSpace(secrets[secretMetaDSN])
	if metaDSN == "" {
		return nil, fmt.Errorf("domain %q requires a meta DSN (CSI node-publish secret key %q)", domain, secretMetaDSN)
	}

	return &DomainSpec{
		Domain:  domain,
		MetaDSN: metaDSN,
		NATSURL: natsURL,
		Creds:   creds,
		Region:  region,
		NodeID:  nodeID,
	}, nil
}

// validateDomain enforces a conservative, filesystem- and NATS-subject-safe
// token. It MUST stay a subset of mlfs's ctlproto.ValidateTenant (the
// authoritative validator): no NATS wildcards or subject separators
// (`.`/`*`/`>`), no path separators, bounded length. Keeping a local copy
// avoids a cross-module dependency on mlfs (the CSI driver builds standalone),
// at the cost of this consistency note.
func validateDomain(d string) error {
	if d == "" {
		return fmt.Errorf("domain is empty")
	}
	if len(d) > 63 {
		return fmt.Errorf("domain %q too long (max 63)", d)
	}
	for _, r := range d {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_':
		default:
			return fmt.Errorf("domain %q contains an invalid character %q (allowed: alphanumerics, '-', '_')", d, string(r))
		}
	}
	return nil
}

// domainDirKey maps a domain token to a stable, collision-resistant directory
// name. The token is already restricted (validateDomain), but hashing keeps the
// on-disk layout uniform and immune to any future token-charset widening.
func domainDirKey(domain string) string {
	sum := sha256.Sum256([]byte(domain))
	return domain + "-" + hex.EncodeToString(sum[:6])
}

// SubdirPerms is the optional POSIX mode/ownership applied to a volume's backing
// subdir at publish time. A nil field means "leave as created". Sourced from the
// StorageClass parameters (dirMode/uid/gid) echoed into the VolumeContext.
type SubdirPerms struct {
	Mode *os.FileMode
	UID  *int
	GID  *int
}

// subdirPermsFromContext parses the optional dirMode/uid/gid VolumeContext keys.
// Absent keys yield nil fields (no change). An invalid value is a hard error so a
// misconfigured StorageClass surfaces at publish instead of being silently ignored.
func subdirPermsFromContext(volCtx map[string]string) (SubdirPerms, error) {
	var p SubdirPerms
	if v := strings.TrimSpace(volCtx[paramDirMode]); v != "" {
		m, err := strconv.ParseUint(v, 8, 32) // chmod-style octal, leading 0 optional
		if err != nil || m&^0o7777 != 0 {
			return p, fmt.Errorf("invalid %q %q: expected an octal POSIX mode like %q", paramDirMode, v, "0777")
		}
		mode := os.FileMode(m & 0o777)
		if m&0o4000 != 0 {
			mode |= os.ModeSetuid
		}
		if m&0o2000 != 0 {
			mode |= os.ModeSetgid
		}
		if m&0o1000 != 0 {
			mode |= os.ModeSticky
		}
		p.Mode = &mode
	}
	if v := strings.TrimSpace(volCtx[paramUID]); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return p, fmt.Errorf("invalid %q %q: must be a non-negative integer", paramUID, v)
		}
		p.UID = &n
	}
	if v := strings.TrimSpace(volCtx[paramGID]); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return p, fmt.Errorf("invalid %q %q: must be a non-negative integer", paramGID, v)
		}
		p.GID = &n
	}
	return p, nil
}

// apply stamps the configured mode/ownership on path. Chown runs before Chmod so
// setuid/setgid bits survive (Linux clears them on chown). A nil field is skipped.
func (p SubdirPerms) apply(path string) error {
	if p.UID != nil || p.GID != nil {
		uid, gid := -1, -1
		if p.UID != nil {
			uid = *p.UID
		}
		if p.GID != nil {
			gid = *p.GID
		}
		if err := os.Chown(path, uid, gid); err != nil {
			return fmt.Errorf("chown %s (uid=%d gid=%d): %w", path, uid, gid, err)
		}
	}
	if p.Mode != nil {
		if err := os.Chmod(path, *p.Mode); err != nil {
			return fmt.Errorf("chmod %s (%#o): %w", path, *p.Mode, err)
		}
	}
	return nil
}

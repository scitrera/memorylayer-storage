// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package tenantconfig loads blobgw's per-tenant resolver records + secret
// references from a JSON file and assembles a tenantbind.Provider from them. It
// is the dev/static-keys wiring of the ADR-001 §2.9 "resolver record":
// provisioning publishes, per tenant, a record of {endpoint, region, bucket,
// prefix, credentialRef}; blobgw reads that record at runtime to build the
// tenant→Descriptor Resolver, and resolves each credentialRef to a static
// access/secret key pair through a SecretStore.
//
// # JSON schema
//
// The config file is a single JSON object:
//
//	{
//	  "tenants": {
//	    "<tenant>": {
//	      "endpoint": "https://s3.example.com",   // optional (empty = AWS default)
//	      "region": "us-east-1",                   // required
//	      "bucket": "tenant-acme-7f3a",            // required
//	      "prefix": "packs",                        // optional
//	      "credentialRef": "acme"                   // required; key into the secret source
//	    }
//	  }
//	}
//
// Each tenant entry maps to a tenantbind.Descriptor. The credentialRef is the
// opaque, impl-interpreted reference of ADR §2.9 — here it names a static key
// pair that the FileEnvSecretStore resolves from either an environment variable
// or a JSON secrets file.
//
// # Secret resolution (FileEnvSecretStore)
//
// credentialRef is resolved to an access/secret key pair, in order:
//
//  1. Environment variables BLOBGW_TENANT_KEY_<REF> and
//     BLOBGW_TENANT_SECRET_<REF>, where <REF> is the upper-cased credentialRef
//     with every non-alphanumeric byte replaced by '_'. This keeps secrets out
//     of the config file (12-factor / K8s-secret-as-env friendly).
//  2. A JSON secrets file (optional, given by SecretsFile): a single object
//     mapping credentialRef → {"accessKey": "...", "secretKey": "..."}.
//
// The env path takes precedence so an operator can override a file-published key
// per process. A ref present in neither source resolves to
// tenantbind.ErrSecretNotFound, so the caller (StaticKeysProvider) surfaces a
// clear per-tenant failure rather than minting an empty credential.
//
// The AWS AssumeRoleWithWebIdentity provider (ADR §2.9 "Option 2") is a deferred
// milestone; this package implements only the portable static-keys source.
package tenantconfig

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/scitrera/memorylayer-storage/blobgw/tenantbind"
)

// File is the on-disk schema for the tenant resolver records (ADR §2.9). It is
// the deserialized form of the -tenant-config JSON file.
type File struct {
	// Tenants maps a tenant token to its resolver record.
	Tenants map[string]TenantRecord `json:"tenants"`
}

// TenantRecord is the per-tenant resolver record of ADR §2.9: the static,
// provisioning-published locator for one tenant's backend bucket plus the
// reference used to fetch that tenant's scoped credential.
type TenantRecord struct {
	Endpoint      string `json:"endpoint"`
	Region        string `json:"region"`
	Bucket        string `json:"bucket"`
	Prefix        string `json:"prefix"`
	CredentialRef string `json:"credentialRef"`
	// ManifestDSN is the optional per-tenant PostgreSQL DSN for slice-manifest
	// routing: non-empty routes this tenant's manifests to its own meta DB (the
	// converged mlfs -remote posture), empty keeps the S3 manifest store. Folding it
	// into the resolver record lets a tenant onboard its manifest destination as one
	// field of one record, and lets the hot-reloading resolver pick up a rebind
	// without the legacy -manifest-dsn-file startup map. It is OPTIONAL (validation
	// does not require it), but a known field so DisallowUnknownFields still rejects
	// typos.
	ManifestDSN string `json:"manifestDSN"`
}

// keyPair is one entry in the optional JSON secrets file.
type keyPair struct {
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
}

// Load reads and parses the tenant-config JSON file at path. It validates that
// each tenant record carries the required fields (region, bucket,
// credentialRef) and that the file lists at least one tenant. A malformed file,
// an unreadable path, or a record missing a required field is returned as an
// error, so a misconfiguration fails fast at startup rather than at first
// request.
func Load(path string) (*File, error) {
	if path == "" {
		return nil, fmt.Errorf("tenantconfig: empty config path")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("tenantconfig: read %q: %w", path, err)
	}
	return parseFile(path, data)
}

// LoadPath loads the tenant config at path, dispatching on whether path is a
// single file (Load) or a directory of per-tenant *.json records (LoadDir). It is
// the one-shot analogue of NewReloadingResolver's file-vs-dir detection, for
// callers that need a static snapshot of the tenant set (e.g. the GC-leader
// enumerating tenants). A missing path is an error.
func LoadPath(path string) (*File, error) {
	if path == "" {
		return nil, fmt.Errorf("tenantconfig: empty config path")
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("tenantconfig: stat %q: %w", path, err)
	}
	if info.IsDir() {
		return LoadDir(path)
	}
	return Load(path)
}

// parseFile decodes and validates already-read config bytes. It is the
// read-free half of Load, shared with the restart-free reload path
// (ReloadingResolver.reloadOnce) so both apply the identical schema, required-
// field, and prefix-ambiguity validation. path is used only for error context.
func parseFile(path string, data []byte) (*File, error) {
	f, err := decodeFile(path, data)
	if err != nil {
		return nil, err
	}
	if err := validateFile(path, f); err != nil {
		return nil, err
	}
	return f, nil
}

// decodeFile deserializes config bytes into a File under DisallowUnknownFields
// (so a typo'd field is rejected, not silently ignored). It does NOT run the
// required-field or prefix-ambiguity validation — that is validateFile's job,
// split out so the directory path (LoadDir) can decode each file individually
// and validate ONLY the merged set (a tenant complete across two files is
// nonsensical, but the prefix-ambiguity check is inherently cross-tenant). path
// is used only for error context.
func decodeFile(path string, data []byte) (*File, error) {
	var f File
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("tenantconfig: parse %q: %w", path, err)
	}
	return &f, nil
}

// validateFile applies the schema-independent validation to an already-decoded
// File: it must list at least one tenant, every tenant key must be a safe ID
// segment, every record must carry the required fields (region, bucket,
// credentialRef), and no tenant name may be a prefix of another. It is shared by
// the single-file path (parseFile) and the merged directory path (LoadDir), so
// both apply identical rules. path is used only for error context.
func validateFile(path string, f *File) error {
	if len(f.Tenants) == 0 {
		return fmt.Errorf("tenantconfig: %q lists no tenants", path)
	}
	for tenant, rec := range f.Tenants {
		if tenant == "" {
			return fmt.Errorf("tenantconfig: %q has an empty tenant key", path)
		}
		if !isSafeTenantName(tenant) {
			return fmt.Errorf("tenantconfig: tenant %q: name is not a safe ID segment (allowed: ASCII letters, digits, '-', '_'; must be non-empty)", tenant)
		}
		if rec.Region == "" {
			return fmt.Errorf("tenantconfig: tenant %q: missing required field \"region\"", tenant)
		}
		if rec.Bucket == "" {
			return fmt.Errorf("tenantconfig: tenant %q: missing required field \"bucket\"", tenant)
		}
		if rec.CredentialRef == "" {
			return fmt.Errorf("tenantconfig: tenant %q: missing required field \"credentialRef\"", tenant)
		}
	}
	// Reject any tenant whose name is a prefix of another tenant's name. Blob IDs
	// are namespaced as "pack-<tenant>-"/"chunk-<tenant>-", and the GC sweeper
	// lists by that prefix; if "acme" is a prefix of "acme-eu", a prefix-listing
	// for "acme" also matches "acme-eu"'s blobs, so in a shared-backend posture
	// acme's GC pass could delete acme-eu's packs (cross-tenant deletion). Fail
	// fast at config load rather than risk that at sweep time.
	return rejectPrefixAmbiguousTenants(path, f)
}

// LoadDir reads every *.json file directly under dir (non-recursive), parses
// each as a {"tenants":{...}} File, and MERGES them into one File — the
// onboarding-friendly layout where each tenant is a single file/Secret-key, so
// adding or removing one tenant is one file, not a rewrite of a shared blob.
//
// Robustness is the point: a single malformed or unreadable file is SKIPPED with
// a WARN rather than failing the whole load, so one broken tenant file cannot
// blank the entire tenant set. But if ZERO valid tenants result (no *.json, or
// every file was skipped) it returns an error, mirroring the single-file "lists
// no tenants" fail-fast. A tenant name appearing in two files is an error (two
// files claiming the same tenant is ambiguous — fail rather than silently pick a
// winner). The required-field + prefix-ambiguity validation runs once over the
// MERGED set (the prefix check is inherently cross-tenant, so it must see every
// file's tenants together).
func LoadDir(dir string) (*File, error) {
	if dir == "" {
		return nil, fmt.Errorf("tenantconfig: empty config dir")
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, fmt.Errorf("tenantconfig: glob %q: %w", dir, err)
	}
	sort.Strings(paths) // deterministic order for the duplicate-detection error message
	merged := &File{Tenants: make(map[string]TenantRecord)}
	for _, p := range paths {
		data, rerr := os.ReadFile(p)
		if rerr != nil {
			// A single unreadable file must not sink the whole load (onboarding
			// robustness) — skip it and keep going.
			slog.Warn("blobgw: tenant-config dir: skipping unreadable file", "path", p, "err", rerr)
			continue
		}
		// Decode-only (no per-file "lists no tenants"/prefix validation): the merged
		// set is validated below. A malformed file is skipped, not fatal.
		f, derr := decodeFile(p, data)
		if derr != nil {
			slog.Warn("blobgw: tenant-config dir: skipping malformed file", "path", p, "err", derr)
			continue
		}
		for tenant, rec := range f.Tenants {
			if _, dup := merged.Tenants[tenant]; dup {
				// A duplicate tenant across two files is a hard error: silently choosing
				// a winner could bind a tenant to the wrong bucket/credential.
				return nil, fmt.Errorf("tenantconfig: dir %q: tenant %q defined in more than one file (last seen %q)", dir, tenant, p)
			}
			merged.Tenants[tenant] = rec
		}
	}
	// Zero valid tenants (empty dir, all files skipped, or every file empty) fails
	// fast, exactly like the single-file "lists no tenants" case.
	if err := validateFile(dir, merged); err != nil {
		return nil, err
	}
	return merged, nil
}

// isSafeTenantName reports whether name is a safe ID segment for embedding in a
// blob-ID prefix ("pack-<tenant>-"): non-empty and composed only of ASCII
// letters, digits, '-', and '_'. Anything else (spaces, slashes, dots, non-
// ASCII) is rejected so a tenant token can't smuggle structure into the ID
// namespace the GC sweeper lists over.
func isSafeTenantName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case c == '-' || c == '_':
		default:
			return false
		}
	}
	return true
}

// rejectPrefixAmbiguousTenants returns an error if any configured tenant name is
// a prefix of another. See Load for why (GC prefix-listing safety): a prefix
// relationship makes one tenant's prefix-scoped blob listing match a second
// tenant's blobs, which the GC sweeper would then treat as in-scope.
func rejectPrefixAmbiguousTenants(path string, f *File) error {
	names := f.TenantNames()
	for _, a := range names {
		for _, b := range names {
			if a == b {
				continue
			}
			if strings.HasPrefix(b, a) {
				return fmt.Errorf("tenantconfig: %q: tenant name %q is a prefix of %q; this makes their blob-ID namespaces (\"pack-<tenant>-\") ambiguous and could let one tenant's GC delete the other's packs", path, a, b)
			}
		}
	}
	return nil
}

// TenantNames returns the configured tenant tokens, for callers that need to
// enumerate the per-tenant set (e.g. the GC runner sweeping each tenant's pack
// store). The order is unspecified (map iteration); callers that need stability
// should sort. Returns an empty slice when no tenants are configured.
func (f *File) TenantNames() []string {
	names := make([]string, 0, len(f.Tenants))
	for tenant := range f.Tenants {
		names = append(names, tenant)
	}
	return names
}

// Resolver builds a tenantbind.MapResolver from the parsed records: one
// Descriptor per tenant. It is the tenant→Descriptor half of the ADR §2.9 seam.
func (f *File) Resolver() *tenantbind.MapResolver {
	r := tenantbind.NewMapResolver()
	for tenant, rec := range f.Tenants {
		r.Set(tenant, tenantbind.Descriptor{
			Endpoint:      rec.Endpoint,
			Region:        rec.Region,
			Bucket:        rec.Bucket,
			Prefix:        rec.Prefix,
			CredentialRef: rec.CredentialRef,
			ManifestDSN:   rec.ManifestDSN,
		})
	}
	return r
}

// FileEnvSecretStore is a tenantbind.SecretStore that resolves a credentialRef
// to a static access/secret key pair from environment variables first, then an
// optional JSON secrets file. See the package doc for the schema and lookup
// order. It is safe for concurrent use (its state is read-only after Load).
type FileEnvSecretStore struct {
	keys map[string]keyPair // from the optional secrets file; nil if none
}

var _ tenantbind.SecretStore = (*FileEnvSecretStore)(nil)

// NewFileEnvSecretStore builds a FileEnvSecretStore. secretsFile is optional: if
// empty, only the environment-variable source is used. A non-empty path that is
// unreadable or malformed is returned as an error (fail fast at startup).
func NewFileEnvSecretStore(secretsFile string) (*FileEnvSecretStore, error) {
	s := &FileEnvSecretStore{}
	if secretsFile == "" {
		return s, nil
	}
	data, err := os.ReadFile(secretsFile)
	if err != nil {
		return nil, fmt.Errorf("tenantconfig: read secrets file %q: %w", secretsFile, err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	keys := map[string]keyPair{}
	if err := dec.Decode(&keys); err != nil {
		return nil, fmt.Errorf("tenantconfig: parse secrets file %q: %w", secretsFile, err)
	}
	s.keys = keys
	return s, nil
}

// Get resolves ref to a key pair, env first then the secrets file. A ref present
// in neither source returns tenantbind.ErrSecretNotFound. A partially-specified
// source (only the access key or only the secret key) is an error: minting a
// half-credential would fail opaquely downstream in the S3 signer.
func (s *FileEnvSecretStore) Get(_ context.Context, ref string) (accessKey, secretKey string, err error) {
	if ref == "" {
		return "", "", fmt.Errorf("tenantconfig: empty credentialRef")
	}
	envKey := os.Getenv("BLOBGW_TENANT_KEY_" + envSuffix(ref))
	envSecret := os.Getenv("BLOBGW_TENANT_SECRET_" + envSuffix(ref))
	if envKey != "" || envSecret != "" {
		if envKey == "" || envSecret == "" {
			return "", "", fmt.Errorf("tenantconfig: ref %q: incomplete env credential (need both BLOBGW_TENANT_KEY_* and BLOBGW_TENANT_SECRET_*)", ref)
		}
		return envKey, envSecret, nil
	}
	if kp, ok := s.keys[ref]; ok {
		if kp.AccessKey == "" || kp.SecretKey == "" {
			return "", "", fmt.Errorf("tenantconfig: ref %q: incomplete credential in secrets file (need both accessKey and secretKey)", ref)
		}
		return kp.AccessKey, kp.SecretKey, nil
	}
	return "", "", fmt.Errorf("%w: %q", tenantbind.ErrSecretNotFound, ref)
}

// envSuffix upper-cases ref and replaces every non-alphanumeric byte with '_',
// producing the BLOBGW_TENANT_KEY_<REF> / BLOBGW_TENANT_SECRET_<REF> suffix.
func envSuffix(ref string) string {
	var b strings.Builder
	b.Grow(len(ref))
	// Intentional byte loop: refs are operator-controlled ASCII; non-ASCII bytes
	// deliberately collapse to '_'. Do not change to a rune loop — it would alter
	// the env-var mapping contract.
	for i := 0; i < len(ref); i++ {
		c := ref[i]
		switch {
		case c >= 'a' && c <= 'z':
			b.WriteByte(c - ('a' - 'A'))
		case (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'):
			b.WriteByte(c)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

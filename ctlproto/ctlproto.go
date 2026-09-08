// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package ctlproto defines the control-plane message contract, a swappable
// codec, and NATS subject conventions shared by the blobgw server and the
// mlfs-node client (RemoteDedupStore).
//
// # Architecture reference
//
// See ADR-001 §2.5: the internal node ↔ blobgw control plane uses NATS
// request-reply. blobgw replicas subscribe to subjects of the form
// blobgw.<tenant>.* as a queue group so NATS load-balances across replicas —
// no external load balancer is required for control-plane scale-out.
//
// # Codec
//
// The ADR commits to protobuf as the wire format (schema evolution + python
// edge client contract). The generated Go types and a ProtobufCodec will be
// provided once protoc-gen-go is available in the build toolchain. Until then
// the wire types are hand-written Go structs and the default codec is
// JSONCodec. The Codec interface is intentionally the only abstraction here:
// callers hold a Codec and the swap to protobuf requires only switching which
// Codec implementation is passed in — no call-site changes. See
// controlplane.proto for the canonical message schema.
//
// # NATS payload limits
//
// NATS defaults to a 1 MB max payload. Callers MUST split batches larger than
// MaxChunkHashesPerBatch (for LookupBatch / RecordRequest) or
// MaxPresignKeysPerBatch (for PresignBatchRequest) across multiple requests.
// Both constants are sized conservatively so a full batch fits well within
// 1 MB; see their doc comments for the sizing rationale.
//
// # Tenant validation
//
// Tenant strings are used directly as NATS subject tokens. A tenant string
// MUST NOT contain dots ('.'), spaces, or the NATS wildcard characters ('*',
// '>'); these would break subject routing. ValidateTenant returns an error for
// any such input and all subject helpers call it internally.
package ctlproto

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

// Codec is the swappable serialisation interface for control-plane messages.
// The committed target format is protobuf (see controlplane.proto and ADR-001
// §2.5). JSONCodec is the default until protoc-gen-go is in the build chain.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONCodec marshals/unmarshals using encoding/json. It is the interim default
// pending the protobuf migration; see the package doc.
type JSONCodec struct{}

// Marshal serialises v to JSON.
func (JSONCodec) Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("ctlproto: json marshal: %w", err)
	}
	return b, nil
}

// Unmarshal deserialises JSON data into v.
func (JSONCodec) Unmarshal(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("ctlproto: json unmarshal: %w", err)
	}
	return nil
}

// DefaultCodec is the codec used when callers do not supply one explicitly.
// Swap this (or pass a Codec to your RPC layer) to switch to protobuf once
// protoc-gen-go is available.
var DefaultCodec Codec = JSONCodec{}

// ---------------------------------------------------------------------------
// Shared wire types
// ---------------------------------------------------------------------------

// PackRef locates a content-addressed chunk within a pack blob. It is the
// value half of the dedup mapping (chunk hash → where the chunk physically
// lives in the object store). The blobgw server maps this to and from
// snapshot.PackRef; the mlfs client maps it to its own local representation.
type PackRef struct {
	PackHash string `json:"pack_hash"`
	Offset   int64  `json:"offset"`
	Size     int64  `json:"size"`
}

// ChunkLocation pairs a chunk's content hash with its PackRef. It is the unit
// a RecordRequest conveys — one entry per chunk that has been packed and
// uploaded, mirroring the pack_manifest(domain, chunk_hash, pack_hash, offset,
// length) row.
type ChunkLocation struct {
	ChunkHash string  `json:"chunk_hash"`
	PackRef   PackRef `json:"pack_ref"`
}

// ---------------------------------------------------------------------------
// PresignOp
// ---------------------------------------------------------------------------

// PresignOp is the HTTP verb a presigned URL will be valid for.
//
// Codec migration note: the current JSONCodec encodes PresignOp as its string
// value ("GET" or "PUT"). The .proto definition declares PresignOp as an enum
// (GET=1, PUT=2); switching to a ProtobufCodec changes the wire representation
// from the string name to an integer. A rolling upgrade requires all nodes to
// adopt the new codec simultaneously, or the proto enum must be annotated to
// preserve string-name encoding (e.g. via protojson or a custom codec). Plan
// the codec migration as a coordinated cutover, not a gradual rollout, to
// avoid mixed-codec clusters misrouting presign requests.
type PresignOp string

const (
	// PresignGet mints a presigned URL that authorises a GET (read) of the
	// object. Used by mlfs nodes fetching pack blobs from the tenant S3 bucket.
	PresignGet PresignOp = "GET"

	// PresignPut mints a presigned URL that authorises a PUT (write) of the
	// object. Used by mlfs nodes uploading newly assembled pack blobs.
	PresignPut PresignOp = "PUT"
)

// ---------------------------------------------------------------------------
// LookupBatch
// ---------------------------------------------------------------------------

// LookupBatchRequest asks blobgw to resolve a batch of chunk hashes to their
// pack locations within Domain. Chunk hashes not present in the index are
// simply absent from the response map (they are novel chunks that the caller
// must pack and upload).
//
// Batch size: callers MUST split slices longer than MaxChunkHashesPerBatch
// across multiple requests to stay within the NATS 1 MB payload limit.
type LookupBatchRequest struct {
	Domain      string   `json:"domain"`
	ChunkHashes []string `json:"chunk_hashes"`
}

// LookupBatchResponse carries the resolved chunk locations. Locations contains
// only the hashes that were found in the index. Error is non-empty when the
// server encountered a fatal error; on error, Locations may be nil or partial.
type LookupBatchResponse struct {
	Locations map[string]PackRef `json:"locations"`
	Error     string             `json:"error,omitempty"`
}

// ---------------------------------------------------------------------------
// Record
// ---------------------------------------------------------------------------

// RecordRequest asks blobgw to persist a batch of chunk locations in Domain.
// This is called after pack blobs have been durably uploaded to S3 (ADR-001
// §4 invariant I1: durability before pointer). Recording is idempotent —
// first-writer-wins at the chunk_hash primary key.
//
// Batch size: callers MUST split slices longer than MaxChunkHashesPerBatch
// across multiple requests.
type RecordRequest struct {
	Domain    string          `json:"domain"`
	Locations []ChunkLocation `json:"locations"`
}

// RecordResponse is returned by blobgw after a Record RPC. Error is non-empty
// on failure.
type RecordResponse struct {
	Error string `json:"error,omitempty"`
}

// ---------------------------------------------------------------------------
// PresignBatch
// ---------------------------------------------------------------------------

// PresignBatchRequest asks blobgw to mint a batch of presigned S3 URLs for
// the given object keys within Domain. Op selects GET (read) or PUT (write).
// TTLSeconds is the requested validity duration; blobgw may cap it based on
// the credential impl (e.g. AWS STS session limit — see ADR-001 §2.9).
//
// Batch size: callers MUST split slices longer than MaxPresignKeysPerBatch
// across multiple requests to stay within the NATS 1 MB payload limit.
type PresignBatchRequest struct {
	Domain     string    `json:"domain"`
	Op         PresignOp `json:"op"`
	Keys       []string  `json:"keys"`
	TTLSeconds int32     `json:"ttl_seconds"`
}

// PresignBatchResponse carries the minted presigned URLs. URLs maps each
// requested key to its presigned URL. ExpiresAtUnix is the Unix timestamp
// (seconds) at which all URLs in this batch expire; the node should schedule
// a refresh before this time (refresh-on-expiry is required by the AWS STS
// impl — see ADR-001 §2.9). Error is non-empty on failure.
type PresignBatchResponse struct {
	URLs          map[string]string `json:"urls"`
	ExpiresAtUnix int64             `json:"expires_at_unix"`
	Error         string            `json:"error,omitempty"`
}

// ---------------------------------------------------------------------------
// PurgePacks
// ---------------------------------------------------------------------------

// PurgePacksRequest asks blobgw to remove every index entry in Domain that
// points to one of the given pack hashes. GC calls this after it reclaims a
// pack blob so the index never hands out a reference to collected bytes.
// Purging an absent pack is a no-op (idempotent).
type PurgePacksRequest struct {
	Domain     string   `json:"domain"`
	PackHashes []string `json:"pack_hashes"`
}

// PurgePacksResponse is returned by blobgw after a PurgePacks RPC. Error is
// non-empty on failure.
type PurgePacksResponse struct {
	Error string `json:"error,omitempty"`
}

// ---------------------------------------------------------------------------
// Pack associations and reclamation (casstore PackAssociator / PackReclaimer)
// ---------------------------------------------------------------------------

// PackAssociation binds one pack to one owning context. It mirrors
// snapshot.PackAssociation on the wire.
type PackAssociation struct {
	PackHash string `json:"pack_hash"`
	Context  string `json:"context"`
}

// AssociateRequest records that each context references each pack, so
// reclamation can later scope itself to an owner without walking the store. A
// filesystem node issues this on its write path; only the association WRITE
// crosses the wire, because reclamation needs the durable index and the blob
// store together and therefore runs blobgw-side.
type AssociateRequest struct {
	Domain       string            `json:"domain"`
	Associations []PackAssociation `json:"associations"`
}

// AssociateResponse is returned after an Associate RPC. Error is non-empty on
// failure. Obliterating is set when the batch was refused because a named pack is
// being reclaimed — the caller must treat its write as failed and retry, which is
// distinct from a transport or backend error.
type AssociateResponse struct {
	Error        string `json:"error,omitempty"`
	Obliterating bool   `json:"obliterating,omitempty"`
}

// ObliterateContextRequest asks blobgw to reclaim the bytes an owner context
// held, using the mark-based sequence. The node cannot do this itself: it needs
// the durable association index and blob deletion authority at once.
type ObliterateContextRequest struct {
	Domain  string `json:"domain"`
	Context string `json:"context"`
}

// ObliterateContextResponse reports what a reclamation pass did. The counters
// mirror snapshot.ReclaimResult so an operator sees the same numbers on both
// sides of the wire.
type ObliterateContextResponse struct {
	Error                string `json:"error,omitempty"`
	PacksConsidered      int    `json:"packs_considered,omitempty"`
	PacksReclaimed       int    `json:"packs_reclaimed,omitempty"`
	PacksStillReferenced int    `json:"packs_still_referenced,omitempty"`
	PacksHeldByOther     int    `json:"packs_held_by_other,omitempty"`
	PacksSkippedError    int    `json:"packs_skipped_error,omitempty"`
	BytesReclaimed       int64  `json:"bytes_reclaimed,omitempty"`
}

// ---------------------------------------------------------------------------
// Payload limits
// ---------------------------------------------------------------------------

// MaxChunkHashesPerBatch is the maximum number of chunk hashes a caller
// should include in a single LookupBatchRequest or RecordRequest.
//
// Sizing — the binding worst case is the RESPONSE direction, not the request:
//
//	RecordRequest:        one ChunkLocation per entry ≈ 228 bytes (64-char
//	                      chunk hash + 64-char pack hash + offset + size +
//	                      JSON field names and delimiters). 4 000 × 228 ≈ 912 KB.
//
//	LookupBatchResponse:  at full hit-rate the response is a map[string]PackRef
//	                      ≈ 213 bytes per entry (64-char chunk hash key +
//	                      PackRef fields). 4 000 × 213 ≈ 852 KB.
//
//	LookupBatchRequest:   request side is only the hash array ≈ 70 bytes per
//	                      hash; 4 000 × 70 ≈ 280 KB — well under the limit.
//
// All three fit within 900 KB, leaving margin under the NATS 1 MB default
// max_payload for NATS framing and domain/field overhead. Callers MUST split
// larger sets across multiple sequential requests.
const MaxChunkHashesPerBatch = 4_000

// MaxPackHashesPerBatch is the maximum number of pack hashes a caller should
// include in a single PurgePacksRequest.
//
// Sizing: a PurgePacksRequest carries only pack hashes (no PackRef values),
// so the per-entry cost is lower (~70 bytes per 64-char hex hash). We use the
// same conservative value as MaxChunkHashesPerBatch (4 000) so a single
// constant governs all chunked RPC loops and the math stays straightforward:
// 4 000 × 70 ≈ 280 KB, well within the 900 KB target. Callers MUST split
// larger sets across multiple sequential requests.
const MaxPackHashesPerBatch = 4_000

// MaxPresignKeysPerBatch is the maximum number of S3 object keys a caller
// should include in a single PresignBatchRequest.
//
// Sizing: S3 object keys are up to 1024 bytes. Assuming a typical pack key
// (~80 bytes) and URL response (~300 bytes each), 2 000 keys fits within
// ~760 KB — safely under the NATS 1 MB limit. Callers MUST split larger sets
// across multiple sequential requests.
const MaxPresignKeysPerBatch = 2_000

// ---------------------------------------------------------------------------
// NATS subject helpers
// ---------------------------------------------------------------------------

// QueueGroup is the NATS queue group name that all blobgw replicas subscribe
// under for control-plane subjects. NATS delivers each incoming request to
// exactly one member of the group, providing load-balanced scale-out without
// an external load balancer. To scale blobgw horizontally, run more replicas
// subscribing to the same subjects with this queue group name.
const QueueGroup = "blobgw"

// ValidateTenant checks that tenant is safe to use as a NATS subject token.
// A tenant string MUST NOT contain '.', ' ', '*', or '>' because these
// characters have special meaning in NATS subject routing and would silently
// mismatch or broadcast to unintended subscribers. An empty tenant is also
// rejected.
func ValidateTenant(tenant string) error {
	if tenant == "" {
		return fmt.Errorf("ctlproto: tenant must not be empty")
	}
	for _, ch := range []string{".", " ", "*", ">"} {
		if strings.Contains(tenant, ch) {
			return fmt.Errorf("ctlproto: tenant %q must not contain %q (invalid NATS subject token)", tenant, ch)
		}
	}
	return nil
}

// TenantFromSubject extracts the tenant token from a control-plane subject of
// the form blobgw.<tenant>.<op...> (e.g. blobgw.acme.index.lookup →
// "acme"). It is the inverse of the *Subject helpers and is used by the blobgw
// server, which subscribes with a wildcard (blobgw.*.index.lookup, …) across
// all tenants and must recover the concrete tenant from the delivered subject.
//
// ok is false when subject does not have the blobgw.<tenant>.* shape or when
// the extracted token is not a valid tenant (per ValidateTenant). Callers
// should still re-run ValidateTenant if they want the specific error; this
// helper only reports whether a usable tenant token was found.
func TenantFromSubject(subject string) (tenant string, ok bool) {
	parts := strings.Split(subject, ".")
	// Smallest valid subject is blobgw.<tenant>.presign → 3 tokens; the index
	// and gc subjects have 4. Anything shorter cannot carry a tenant token.
	if len(parts) < 3 || parts[0] != "blobgw" {
		return "", false
	}
	tenant = parts[1]
	if ValidateTenant(tenant) != nil {
		return "", false
	}
	return tenant, true
}

// IndexLookupSubject returns the NATS subject for LookupBatch RPCs for the
// given tenant: blobgw.<tenant>.index.lookup
//
// blobgw replicas subscribe as queue group QueueGroup; NATS load-balances
// requests across them.
func IndexLookupSubject(tenant string) string {
	if err := ValidateTenant(tenant); err != nil {
		panic(err)
	}
	return "blobgw." + tenant + ".index.lookup"
}

// IndexRecordSubject returns the NATS subject for Record RPCs for the given
// tenant: blobgw.<tenant>.index.record
func IndexRecordSubject(tenant string) string {
	if err := ValidateTenant(tenant); err != nil {
		panic(err)
	}
	return "blobgw." + tenant + ".index.record"
}

// PresignSubject returns the NATS subject for PresignBatch RPCs for the given
// tenant: blobgw.<tenant>.presign
func PresignSubject(tenant string) string {
	if err := ValidateTenant(tenant); err != nil {
		panic(err)
	}
	return "blobgw." + tenant + ".presign"
}

// GCPurgeSubject returns the NATS subject for PurgePacks RPCs for the given
// tenant: blobgw.<tenant>.gc.purge
func GCPurgeSubject(tenant string) string {
	if err := ValidateTenant(tenant); err != nil {
		panic(err)
	}
	return "blobgw." + tenant + ".gc.purge"
}

// AssocRecordSubject returns the NATS subject for Associate RPCs for the given
// tenant: blobgw.<tenant>.assoc.record
func AssocRecordSubject(tenant string) string {
	if err := ValidateTenant(tenant); err != nil {
		panic(err)
	}
	return "blobgw." + tenant + ".assoc.record"
}

// AssocObliterateSubject returns the NATS subject for ObliterateContext RPCs for
// the given tenant: blobgw.<tenant>.assoc.obliterate
func AssocObliterateSubject(tenant string) string {
	if err := ValidateTenant(tenant); err != nil {
		panic(err)
	}
	return "blobgw." + tenant + ".assoc.obliterate"
}

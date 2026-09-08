// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package controlplane

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
	"github.com/scitrera/memorylayer-storage/ctlproto"
)

// handleLookup serves blobgw.*.index.lookup: decode → LookupBatch → encode. A
// validation/decode/store error is reported via the response's Error field (the
// client always gets a reply), per the ctlproto contract.
// handleLookup returns the effective handler outcome (nil on success, the error
// surfaced to the client otherwise) so Start's wrapper records the cp.* RED
// metrics. A dropped invalid-subject request returns nil — it never reached the
// operation.
func (s *Server) handleLookup(ctx context.Context, msg *nats.Msg) error {
	tenant, ok := s.tenantFromMsg(msg)
	if !ok {
		return nil
	}
	var req ctlproto.LookupBatchRequest
	if err := s.codec.Unmarshal(msg.Data, &req); err != nil {
		s.reply(msg, ctlproto.LookupBatchResponse{Error: fmt.Sprintf("decode lookup request: %v", err)})
		return err
	}
	if err := validateDomain(req.Domain); err != nil {
		s.reply(msg, ctlproto.LookupBatchResponse{Error: err.Error()})
		return err
	}
	if len(req.ChunkHashes) > ctlproto.MaxChunkHashesPerBatch {
		s.reply(msg, ctlproto.LookupBatchResponse{Error: oversizeErr("chunk hashes", len(req.ChunkHashes), ctlproto.MaxChunkHashesPerBatch)})
		return errors.New("oversize batch")
	}

	cctx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	found, err := s.index.LookupBatch(cctx, req.Domain, req.ChunkHashes)
	if err != nil {
		s.logger.Error("controlplane: lookup", "tenant", tenant, "domain", req.Domain, "err", err)
		s.reply(msg, ctlproto.LookupBatchResponse{Error: fmt.Sprintf("lookup: %v", err)})
		return err
	}

	out := make(map[string]ctlproto.PackRef, len(found))
	for hash, ref := range found {
		out[hash] = toWirePackRef(ref)
	}
	s.reply(msg, ctlproto.LookupBatchResponse{Locations: out})
	return nil
}

// handleRecord serves blobgw.*.index.record: decode → Record → encode. Returns
// the effective handler outcome for the cp.* RED metrics.
func (s *Server) handleRecord(ctx context.Context, msg *nats.Msg) error {
	tenant, ok := s.tenantFromMsg(msg)
	if !ok {
		return nil
	}
	var req ctlproto.RecordRequest
	if err := s.codec.Unmarshal(msg.Data, &req); err != nil {
		s.reply(msg, ctlproto.RecordResponse{Error: fmt.Sprintf("decode record request: %v", err)})
		return err
	}
	if err := validateDomain(req.Domain); err != nil {
		s.reply(msg, ctlproto.RecordResponse{Error: err.Error()})
		return err
	}
	if len(req.Locations) > ctlproto.MaxChunkHashesPerBatch {
		s.reply(msg, ctlproto.RecordResponse{Error: oversizeErr("chunk locations", len(req.Locations), ctlproto.MaxChunkHashesPerBatch)})
		return errors.New("oversize batch")
	}

	locs := make([]snapshot.ChunkLocation, len(req.Locations))
	for i, l := range req.Locations {
		locs[i] = snapshot.ChunkLocation{
			ChunkHash: l.ChunkHash,
			PackRef:   fromWirePackRef(l.PackRef),
		}
	}

	cctx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	if err := s.index.Record(cctx, req.Domain, locs); err != nil {
		s.logger.Error("controlplane: record", "tenant", tenant, "domain", req.Domain, "err", err)
		s.reply(msg, ctlproto.RecordResponse{Error: fmt.Sprintf("record: %v", err)})
		return err
	}
	s.reply(msg, ctlproto.RecordResponse{})
	return nil
}

// handlePurge serves blobgw.*.gc.purge: decode → PurgePacks → encode. Returns the
// effective handler outcome for the cp.* RED metrics.
func (s *Server) handlePurge(ctx context.Context, msg *nats.Msg) error {
	tenant, ok := s.tenantFromMsg(msg)
	if !ok {
		return nil
	}
	var req ctlproto.PurgePacksRequest
	if err := s.codec.Unmarshal(msg.Data, &req); err != nil {
		s.reply(msg, ctlproto.PurgePacksResponse{Error: fmt.Sprintf("decode purge request: %v", err)})
		return err
	}
	if err := validateDomain(req.Domain); err != nil {
		s.reply(msg, ctlproto.PurgePacksResponse{Error: err.Error()})
		return err
	}

	cctx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	if err := s.index.PurgePacks(cctx, req.Domain, req.PackHashes); err != nil {
		s.logger.Error("controlplane: purge", "tenant", tenant, "domain", req.Domain, "err", err)
		s.reply(msg, ctlproto.PurgePacksResponse{Error: fmt.Sprintf("purge: %v", err)})
		return err
	}
	s.reply(msg, ctlproto.PurgePacksResponse{})
	return nil
}

// validateDomain rejects an empty request domain. The domain is a NAMESPACE key
// for the shared dedup index (snapshot.DedupStore doc) — the trust boundary that
// keeps one tenant's chunks from pooling with another's — and it MUST be present.
//
// The domain is NOT a credential selector: per-tenant credentials/buckets are
// resolved from the subject-validated tenant (TenantFromSubject/ValidateTenant),
// never from this client-supplied field (ADR §2.6/§2.9). We do not force
// domain==tenant here — the mapping from the subject's tenant token to the dedup
// domain is the caller's (ADR §2.5/§2.6) — but an empty domain is always a client
// bug and would silently pool every tenant's chunks together.
func validateDomain(domain string) error {
	if domain == "" {
		return errors.New("empty domain")
	}
	return nil
}

// oversizeErr formats the rejection message for a batch exceeding its limit.
func oversizeErr(what string, n, limit int) string {
	return fmt.Sprintf("%s batch too large: %d > %d (split across requests)", what, n, limit)
}

// toWirePackRef maps a snapshot.PackRef to its ctlproto wire form. snapshot uses
// int offset/size; the wire contract uses int64.
func toWirePackRef(r snapshot.PackRef) ctlproto.PackRef {
	return ctlproto.PackRef{
		PackHash: r.PackHash,
		Offset:   int64(r.Offset),
		Size:     int64(r.Size),
	}
}

// fromWirePackRef maps a ctlproto.PackRef back to a snapshot.PackRef.
func fromWirePackRef(r ctlproto.PackRef) snapshot.PackRef {
	return snapshot.PackRef{
		PackHash: r.PackHash,
		Offset:   int(r.Offset),
		Size:     int(r.Size),
	}
}

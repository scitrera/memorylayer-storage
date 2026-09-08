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

// handleAssociate serves the Associate RPC: a filesystem node telling blobgw
// which owner context references which packs, so reclamation can later be scoped
// to that owner without walking the store.
//
// Only the association WRITE crosses the wire. Reclamation itself needs the
// durable index and blob-deletion authority together, so it runs blobgw-side
// against the per-tenant gateway rather than being driven by a node.
//
// A refusal (the pack is being reclaimed) is reported distinctly from a backend
// error via Obliterating, because the two demand different client behaviour: a
// refusal means "your write raced a reclamation — retry and you will re-pack",
// while an error means "try again later".
func (s *Server) handleAssociate(ctx context.Context, msg *nats.Msg) error {
	tenant, ok := s.tenantFromMsg(msg)
	if !ok {
		return nil
	}
	var req ctlproto.AssociateRequest
	if err := s.codec.Unmarshal(msg.Data, &req); err != nil {
		s.reply(msg, ctlproto.AssociateResponse{Error: fmt.Sprintf("decode associate request: %v", err)})
		return err
	}
	if err := validateDomain(req.Domain); err != nil {
		s.reply(msg, ctlproto.AssociateResponse{Error: err.Error()})
		return err
	}

	associator, ok := s.index.(snapshot.PackAssociator)
	if !ok {
		// The configured dedup index predates associations. Report it rather than
		// silently succeeding, which would leave the node believing its bytes are
		// tracked for reclamation when they are not.
		s.reply(msg, ctlproto.AssociateResponse{Error: "dedup index does not support pack associations"})
		return errors.New("controlplane: associate: index has no association support")
	}

	assocs := make([]snapshot.PackAssociation, 0, len(req.Associations))
	for _, a := range req.Associations {
		assocs = append(assocs, snapshot.PackAssociation{PackHash: a.PackHash, Context: a.Context})
	}

	cctx, cancel := context.WithTimeout(ctx, s.requestTimeout)
	defer cancel()
	if err := associator.Associate(cctx, req.Domain, assocs); err != nil {
		if errors.Is(err, snapshot.ErrPackObliterating) {
			// Expected under a concurrent reclamation, not an operational fault:
			// log at a lower level and let the client retry.
			s.logger.Info("controlplane: associate refused; pack is being reclaimed",
				"tenant", tenant, "domain", req.Domain)
			s.reply(msg, ctlproto.AssociateResponse{
				Error:        err.Error(),
				Obliterating: true,
			})
			return nil
		}
		s.logger.Error("controlplane: associate", "tenant", tenant, "domain", req.Domain, "err", err)
		s.reply(msg, ctlproto.AssociateResponse{Error: fmt.Sprintf("associate: %v", err)})
		return err
	}
	s.reply(msg, ctlproto.AssociateResponse{})
	return nil
}

// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package metrics

import (
	"context"
	"testing"
)

// TestNewTracingDisabled proves the disabled-safe contract: with no OTLP endpoint
// NewTracing returns a nil *Tracing (and no error), and Shutdown on it is a
// no-op. This is the default daemon configuration (traces need a collector), so
// it must never error or panic — the global provider stays the OTEL no-op and
// casstore/bridge spans cost nothing.
func TestNewTracingDisabled(t *testing.T) {
	tr, err := NewTracing(context.Background(), 1) // no WithOTLP → no endpoint
	if err != nil {
		t.Fatalf("NewTracing (disabled) returned error: %v", err)
	}
	if tr != nil {
		t.Fatalf("NewTracing (disabled) = %v, want nil", tr)
	}
	if err := tr.Shutdown(context.Background()); err != nil {
		t.Fatalf("nil Tracing Shutdown returned error: %v", err)
	}
}

// TestNewTracingDisabledIgnoresIdentity confirms identity options alone (without
// an endpoint) still yield the disabled no-op provider — the trace half only
// stands up when there is somewhere to send the spans.
func TestNewTracingDisabledIgnoresIdentity(t *testing.T) {
	tr, err := NewTracing(context.Background(), 0.5, WithIdentity("dom", "node-1", 3))
	if err != nil {
		t.Fatalf("NewTracing returned error: %v", err)
	}
	if tr != nil {
		t.Fatalf("NewTracing with identity but no endpoint = %v, want nil", tr)
	}
}

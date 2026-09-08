// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package metrics

import (
	"context"
	"database/sql"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// poolGauges registers the three observable DB connection-pool gauges and tracks
// the set of pools to observe on each collection. One callback fans out over every
// registered pool, tagging each by a bounded `pool` label so the index pool and a
// per-tenant manifest pool are distinguishable without unbounded cardinality.
type poolGauges struct {
	inUse     metric.Int64ObservableGauge
	idle      metric.Int64ObservableGauge
	waitCount metric.Int64ObservableGauge

	mu    sync.Mutex
	pools map[string]*sql.DB // name → pool
}

// BindPool registers db's connection-pool stats as observable gauges
// (blobgw.pg.pool.in_use / idle / wait_count), tagged pool=name. It is idempotent
// per name (a later BindPool with the same name replaces the pool). The first call
// lazily creates the gauges + the single observe callback. Safe for a nil/no-op
// DataPath (it then no-ops) and concurrent use.
//
// USE saturation view (docs/OBSERVABILITY.md): in_use vs idle shows utilization;
// wait_count climbing means the pool is a bottleneck (callers blocking on a free
// connection) — the signal that the -db-max-open-conns ceiling is too low.
func (d *DataPath) BindPool(meter metric.Meter, name string, db *sql.DB) error {
	if d == nil || db == nil || name == "" {
		return nil
	}
	if d.pool == nil {
		p := &poolGauges{pools: make(map[string]*sql.DB)}
		var err error
		if p.inUse, err = meter.Int64ObservableGauge("blobgw.pg.pool.in_use",
			metric.WithDescription("Connections currently in use, by pool")); err != nil {
			return err
		}
		if p.idle, err = meter.Int64ObservableGauge("blobgw.pg.pool.idle",
			metric.WithDescription("Idle connections in the pool, by pool")); err != nil {
			return err
		}
		if p.waitCount, err = meter.Int64ObservableGauge("blobgw.pg.pool.wait_count",
			metric.WithDescription("Cumulative count of connection waits (pool saturation), by pool")); err != nil {
			return err
		}
		if _, err = meter.RegisterCallback(p.observe, p.inUse, p.idle, p.waitCount); err != nil {
			return err
		}
		d.pool = p
	}
	d.pool.mu.Lock()
	d.pool.pools[name] = db
	d.pool.mu.Unlock()
	return nil
}

// observe is the single callback feeding all three gauges for every registered
// pool, tagged pool=name.
func (p *poolGauges) observe(_ context.Context, o metric.Observer) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for name, db := range p.pools {
		st := db.Stats()
		a := metric.WithAttributes(attribute.String("pool", name))
		o.ObserveInt64(p.inUse, int64(st.InUse), a)
		o.ObserveInt64(p.idle, int64(st.Idle), a)
		o.ObserveInt64(p.waitCount, st.WaitCount, a)
	}
	return nil
}

# blobgw control plane: account-per-domain isolation (ADR-001 §7.7)

How to run ONE converged blobgw control plane that serves every tenant's
`blobgw.<tenant>.*` RPCs while keeping tenants isolated at the **NATS account
boundary** — not merely by blobgw's app-level subject validation.

This is the converged-control-plane counterpart to mlfs's account-per-domain
coordination (`docs/multi-tenant-coordination.md`). mlfs is simpler: each mlfs
node lives *only* in its own tenant account and never needs to reach a shared
service, so plain per-account credentials suffice. blobgw is different: it is a
**shared** server that must answer *every* tenant — so tenant nodes in account A
must be able to reach the blobgw service, but **only** for `blobgw.A.>`. That
cross-account reach is a NATS **service export/import**, and the import subject is
the isolation boundary.

## The model

| Account | Role | Export / Import |
|---|---|---|
| `BLOBGW_SVC` (shared) | runs the `controlplane.Server` (queue group `blobgw`) | **exports** the service `blobgw.*.>` |
| `tenant-acme` | acme's mlfs nodes (reuse coord's per-tenant account) | **imports** the service remapped to `blobgw.acme.>` ONLY |
| `tenant-globex` | globex's mlfs nodes | **imports** `blobgw.globex.>` ONLY |

A node in `tenant-acme` publishing `blobgw.acme.index.lookup` is routed across the
account boundary to the `BLOBGW_SVC` responder and the reply routes back. The
same node publishing `blobgw.globex.index.lookup` has **no matching import**, so
NATS never routes it to the service → the client gets `no responders`. Tenant A
cannot reach tenant B's subjects even if it forges the subject. The account
boundary — the export/import mapping — is the enforcement, on top of blobgw's
existing `TenantFromSubject` / `ValidateTenant` app-level checks.

blobgw's own subscription is unchanged: it still `QueueSubscribe`s the
`blobgw.*.>` wildcards (one per op) in whatever account its connection lives in.
The only blobgw-side change is the **connection's account**, selected by
`cmd/blobgw -nats-creds` (operator/JWT mode) or `-nats-nkey` (signed nkey). Both
empty = the legacy single-account model (back-compat).

## What is enforced by code+test vs. deployment config

- **Code + hermetic test (this repo):** the wildcard subscribe + tenant routing,
  the connection account-selection flags, and the SAME export/import isolation
  semantics — proven by `controlplane.TestServiceAccountIsolation` using an
  embedded nats-server with in-server `accounts{}` (A reaches A; A is blocked
  from B with `no responders` at the account layer; back-compat single-account
  still works via `TestSingleAccountBackCompat`).
- **Deployment config (operators apply):** the actual production account/JWT
  provisioning (the `accounts{}` block below, or its `nsc` operator-mode
  equivalent) and the per-tenant credentials handed to blobgw and to each tenant's
  nodes. Account JWT issuance/rotation is a deployment concern; this repo does not
  provision it.

## In-server config (static / simple deployments)

Define the accounts directly in `nats.conf`. This is the exact shape the
hermetic test exercises.

```hcl
# nats.conf — converged blobgw control plane with account-per-domain isolation
host: "0.0.0.0"
port: 4222

accounts {
  # Shared service account: the blobgw control-plane replicas connect here and
  # export the control-plane service across all tenant subjects.
  BLOBGW_SVC {
    users: [ { user: "blobgw", password: "$BLOBGW_PASS" } ]   # or nkey/JWT
    exports: [
      { service: "blobgw.*.>" }
    ]
  }

  # One account per dedup domain / tenant. Each imports the blobgw service
  # remapped to its OWN prefix only — this single line is the isolation boundary.
  tenant-acme {
    users: [ { user: "acme", password: "$ACME_PASS" } ]
    imports: [
      { service: { account: "BLOBGW_SVC", subject: "blobgw.acme.>" } }
    ]
  }
  tenant-globex {
    users: [ { user: "globex", password: "$GLOBEX_PASS" } ]
    imports: [
      { service: { account: "BLOBGW_SVC", subject: "blobgw.globex.>" } }
    ]
  }
}
```

blobgw connects with the `BLOBGW_SVC` credentials:

```sh
blobgw -control-plane \
  -nats-url="nats://nats.dc1:4222" \
  -nats-creds=/etc/blobgw/blobgw-svc.creds \   # selects BLOBGW_SVC (ADR §7.7)
  -tenant-config=/etc/blobgw/tenants.json
```

Each tenant's mlfs nodes already connect with their per-tenant account creds for
coordination (`mlfs -nats-creds=/etc/mlfs/tenant-acme.creds`, see
`docs/multi-tenant-coordination.md`). Add the blobgw **import** to that same
account and the node's existing connection reaches blobgw — no node-side code or
flag change is needed; the M7 remote-dedup path rides coord's per-tenant
connection.

## Operator-mode (JWT / `nsc`) equivalent

For production, prefer operator mode so each account is an issued JWT with
rotatable user creds. The export/import is the same; expressed with `nsc`:

```sh
# Service account exports the control-plane service.
nsc add account BLOBGW_SVC
nsc add export --account BLOBGW_SVC --service --subject "blobgw.*.>" --name blobgw

# Each tenant account imports it, restricted to its own prefix.
nsc add account tenant-acme
nsc add import --account tenant-acme --service \
  --src-account BLOBGW_SVC --remote-subject "blobgw.acme.>" --local-subject "blobgw.acme.>"
```

Generate `blobgw-svc.creds` for the service account and per-tenant `.creds` for
each tenant; hand `blobgw-svc.creds` to blobgw via `-nats-creds` and each
tenant's `.creds` to that tenant's mlfs nodes. The runtime routing/isolation is
identical to the in-server config above (and to what the hermetic test proves);
only the credential issuance/rotation differs.

## Notes

- **Queue group across replicas is preserved.** All blobgw replicas connect to
  `BLOBGW_SVC` and `QueueSubscribe` under queue group `blobgw`; the service
  export is shared, so NATS still load-balances each tenant's request across
  replicas. Scale-out is unchanged: run more replicas in the service account.
- **Presign / record / lookup / gc.purge all ride the one export.** The export
  subject `blobgw.*.>` covers every control-plane op; the per-tenant import
  prefix (`blobgw.<tenant>.>`) covers all of that tenant's ops in one mapping.
- **Reply routing.** NATS service imports route the reply back to the requesting
  account automatically (the `_INBOX` reply subject is account-local); no extra
  export is required for replies.
- **Defense in depth.** The dedup index is also namespaced by `Domain`, and
  blobgw rejects a presign whose payload `Domain` ≠ subject tenant — so even
  within an account a confused client cannot cross tenants. The account boundary
  is the outer, server-enforced ring.
```

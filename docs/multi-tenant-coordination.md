# Multi-tenant mlfs: dedup domains + NATS account-per-domain

How to run many tenants (dedup domains) over shared infrastructure — one NATS
cluster, one Postgres/Yugabyte, one casstore backing — without their data or
coordination colliding.

## Isolation holds at three layers

| Layer | Boundary | Mechanism |
|---|---|---|
| **Data** (casstore) | dedup domain | blob IDs are domain-prefixed (`<domain>/chunks/<sha>`); content never dedups or collides across domains. Set with the daemon's `-domain`. |
| **Metadata SoR** (Postgres) | per-domain database/schema | the authoritative fence + the inode/scope namespace. Run a separate DB (or schema) per domain so `scope_key=ino:<n>` is unique per tenant and the fence is per-tenant. |
| **Coordination** (NATS) | **per-domain NATS account** | each domain authenticates to its own account → its own JetStream (own KV buckets + streams). Identical bucket names (`mlfs_live`) and scope keys (`ino:<n>`) in different accounts are physically different objects. |

The natural unit is **one dedup domain = one NATS account = one Postgres DB**,
all sharing physical infrastructure. NATS being shared never risks *data*
collision (that's casstore's prefix); it would only risk *coordination*
collision/leak, which accounts prevent (proven by
`internal/coord` `TestAccountPerDomainIsolation`).

## Why accounts (not bucket-name prefixing)

NATS accounts are the **hard, server-enforced** multi-tenancy boundary: account
A cannot see account B's subjects or JetStream assets, full stop. They also give
**per-account JetStream limits** (max memory/file/streams/consumers) →
noisy-neighbor protection. Because buckets are scoped by account, mlfs's bucket
names stay constant — no per-domain naming gymnastics in the code. (Prefixing
bucket names within a single shared account is only "soft" isolation by
convention; acceptable for a trusted single-instance enterprise deployment, not
for untrusted SaaS.)

## Daemon flags

```sh
mlfs \
  -domain=tenant-acme \                         # casstore data isolation
  -meta-dsn="postgres://…/mlfs_tenant_acme" \   # per-domain SoR (separate DB/schema)
  -node-id="$HOSTNAME" \
  -nats-url="nats://nats.dc1:4222" \            # shared external NATS backbone
  -nats-creds=/etc/mlfs/tenant-acme.creds \     # selects this domain's NATS account
  -nats-replicas=3                              # in-region JetStream quorum
```

`-nats-creds` is the only coordination change needed for multi-tenancy: the
credentials select the account, and isolation follows. By convention keep
`-domain` ↔ account ↔ Postgres DB one-to-one.

## Sample NATS server config (accounts + JetStream limits)

For a simple/static setup, define accounts in the server config. For production,
prefer **operator mode** (`nsc`) so each domain gets an issued account JWT +
user creds file, rotatable without server restarts.

```hcl
# nats.conf — one JetStream-enabled account per dedup domain
host: "0.0.0.0"
port: 4222
jetstream { store_dir: "/data/jetstream", max_file: 500GB }

accounts {
  tenant-acme {
    jetstream { max_mem: 256MB, max_file: 50GB, max_streams: 64 }
    users: [ { user: "acme", password: "$ACME_PASS" } ]   # or nkey/JWT
  }
  tenant-globex {
    jetstream { max_mem: 256MB, max_file: 50GB, max_streams: 64 }
    users: [ { user: "globex", password: "$GLOBEX_PASS" } ]
  }
}
```

Each mlfs node for `tenant-acme` connects with the acme creds; its
`mlfs_live` / `mlfs_nodes` / `mlfs_leader` buckets and `mlfs.changes` stream live
in the acme account, isolated from globex.

## How it composes with geo-distribution

Stack the two: **region → one 3-node JetStream cluster (in-region RAFT quorum) →
per-domain accounts → per-domain buckets.** Tenants are pinned to a home region
(data residency); within the region they share the cluster via isolated
accounts. When a single cluster's RAFT/meta-group is saturated (many tenants ×
frequent refreshes), shard tenants across multiple clusters federated by NATS
gateways — accounts make that shard boundary clean to move. PostgreSQL/Yugabyte
remains the authoritative cross-region fence; NATS stays fast because it stays
regional.

## Capacity notes

- All accounts on a cluster share the same JetStream RAFT meta-group, so account
  count × stream count × refresh rate drives meta-group load. Set per-account
  limits and shard to more clusters before the meta-group saturates.
- The embedded-NATS mode (`-nats-cluster`/`-nats-routes`, no `-nats-url`) is
  single-domain (dev / self-clustered single tenant). Multi-tenant shared
  backbone is the external mode (`-nats-url` + `-nats-creds`).

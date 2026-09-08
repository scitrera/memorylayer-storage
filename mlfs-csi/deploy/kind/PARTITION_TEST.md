# L2.8 network-partition self-fence — kind + tc procedure

Demonstrates the L2.8 acceptance "partition a node via `tc` → it self-fences;
the survivor takes over" against two real mlfs daemons in a kind cluster. Run
after `kind create cluster --config kind-config.yaml` and loading the `mlfs:dev`
image (`kind load docker-image mlfs:dev --name mlfs`).

```sh
K="kubectl --context kind-mlfs"
$K apply -f mlfs-cluster.yaml          # PG + external NATS + 2 mlfs daemons (lease-ttl=6s, refresh=2s)
$K wait --for=condition=Ready pod/mlfs-0 pod/mlfs-1 --timeout=120s

# 1. nodeA (mlfs-0) acquires a scope by writing.
$K exec mlfs-0 -- sh -c 'mkdir -p /mnt/mlfs/s1 && echo hi > /mnt/mlfs/s1/f'
$K exec mlfs-pg -- psql -U postgres -d mlfs -tAc \
  "select scope_key,owner_node,generation from ownership where scope_key like 'ino:%';"
#   ino:<s1>|mlfs-0|1

# 2. Partition mlfs-0 with real tc (ephemeral netadmin container shares its netns).
$K debug mlfs-0 --image=nicolaka/netshoot --profile=netadmin -q -- \
  tc qdisc add dev eth0 root netem loss 100%

# 3. After the lease TTL (3x refresh), the survivor steals the scope.
sleep 14
$K exec mlfs-1 -- sh -c 'echo from-node1 > /mnt/mlfs/s1/f'   # succeeds
$K exec mlfs-pg -- psql -U postgres -d mlfs -tAc \
  "select scope_key,owner_node,generation from ownership where scope_key like 'ino:%';"
#   ino:<s1>|mlfs-1|2     <- ownership moved, generation bumped

# 4. The partitioned node cannot write (no split-brain).
$K exec mlfs-0 -- sh -c 'timeout 8 sh -c "echo evil > /mnt/mlfs/s1/f"; echo rc=$?'   # blocks → killed

# 5. Heal; the revived node is fenced (it does NOT reclaim by force).
$K debug mlfs-0 --image=nicolaka/netshoot --profile=netadmin -q -- tc qdisc del dev eth0 root
$K exec mlfs-0 -- sh -c 'echo evil2 > /mnt/mlfs/s1/f'   # -> Input/output error (fenced)
# ownership stays mlfs-1|2
```

## Observed result (2026-06-17, kind v0.31.0, k8s v1.35.0)

| Step | Expected | Observed |
|---|---|---|
| 1. acquire | mlfs-0 owns scope @ gen 1 | `ino:…|mlfs-0|1` ✓ |
| 2. partition | tc applied | qdisc added ✓ |
| 3. survivor steals | mlfs-1 owns @ gen 2 | `ino:…|mlfs-1|2` ✓ |
| 4. partitioned write | does not succeed | blocked → terminated ✓ |
| 5. post-heal write | fenced | `Input/output error` (EIO), ownership unchanged ✓ |

Self-fence held within `lease-ttl` (6s = 3×refresh). PostgreSQL remained the
authoritative fence throughout; NATS only governed liveness/discovery.

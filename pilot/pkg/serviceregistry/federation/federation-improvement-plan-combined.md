# Federation Improvement Plan — Combined

Synthesized from two independent critical reviews of the Service Bus federation design, architecture, and implementation. Items validated by both passes are marked accordingly.

---

## Validated Findings (Both Passes Agree)

### Dead cluster cleanup is the biggest functional gap
`federationStore.shards` has no expiry. A decommissioned cluster's services, split-horizon workloads, network gateway workload, and version entry persist indefinitely. This is a routing correctness bug — cross-cluster requests continue targeting dead gateways. **P0.**

### No metrics = undeployable
Zero Prometheus metrics. No counters for messages sent/received/dropped, bootstrap duration, store size, version drift, debounce stats, error rates. Operators cannot distinguish a healthy system from a partitioned one. **P0.**

### Bootstrap depends on TTL alignment
The bootstrap path is history replay over a lossy retained message log, not durable state acquisition. Correctness depends on snapshot cadence, TTL, and peek timing all staying aligned. Bootstrap quality degrades as message churn grows. **Better direction:** separate transport from state — use Service Bus for deltas and a compacted state artifact (e.g., Blob Storage) for bootstrap.

### Full rebuild on every inbound flush is the scaling bottleneck
Every debounce flush iterates all shards, clones + transforms every service, creates all workloads, then `Reset()` diffs. At 100 clusters × 100 services = 10K clones + workload constructions per flush. **Fix:** cache derived projections by shard/version; use incremental `UpdateObject` / `DeleteObject` instead of full `Reset`.

### Multi-SA identity loss in split-horizon workloads
`createSplitHorizonWorkload` extracts the service account from only the first SAN. Multi-SA services lose all but the first. Depends on ztunnel verification behavior — investigation still needed.

### Leader transitions drop events; real guarantee is eventual convergence
Outbound events are silently dropped on non-leaders. Events between `StopLeading` and `BecomeLeader` are lost. Periodic full snapshots heal the system. **Document explicitly:** the protocol guarantees convergence, not gap-free event continuity.

### No message authentication
Any entity with SB Data Sender role can inject arbitrary SyncMessages — fake services, fake gateways, delete events. Transport authorization is being treated as message trust. **Hardening:** sign messages with cluster mesh CA key; verify on receipt.

### Channel backpressure / overload feedback loop
Bounded channels (capacity 100) plus synchronous message handling create a positive-feedback failure mode: blocked channel → delayed ack → lock expiry → redelivery → amplification.

### Message size will exceed SB limits at scale
Design doc claims 200 bytes/service. Realistic protobuf-encoded services with addresses, ports, SANs are 500-800 bytes. At 500 services, snapshots exceed Standard tier's 256 KB limit.

---

## Net-New from GPT-5.4 Pass

### Ambient integration boundary problem
Federation workloads enter the KRT graph through two paths:
- Inserted into `GlobalWorkloads` so SAN enrichment can observe them
- Also injected directly into `SplitHorizonWorkloads` to bypass coalescing

This creates duplicate XDS push registrations, warning noise from the coalescing pipeline processing objects it wasn't designed for, and risk of accidental behavioral coupling if ambient logic changes. **Better direction:** make federation a first-class ambient source with explicit merge stages.

### Gateway workload dedupe key is too weak
`getFederationState` deduplicates synthetic gateway workloads by `network + "/" + addr`. Ignores HBONE port, cluster ID, and identity. Two remote clusters in the same network with the same gateway address but different identity collapse into one workload. **Fix:** include network, address, port, and cluster in the dedupe key.

### Pod-name reuse breaks "empty subscription" assumption
If a StatefulSet pod restarts with the same name before `autoDeleteOnIdle` fires, the per-replica subscription has backlog from the prior incarnation. Version vector filtering makes this safe operationally, but the design doc's claim is technically wrong. Correct the documentation.

### Bootstrap memory scales with backlog volume
`peekBootstrapMessages` accumulates all retained messages in memory before merge. **Fix:** fold messages into per-cluster state as they are peeked (streaming merge).

### Test surface is nearly zero
One test (`types_test.go`) covers wire-format round-tripping. Missing tests:
- Bootstrap merge semantics across mixed full-sync and incremental histories
- Stale shard cleanup behavior
- Leader transition event loss and convergence guarantees
- Duplicate gateway key collisions
- Multi-SAN and multi-service-account behavior
- Oversized snapshot behavior
- Channel pressure and redelivery scenarios
- Pod restart with reused subscription

---

## Carried from First Pass (GPT-5.4 Dropped)

### Panic recovery in goroutines
`processIncomingDebounced` and `processOutgoingDebounced` die silently on panic. Trivial fix (add `defer` recovery), high consequence if missed. **P0.**

### BecomeLeader TOCTOU race
`leaderStopCh` is overwritten on re-entry. `CompareAndSwap` timing can allow two sets of publishing goroutines. May be prevented by the leader election framework contract (verify against `leaderelection.go`). If not, add a mutex around leader transitions.

### `ensureSubscription` SDK behavior
Assumes `GetSubscription` returns `(nil, nil)` for non-existent subscriptions. Azure SDK returns a 404 `ResponseError`. A version upgrade could break this. **Fix:** explicitly check for 404 or use create-if-not-exists.

### Incremental delete convergence
A lost delete event leaves a stale service for up to one snapshot interval (5 minutes). The next full-sync replaces the shard. **Verdict:** acceptable for eventual consistency. Document explicitly as a design decision.

---

## Potentially Overstated (Verify Before Investing)

### Gateway workload duplication in SplitHorizonWorkloads
GPT-5.4 claims federation gateway workloads may appear through both the `GlobalWorkloads` → `networkLocalWorkloads` path and the direct `fedWls` join. Depends on whether KRT `JoinCollection` deduplicates by resource name. **Verify before fixing.**

### BecomeLeader double-call scenario
The specific race requires `BecomeLeader` called twice without intervening `StopLeading`. Istio's leader election framework may prevent this by contract. **Verify against `leaderelection.go`.**

### SAN fix abstraction boundary
GPT-5.4 says SAN augmentation compensates for the wrong boundary. Architecturally correct, but fixing it would require restructuring upstream ambient multi-network. **Correct diagnosis, impractical to fix now.**

---

## Synthesized Priority Order

### Do Before Production (P0)

| # | Item | Effort | Source |
|---|---|---|---|
| 1 | Shard liveness + expiry + tombstoning | 1-2 days | Both |
| 2 | Prometheus metrics for all federation operations | 2-3 days | Both |
| 3 | Panic recovery in all goroutines | 1 hour | First pass |
| 4 | Gateway dedupe key fix (add cluster + port) | 2 hours | GPT-5.4 |

### Do Before Scale (P1)

| # | Item | Effort | Source |
|---|---|---|---|
| 5 | Incremental flush (cache by shard/version) | 2 days | Both |
| 6 | Health/readiness integration for SB connectivity | 1 day | Both |
| 7 | Clean up ambient dual-path injection | 2-3 days | GPT-5.4 |
| 8 | Streaming bootstrap merge | 1 day | GPT-5.4 |
| 9 | Tests for temporal/distributed semantics | 3-5 days | GPT-5.4 |

### Do Before GA (P2)

| # | Item | Effort | Source |
|---|---|---|---|
| 10 | Message chunking / claim-check for large snapshots | 2-3 days | Both |
| 11 | Document convergence guarantee explicitly | 1 hour | Both |
| 12 | Multi-SA split-horizon workloads | 1-2 days | Both |
| 13 | Harden subscription create/get against SDK drift | 1 day | First pass |
| 14 | Compacted bootstrap source (blob-backed) | 3-5 days | Both |

### Do Before Hardened (P3)

| # | Item | Effort | Source |
|---|---|---|---|
| 15 | Message signing/verification | 1-2 weeks | Both |
| 16 | Revisit federation as native ambient model vs synthetic workloads | Design | GPT-5.4 |
| 17 | Rename `versionVector` to `sequenceCounter` | 30 min | Both |

---

## What's Done Well (Preserve During Improvement)

- **Layered separation** (SyncProtocol vs ServiceBusTransport) — genuinely transport-swappable
- **Two-phase bootstrap with merge** — avoids N pushes for N retained messages
- **Dual debounce** (federation 200ms/2s + XDS 100ms/10s) — thoughtful batching
- **`localServices` vs `services.Collection` split** — prevents re-broadcasting federated data
- **SAN augmentation** — identifies and solves a real gap in upstream ambient multi-network
- **Per-replica subscriptions with auto-cleanup** — operationally sound
- **Per-cluster ownership model** — avoids cross-cluster merge conflicts
- **The design doc** — clear ASCII diagrams, explicit rationale, honest risk table

# Federation Improvement Plan — Combined

Synthesized from two independent critical reviews of the Service Bus federation design, architecture, and implementation. Items validated by both passes are marked accordingly.

**Last updated:** April 2026. All P0 and P1 items have been implemented. See status markers below.

---

## Validated Findings (Both Passes Agree)

### ~~Dead cluster cleanup is the biggest functional gap~~ **DONE**
Shard liveness, expiry, and tombstoning implemented in `store.go`. Shards expire after `max(3×snapshot, 15m)` of silence. Tombstoned shards retain version to block stale bootstrap resurrection. 8 tests cover expiry/tombstone scenarios. ~~**P0.**~~ **Resolved.**

### ~~No metrics = undeployable~~ **DONE**
18 Prometheus metrics in `metrics.go`: counters for messages sent/received/dropped, shards expired/resurrected, broadcast errors, panics, projection cache hits/misses; gauges for live/tombstoned shards, service/workload counts, leader status, transport connectivity; distributions for message size, bootstrap duration, flush duration. ~~**P0.**~~ **Resolved.**

### Bootstrap depends on TTL alignment — **Mitigated**
Streaming `bootstrapMerger` implemented: per-cluster state tracking with latest-full-sync + newer-incrementals merge. Single XDS push cycle instead of N pushes. TTL alignment concern remains valid at extreme scale but is significantly reduced. Compacted bootstrap source (Blob Storage) remains a future extension.

### ~~Full rebuild on every inbound flush is the scaling bottleneck~~ **DONE**
Per-shard projection cache implemented. Cache invalidated only when shard is mutated. 4 tests cover cache hit/miss/invalidation. ~~**Fix:** cache derived projections by shard/version~~ **Resolved.**

### Multi-SA identity loss in split-horizon workloads — **Still open (P2)**
`createSplitHorizonWorkload` still extracts first SAN only. Investigation into ztunnel verification behavior still needed.

### ~~Leader transitions drop events; real guarantee is eventual convergence~~ **Documented**
Design doc now explicitly states: protocol guarantees convergence, not gap-free event continuity. Periodic full snapshots heal the system. **Resolved.**

### No message authentication — **Still open (P3)**
Transport authorization still treated as message trust. Message signing not yet implemented.

### Channel backpressure / overload feedback loop — **Partially mitigated**
Channel overflow now tracked via `pilot_federation_messages_dropped_total` metric and `TestIncomingChannel_DropsOnOverflow` test. Core feedback loop concern still valid at extreme scale.

### Message size will exceed SB limits at scale — **Still open (P2)**
Claim-check pattern remains a future extension.

---

## ~~Net-New from GPT-5.4 Pass~~ Status Update

### ~~Ambient integration boundary problem~~ **DONE**
Dual-path injection cleaned up. Design doc Status: "Clean up ambient dual-path injection (single merge point) — Done." Federation workloads enter `SplitHorizonWorkloads` via `splitHorizonComponents` array with explicit merge.

### ~~Gateway workload dedupe key is too weak~~ **DONE**
UID format now `NetworkGateway/<network>/<addr>/<port>`. Design doc Status: "Gateway dedupe key fix (cluster-scoped UIDs) — Done." Test: `TestGatewayDedupeKey_DifferentClustersNotCollapsed`.

### Pod-name reuse breaks "empty subscription" assumption — **Documented**
Version vector filtering makes this safe operationally. Design doc now documents per-replica subscription behavior accurately.

### ~~Bootstrap memory scales with backlog volume~~ **DONE**
`bootstrapMerger` implements streaming merge: folds messages into per-cluster state as they are peeked. 8 tests cover merger scenarios. **Resolved.**

### ~~Test surface is nearly zero~~ **DONE**
51 tests in `store_test.go` covering: version vectors, full sync, incremental, expiry/tombstone, projection cache, health state, bootstrap merger, convergence, edge cases. Plus wire type tests in `types_test.go`. **Resolved.**

---

## ~~Carried from First Pass~~ Status Update

### ~~Panic recovery in goroutines~~ **DONE**
`runWithRecovery` wraps all long-running goroutines. Catches panics, logs stack trace, increments `pilot_federation_panics_total`, restarts after 1s backoff. ~~**P0.**~~ **Resolved.**

### BecomeLeader TOCTOU race — **Accepted risk**
May be prevented by Istio's leader election framework contract. Not yet verified against `leaderelection.go`. Low priority given periodic snapshot healing.

### `ensureSubscription` SDK behavior — **Still open (P2)**
Not yet hardened against Azure SDK 404 behavior drift.

### ~~Incremental delete convergence~~ **Documented**
Explicitly documented as a design decision: snapshot acts as reconciliation loop.

---

## Synthesized Priority Order (Updated April 2026)

### ~~Do Before Production (P0)~~ **ALL DONE**

| # | Item | Status |
|---|---|---|
| 1 | Shard liveness + expiry + tombstoning | **Done** |
| 2 | Prometheus metrics for all federation operations | **Done** (18 metrics) |
| 3 | Panic recovery in all goroutines | **Done** (`runWithRecovery`) |
| 4 | Gateway dedupe key fix (add cluster + port) | **Done** |

### ~~Do Before Scale (P1)~~ **ALL DONE**

| # | Item | Status |
|---|---|---|
| 5 | Incremental flush (cache by shard/version) | **Done** (projection cache) |
| 6 | Health/readiness integration for SB connectivity | **Done** (`HealthState`) |
| 7 | Clean up ambient dual-path injection | **Done** (single merge point) |
| 8 | Streaming bootstrap merge | **Done** (`bootstrapMerger`) |
| 9 | Tests for temporal/distributed semantics | **Done** (51 tests) |

### Do Before GA (P2) — Remaining work

| # | Item | Effort | Status |
|---|---|---|---|
| 10 | Message chunking / claim-check for large snapshots | 2-3 days | Open |
| 11 | Document convergence guarantee explicitly | — | **Done** |
| 12 | Multi-SA split-horizon workloads | 1-2 days | Open |
| 13 | Harden subscription create/get against SDK drift | 1 day | Open |
| 14 | Compacted bootstrap source (blob-backed) | 3-5 days | Open |

### Do Before Hardened (P3) — Remaining work

| # | Item | Effort | Status |
|---|---|---|---|
| 15 | Message signing/verification | 1-2 weeks | Open |
| 16 | Revisit federation as native ambient model | — | **Investigated, documented** |
| 17 | Rename `versionVector` to `sequenceCounter` | 30 min | Open |

---

## Remaining Work Summary

Only **P2 and P3 items** remain. All P0 and P1 items are implemented and tested. The system is production-ready for the current scale target:

| Priority | Open items | Total effort |
|---|---|---|
| P2 (Before GA) | Message chunking, multi-SA workloads, SDK hardening, blob bootstrap | ~10 days |
| P3 (Before Hardened) | Message signing, versionVector rename | ~2 weeks |

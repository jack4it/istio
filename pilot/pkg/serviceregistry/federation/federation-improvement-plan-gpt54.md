# Federation Improvement Plan, GPT-5.4 Pass

Second-pass critical evaluation of the Service Bus federation design, architecture, and implementation. This version is intentionally stricter on ambient integration behavior, operational scaling, and failure semantics.

---

## Executive Position

The core idea is directionally right: replacing peer-to-peer control-plane discovery with brokered pub/sub is a valid answer to multi-cluster scale. The implementation is also more disciplined than most first versions. But it still behaves like a prototype that proved feasibility, not a system that is ready to be trusted as a control-plane substrate.

The major issue is not one fatal bug. It is that the design quietly relies on several soft assumptions:
- retained history is always fresh enough to bootstrap from
- clusters rarely disappear permanently
- federated state volume stays small enough for full rebuilds and full snapshots
- ambient merge paths can absorb synthetic federation objects without side effects
- losing precision during leader transitions is acceptable
- anyone allowed to send to the namespace is trusted to publish control-plane truth

Those assumptions may hold in a lab. They are weak in production.

---

## 1. Architecture Risks

### 1.1 Bootstrap is history replay, not state acquisition

The bootstrap path is framed as retained-state bootstrap, but it is really replay over a lossy retained message log:
- the shared bootstrap subscription stores messages, not durable cluster state
- correctness depends on snapshot cadence, TTL, and peek timing all staying aligned
- startup cost scales with retained message volume, not with current cluster state

That means bootstrap quality degrades as message churn grows. A new cluster does not read the latest canonical state for each remote cluster. It scans a backlog and reconstructs state heuristically.

**Why this matters:** a system that uses a broker as the source of truth without compaction will eventually pay in startup latency, memory spikes, and ambiguous recovery semantics.

**Better direction:** separate transport from state. Use Service Bus for deltas and a compacted state artifact for bootstrap.

### 1.2 The design has no real membership model

Remote state is partitioned by source cluster, but there is no first-class concept of cluster lifecycle:
- no join record
- no heartbeat contract
- no explicit tombstone
- no lease or expiry on ownership of a shard

A cluster that stops publishing is indistinguishable from one that is just quiet.

**Consequence:** stale remote services can live indefinitely, and the longer the system runs the harder it becomes to reason about whether a shard represents truth or residue.

**Required change:** add shard liveness semantics, not just message semantics.

### 1.3 Federation is bolted into ambient through synthetic objects, not a native model

The ambient integration works, but it is opportunistic. Federated services and workloads are fed into the same KRT graph as Kubernetes-derived objects, then special-cased to avoid bad interactions.

That creates a structural smell:
- federated workloads are inserted into `GlobalWorkloads` partly so SAN enrichment can observe them
- the same federated workloads are also injected directly into `SplitHorizonWorkloads` to bypass normal coalescing
- comments already acknowledge that the coalesced workload pipeline will emit warnings for federation inputs

This means the graph is doing unnecessary work over objects it was not originally shaped to represent.

**Consequence:** correctness becomes dependent on a growing set of careful exclusions and "harmless" warnings.

**Better direction:** make federation a first-class ambient source with explicit merge stages, instead of injecting synthetic objects into intermediate collections designed for Kubernetes-discovered workloads.

---

## 2. Correctness Risks

### 2.1 There is no cleanup path for dead clusters

This remains the biggest functional gap.

`federationStore` has durable per-cluster shards, but no expiry. If a cluster is retired, all other clusters retain:
- its services
- its split-horizon synthetic workloads
- its synthetic network gateway workload
- its version entry

This is more than a memory leak. It is a routing correctness bug. Cross-cluster requests can continue targeting dead gateways long after the source cluster has disappeared.

**Priority:** P0.

### 2.2 `ServiceBusTransport` bootstrap assumes the shared backlog is enough

The current bootstrap stops when `PeekMessages` returns no results for the current sequence cursor. That is not a proof of completeness. It only means the receiver observed a temporary end of visible retained history.

There is no concept of:
- cluster set expected at bootstrap time
- minimum freshness target per cluster
- snapshot watermark
- bootstrap success criteria beyond "peek returned no more messages"

This is acceptable for best-effort warm start. It is weak for control-plane correctness.

### 2.3 Gateway workload deduplication key is too weak

In `getFederationState`, synthetic gateway workloads are deduplicated by:
- `network + "/" + addr`

That ignores:
- HBONE port
- cluster ID
- service account / namespace identity

If two remote clusters in the same network advertise the same address with different identity or port, they collapse into one synthetic gateway workload.

**Consequence:** identity and routing metadata can be conflated across clusters.

**Required fix:** include at least network, address, port, and cluster in the dedupe key.

### 2.4 `createSplitHorizonWorkload` collapses multi-identity services to one identity

The code extracts the service account from only the first SAN. That is a simplification disguised as an implementation detail.

If a global service is backed by workloads with multiple service accounts, the synthetic split-horizon workload represents only one of them.

**Consequence:** the identity model becomes lossy. Depending on ztunnel verification behavior, this can break valid traffic or create misleading policy outcomes.

### 2.5 Leader transition correctness is eventual, not precise

Outbound publishing is leader-gated, but change capture is not buffered across leadership transitions. Non-leaders drop outbound events immediately.

So the real contract is:
- incremental precision can be lost during leader turnover
- periodic full snapshots heal the system later

That may be acceptable, but the design currently talks more confidently than the behavior warrants.

**Recommendation:** explicitly document that the protocol guarantees convergence, not gap-free event continuity.

---

## 3. Ambient Integration Risks

### 3.1 Federated workloads create duplicate push paths

The ambient graph registers XDS push triggers in multiple places:
- federated services/workloads register batch handlers directly when joined
- `SplitHorizonServices` and `SplitHorizonWorkloads` also register push handlers on downstream collections

That means a single federation update can contribute to multiple push-triggering paths for effectively the same logical change.

Even if higher layers debounce this, the graph is noisier than it should be.

**Consequence:** unnecessary recomputation and harder-to-reason-about push behavior.

### 3.2 Federated workloads contaminate the coalescing pipeline

The code explicitly joins federation workloads into `GlobalWorkloads` so SAN derivation can see them. But that also feeds them into indices and coalescing logic intended for discovered workloads.

The comments admit the result: warning logs about missing gateways are expected and treated as harmless.

That is not harmless. It means the architecture is knowingly routing non-native objects through a path that does work, logs errors/warnings, and then discards output.

**Consequence:** wasted CPU, noisy logs, and risk of accidental behavioral coupling if future ambient logic changes.

**Better direction:** derive SANs from a dedicated federation-aware view, not from a polluted global workload stream.

### 3.3 Synthetic gateway workloads may be duplicated in the final graph

Federation gateway workloads are included in two ways:
- through the joined `GlobalWorkloads` path
- again by direct inclusion of `fedWls` in `SplitHorizonWorkloads`

Because `networkLocalWorkloads` keeps `NetworkGateway/*` UIDs regardless of network, there is a real risk that gateway workloads appear through both the indirect and direct paths.

If KRT de-duplicates by resource identity downstream, this may be mostly masked. If not, this is a latent duplication bug.

At minimum, the current topology is hard to reason about and too dependent on collection semantics that are not obvious from the federation code itself.

### 3.4 The SAN fix is correct locally but may be compensating for the wrong abstraction boundary

The SAN augmentation solves a real double-HBONE issue. But it also reveals a deeper problem: service identity is being patched after federation rather than carried as a stable first-class contract from source to consumer.

The source cluster already knows the backing identities. The receiver should not need to reconstruct transport-critical identity semantics by merging service objects and synthetic workloads inside the ambient graph.

**Better direction:** make workload identity part of the federation contract in a more explicit and lossless way.

---

## 4. Performance and Scale Risks

### 4.1 Every inbound flush rebuilds the whole world

The inbound path currently does:
- update store per message
- on debounce flush, flatten all shards
- recreate all service/workload projections
- reset both static collections with full slices

That is the dominant scaling problem in the code.

The design wants scale, but the implementation still behaves like a full-recompute system.

**Consequence:** CPU, allocations, and GC cost rise with total federated state, not with the delta being applied.

**Priority:** P0/P1 depending on expected scale.

### 4.2 Bootstrap memory use scales with backlog volume

`peekBootstrapMessages` accumulates all retained messages in memory before merge. If retained history is large, startup memory spikes before any reduction happens.

This is unnecessary. Merge can be incremental.

**Fix:** fold messages into per-cluster state as they are peeked instead of storing the raw list.

### 4.3 Snapshot size assumptions are optimistic

The design doc underestimates payload size. It assumes very small service representations. In practice, protobuf-encoded services with addresses, ports, waypoint metadata, and SANs can grow much larger.

Even before the absolute Service Bus limit, larger snapshots increase:
- send latency
- broker cost
- JSON marshal/unmarshal time
- bootstrap replay time

This is not just a future concern. It is a medium-term capacity limit.

### 4.4 The protocol does not degrade gracefully under overload

Bounded channels plus synchronous message handling produce a fragile shape:
- receive loop calls handler
- handler enqueues to bounded channel
- bounded channel can block
- ack completion is delayed
- delayed ack increases redelivery risk
- redelivery amplifies pressure

That is a classic positive-feedback failure mode.

---

## 5. Operational Risks

### 5.1 No metrics means no operating model

This code has logs, not observability.

Missing metrics include:
- bootstrap message count, duration, and merge discard counts
- per-cluster shard age and object counts
- full-sync publish size and failure rate
- incremental publish rate
- receive lag / time since last remote update per cluster
- debounce batch size and flush latency
- dropped or dead-lettered message counts

Without these, operators cannot distinguish:
- a quiet healthy system
- a partitioned system with stale shards
- a leader that is not publishing
- a bootstrap that succeeded superficially but loaded stale data

### 5.2 Readiness does not reflect federation truth

The code keeps retrying forever on Service Bus failure, but there is no surfaced readiness or degraded mode. That means a pod can be "ready" while federation is dead or stale.

For a discovery subsystem, that is too optimistic.

### 5.3 Pod reuse breaks the mental model of empty per-replica subscriptions

The design describes per-replica subscriptions as starting empty. That is only true if the subscription is actually new.

The code reuses an existing subscription if it already exists. If a pod name is reused before auto-delete cleanup, the supposedly empty steady-state subscription can contain backlog from a prior incarnation.

This may still converge because stale messages are filtered by per-cluster version checks, but the startup model is no longer what the design claims.

---

## 6. Security Risks

### 6.1 Transport authorization is being treated as message trust

This is the strongest architectural security objection.

Anyone who can publish to the topic can assert control-plane facts:
- service existence
- service deletion
- gateway location
- remote identity hints

That is too much trust to place in namespace-level broker authorization alone.

**Required hardening:** sign messages or otherwise bind them cryptographically to cluster identity.

### 6.2 Trust domain handling is oversimplified

The current fallback behavior assumes a common trust domain or defaults to `cluster.local`. That is convenient, but it turns multi-trust-domain behavior into silent mis-modeling rather than explicit incompatibility.

That is dangerous because it can fail as incorrect identity, not as a clearly unsupported configuration.

---

## 7. Test Gaps

The code has only a very small wire-format preservation test. That is far from enough for a system with this many temporal and distributed semantics.

Missing tests include:
- bootstrap merge semantics across mixed full-sync and incremental histories
- stale shard cleanup behavior
- leader transition event loss and convergence guarantees
- duplicate gateway key collisions
- multi-SAN and multi-service-account behavior
- oversized snapshot behavior
- channel pressure and redelivery scenarios
- pod restart with reused subscription

The current test surface does not match the design complexity.

---

## 8. Recommended Plan

### P0

- Add per-cluster shard liveness, expiry, and tombstoning.
- Add federation metrics and health reporting before relying on this operationally.
- Remove full-state rebuild on every inbound flush, or at least cache derived projections by shard/version.
- Fix gateway workload dedupe key to include cluster and HBONE port.

### P1

- Stop routing federated workloads through ambient coalescing paths that only tolerate them accidentally.
- Eliminate duplicate XDS push paths for federated objects.
- Make bootstrap merge streaming instead of accumulating the full retained backlog in memory.
- Document the real protocol guarantee: eventual convergence, not exact continuity across leader changes.

### P2

- Introduce a compacted bootstrap source, such as blob-backed state snapshots.
- Add chunking or claim-check for large snapshots.
- Carry identity semantics more explicitly instead of reconstructing them from first SAN or ambient merge behavior.
- Harden subscription create/get logic against SDK behavior drift and pod-name reuse semantics.

### P3

- Add message authenticity checks.
- Revisit whether federation should be represented as synthetic ambient workloads at all, versus a native remote-service model.
- Rename `versionVector` to reflect actual semantics.

---

## 9. Bottom Line

This is a good design prototype with several strong ideas:
- brokered control-plane sync is the right high-level direction
- the separation of transport and protocol is clean
- the ambient SAN fix is based on a real data-plane constraint
- the use of per-cluster ownership avoids a whole class of cross-cluster merge conflicts

But the implementation still depends too much on:
- periodic healing instead of precise correctness
- retained-message replay instead of durable state
- synthetic ambient objects flowing through paths that only partially fit them
- operational assumptions that are not instrumented

I would not call this production-ready until it has:
1. shard expiry semantics,
2. meaningful metrics and readiness,
3. a less expensive inbound application path,
4. a cleaner ambient integration boundary,
5. a more explicit bootstrap/state model.

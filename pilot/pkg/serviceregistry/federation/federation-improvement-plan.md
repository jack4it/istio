# Federation Improvement Plan

Critical evaluation of the Service Bus federation design, architecture, and implementation with prioritized improvements.

---

## 1. Design-Level Concerns

### 1.1 Single Shared Bootstrap Subscription is Fragile

**Assumption:** One pre-provisioned bootstrap subscription with no SQL filter; all clusters peek non-destructively.

- TTL is 10 minutes. If no leader has published a snapshot recently (restart, idle), a new cluster bootstraps with partial or empty state. The 5m snapshot interval + 10m TTL means at most 1 retained snapshot per cluster at any time.
- Peek is not transactional — a new snapshot arriving mid-peek can cause partial visibility.
- No explicit "bootstrap complete with full state" signal. 5 seconds of silence from `PeekMessages` != "all state loaded."

**Improvement:** Write a compacted state blob (Blob Storage) after every snapshot. New clusters read the blob, then peek only for incrementals newer than the blob's version. Decouples bootstrap from TTL timing.

### 1.2 No Cluster Membership / Deregistration

No mechanism to remove a cluster from the federation store. If cluster-3 is decommissioned:
- Its shard stays in `federationStore.shards` forever
- Its services, split-horizon workloads, and network gateway workload persist
- Version vector entry persists

**Impact:** Ghost services and workloads accumulate. Stale gateway routing. Slow memory leak.

**Improvement:** Heartbeat/liveness mechanism. If no message from cluster X in N snapshot intervals, tombstone the shard. Or require explicit deregistration messages.

### 1.3 Incremental Deletes Are Best-Effort

A delete is published once as `DeletedHostnames`. If lost, remote clusters keep the service until the next full-sync snapshot replaces the shard (up to 5 minutes).

**Verdict:** Acceptable for eventual consistency — the snapshot acts as a reconciliation loop. Document explicitly as a design decision.

### 1.4 Message Size Limits Will Hit at Scale

Design doc claims 200 KB for 1,000 services. Realistic `WireServiceInfo` with ports, addresses, SANs is 500-800 bytes. At 500 services, already 250-400 KB — exceeding Standard tier's 256 KB limit.

**Improvement:** Implement claim-check pattern (mentioned in Future Extensions) as a near-term requirement for >200 global services. Alternatively, chunk full-sync messages.

---

## 2. Protocol-Level Concerns

### 2.1 Version Vector Is Not Actually a Vector Clock

Uses `max(counter+1, time.Now().UnixMilli())` — this is a Lamport timestamp, not a vector clock. Each cluster only increments its own entry; no causal ordering across participants.

**Verdict:** Fine for current "each cluster owns its own shard" model. Rename to `sequenceCounter` or `lamportClock` to avoid confusion.

### 2.2 Race Between `BecomeLeader` and `processOutgoingDebounced`

`BecomeLeader` overwrites `leaderStopCh` with a new channel. If called twice rapidly (election flap), the first set of goroutines now references the new channel. TOCTOU race between `StopLeading`'s `CompareAndSwap` and `BecomeLeader`'s `isLeader.Load()` guard.

**Improvement:** Use a mutex around leader transitions, or a `sync.Once`-per-term pattern where each leadership term gets its own context.

### 2.3 Events Dropped During Leader Transitions

`handleServiceUpsert` / `handleServiceDelete` silently return when `!sp.isLeader.Load()`. Events between `StopLeading` on old leader and `BecomeLeader` on new leader are lost on all replicas.

**Verdict:** Low severity — the new leader's first snapshot captures current state. But a create-then-delete within the gap loses the delete. Acknowledge as a known window.

### 2.4 No Backpressure on `incomingCh` / `outgoingCh`

Both channels have capacity 100. If the debounce loop can't keep up, the receive loop blocks, message locks expire, and redelivery creates cascading overload.

**Improvement:** Unbounded buffer (ring buffer or linked list), or explicit backpressure that pauses the receive loop.

---

## 3. Implementation-Level Concerns

### 3.1 `getFederationState` Rebuilds Everything Every Flush

Every debounce flush iterates all shards, clones + transforms every service, creates all workloads, then `Reset()` diffs. At 100 clusters × 100 services = 10K clones + workload constructions per flush.

**Improvement:** Track which shards changed and only rebuild those. Use `UpdateObject` / `DeleteObject` instead of full `Reset`.

### 3.2 `addLocalNetworkVIPs` Clones Every Service Every Flush

`protomarshal.Clone(svc.Service)` deep-copies every service from every shard on every debounce cycle.

**Improvement:** Cache localized services per shard. Only re-clone when the shard version changes.

### 3.3 No Panic Recovery in Goroutines

If `Reset` or any KRT operation panics, `processIncomingDebounced` dies silently. No recovery, no logging, no restart.

**Improvement:** Add `defer` recovery at the top of all long-running goroutines.

### 3.4 `createSplitHorizonWorkload` Uses First SAN Only

```go
if len(svc.SubjectAltNames) > 0 {
    if parts := parseSpiffeSAN(svc.SubjectAltNames[0]); parts != nil {
        serviceAccount = parts.serviceAccount
    }
}
```

Multi-SA services lose all but the first SA. Connections from workloads using other service accounts may fail mTLS.

**Improvement:** Create one split-horizon workload per unique service account, or use a union identity. Investigate ztunnel's verification behavior.

### 3.5 `ensureSubscription` Relies on Undocumented SDK Behavior

Assumes `GetSubscription` returns `(nil, nil)` for non-existent subscriptions. The Azure SDK actually returns a 404 `ResponseError`. A version upgrade could break this.

**Improvement:** Explicitly check for 404 status code, or use create-if-not-exists with idempotent retry.

---

## 4. Operational Concerns

### 4.1 No Metrics or Observability

Zero Prometheus metrics. No counters for messages sent/received/dropped, bootstrap duration, store size, version drift, debounce stats, error rates.

**Impact:** Production debugging is impossible.

### 4.2 No Health Check Integration

Service Bus connection drops result in infinite retry with 1s backoff. No health check endpoint, no circuit breaker, no alerting hook.

### 4.3 No Graceful Degradation on Service Bus Outage

- New clusters can't bootstrap
- Existing clusters keep last-known state forever (no staleness detection)
- Leader retries publishing silently

**Improvement:** Staleness timer per shard. If no update from cluster X in N minutes, remove its services from KRT.

---

## 5. Security Concerns

### 5.1 No Message Authentication

Any entity with SB Data Sender role can inject arbitrary `SyncMessage` structs — fake services, fake gateways, delete events.

**Improvement:** Sign messages with cluster mesh CA key. Verify on receipt. Significant effort but important.

### 5.2 Trust Domain Assumption

All clusters assumed to share the same trust domain. `pickTrustDomain` falls back to `cluster.local`. Multi-trust-domain setups produce wrong workload identities.

**Improvement:** Include source cluster's trust domain in `WireNetworkGateway`.

---

## 6. Prioritized Improvement Plan

| Priority | Item | Effort | Impact |
|---|---|---|---|
| **P0** | Add Prometheus metrics for all federation operations | 2-3 days | Production observability |
| **P0** | Add panic recovery to all goroutines | 1 hour | Prevents silent goroutine death |
| **P1** | Shard staleness detection and eviction | 1-2 days | Prevents permanent ghost services |
| **P1** | Fix leader transition race (mutex around BecomeLeader/StopLeading) | 1 day | Correctness under election flaps |
| **P1** | Cache localized services to avoid full rebuild per flush | 2 days | Performance at scale |
| **P2** | Message chunking or claim-check for large snapshots | 2-3 days | Required for >200 global services |
| **P2** | Health check integration for SB connectivity | 1 day | Operational visibility |
| **P2** | Multi-SA split-horizon workloads | 1-2 days | Correctness for multi-SA services |
| **P3** | Message signing/verification | 1-2 weeks | Security hardening |
| **P3** | Blob-based bootstrap (decouple from TTL) | 3-5 days | Reliable cold-start |
| **P3** | Rename "version vector" to "sequence counter" | 30 min | Code clarity |

---

## 7. What's Done Well

- **Layered separation** (SyncProtocol vs ServiceBusTransport) — genuinely transport-swappable
- **Two-phase bootstrap with merge** — smart cold-start, avoids N pushes for N retained messages
- **Dual debounce** (federation + XDS) — thoughtful batching
- **`localServices` vs `services.Collection` split** — prevents re-broadcasting federated data (critical correctness property)
- **SAN augmentation** — identifies and solves a real gap in upstream ambient multi-network
- **Per-replica subscriptions with auto-cleanup** — operationally sound
- **The design doc** — clear diagrams, explicit rationale, honest risk table

The core architecture is sound. Most issues above are production hardening rather than fundamental design flaws. The highest-risk gap is deploying without metrics and shard eviction.

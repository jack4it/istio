# Azure Service Bus Federation for Istio Ambient Multi-Cluster

## Motivation

Multi-cluster Istio ambient mode requires each cluster's Istiod to know about services in
every other cluster. At scale (100+ AKS clusters), direct peer-to-peer service discovery
becomes impractical:

- **O(n²) connections** — every Istiod must reach every other Istiod over a direct network
  path. At 100 clusters, that's 4,950 bidirectional connections requiring an overlay network.
- **No persistence** — if a new cluster joins, it must query all peers to build initial
  state, which is slow and unreliable with intermittent connectivity.

Azure Service Bus replaces direct peer-to-peer discovery with a managed pub/sub broker:

```
                    ┌──────────────────────┐
                    │   Azure Service Bus  │
                    │   Topic:             │
                    │   istio-service-sync │
                    │                      │
                    │  ┌─────────────────┐ │
                    │  │ Sub: cluster-1  │ │
                    │  │ Sub: cluster-2  │ │
                    │  │ Sub: cluster-3  │ │
                    │  │     ...         │ │
                    │  │ Sub: cluster-N  │ │
                    │  └─────────────────┘ │
                    └──────────┬───────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
    ┌─────▼──────┐       ┌─────▼──────┐       ┌─────▼──────┐
    │  Istiod    │       │  Istiod    │       │  Istiod    │
    │  cluster-1 │       │  cluster-2 │       │  cluster-3 │
    └────────────┘       └────────────┘       └────────────┘
```

Each Istiod:
- **Publishes** service snapshots and incremental updates to the topic
- **Receives** updates from all other clusters via its own exclusive subscription
- Uses a **per-cluster subscription** with a SQL filter (`ClusterID <> 'self'`) so it doesn't receive its own messages
- No direct network path between any two Istiods is required

---

## Architecture

### Layered Design

The federation system separates concerns into two layers:

- **`SyncProtocol`** — the federation orchestrator. Owns version vectors, tombstones, store, KRT collections, ambient index integration, snapshot scheduling, and debouncing. Transport-agnostic — it doesn't care how `SyncMessage` arrives.
- **`ServiceBusTransport`** — a pure send/receive pipe. Handles Azure Service Bus topic publishing, subscription receiving, and two-phase bootstrap drain. No protocol logic.

This separation means the protocol layer can be backed by any pub/sub broker (Kafka, NATS, RabbitMQ) by swapping only the transport implementation.

### Topics and Subscriptions

| Topic | Purpose | Message Type | Subscription Strategy |
|---|---|---|---|
| `istio-service-sync` | Service add/update/delete + gateway changes | `SyncMessage` (JSON envelope + protobuf services + gateway info) | Per-cluster subscription with SQL filter: `ClusterID <> '<self>'` |

Gateway info is included in every `SyncMessage` as the `NetworkGateway` field. A separate topic is unnecessary — gateway changes are rare and bundled with service syncs.

Messages are JSON-serialized `SyncMessage` structs (services as protobuf bytes in `WireServiceInfo`). Application properties (`ClusterID`, `FullSync`, `Version`) enable subscription-level filtering and bootstrap merge logic.

### Cold-Start Bootstrap (Two-Phase)

Cold-start bootstrap uses a **two-phase drain-and-merge** strategy with Service Bus message retention:

**Phase 1 — Drain:** When a new Istiod starts, the transport reads all retained messages from its subscription in batches using short timeouts (5 s). Messages are accumulated in memory without triggering XDS pushes or KRT collection updates.

**Phase 2 — Merge and Apply:** The retained messages are merged per source cluster:
- Only the **latest full-sync snapshot** per cluster is kept (highest version vector value)
- Stale full syncs and superseded incrementals are discarded
- Incrementals newer than the latest full sync are kept

The merged set is delivered to the message handler in order — producing a **single XDS push cycle** rather than N pushes for N retained messages.

```
Subscription backlog for new cluster-5:

  [cluster-1 full-sync v=10]      ← stale, discarded
  [cluster-2 incremental v=5]     ← stale (< full-sync v=8), discarded
  [cluster-1 full-sync v=15]      ← latest for cluster-1, kept
  [cluster-2 full-sync v=8]       ← latest for cluster-2, kept
  [cluster-2 incremental v=9]     ← newer than full-sync, kept
  [cluster-3 full-sync v=7]       ← latest for cluster-3, kept
  [cluster-1 incremental v=16]    ← newer than full-sync, kept

After merge (5 messages instead of 7):
  cluster-1: full-sync v=15, then incremental v=16
  cluster-2: full-sync v=8, then incremental v=9
  cluster-3: full-sync v=7
```

Snapshot publishing waits for bootstrap to complete before broadcasting, preventing incomplete state from propagating.

| Subscription Setting | Value | Rationale |
|---|---|---|
| `defaultMessageTimeToLive` | `PT10M` | 2x snapshot interval — guarantees at least one full sync per cluster retained |
| `maxDeliveryCount` | `10` | Generous retry before dead-lettering |
| `deadLetteringOnMessageExpiration` | `true` | Observability — don't silently drop |
| `lockDuration` | `PT1M` | Long enough for bootstrap batch processing |

### Debounced Message Processing

Both inbound and outbound paths use symmetric debounce loops (200ms quiet / 2s max) to batch operations:

```
Full data flow:

Inbound:  transport -> incomingCh -> processIncomingDebounced -> store -> KRT -> XDS
Outbound: ambient events -> outgoingCh -> processOutgoingDebounced -> broadcast -> transport
```

**Inbound:** Messages are applied to the store immediately (version vectors stay current), but the expensive KRT collection rebuild and XDS push are deferred until a quiet period. 10 messages in rapid succession = 10x store updates + 1x KRT Reset + 1x ConfigUpdate.

**Outbound:** Local service change events are deduplicated by hostname (last event wins), batched into a single `SyncMessage` with one version vector increment, and broadcast. At 100 clusters, a 20-service rollout produces 1 message with 20 services instead of 20 individual messages.

This provides **two layers of batching:**
1. **Federation debounce** (200ms/2s) — batches store to KRT to push within the federation sync protocol
2. **XDS debounce** (100ms/10s, built-in Istiod) — batches pushes from Istiod to ztunnel proxies

| Parameter | Value | Rationale |
|---|---|---|
| `debounceAfter` | 200 ms | Quiet period — slightly longer than Istiod's built-in 100ms |
| `debounceMax` | 2 s | Maximum wait — bounds worst-case latency during sustained updates |
| `channel capacity` | 100 | Absorbs bursts without blocking the receive loop |

### Multi-Replica HA

Istiod runs multiple replicas per cluster. Federation uses **Istio's existing leader election** to designate one replica as the active publisher:

- **Leader:** Publishes snapshots and incrementals, creates and manages the shared subscription
- **Non-leader replicas:** Receive messages via a separate **per-replica subscription** (auto-created with `autoDeleteOnIdle=30m`) for store hydration, but do not publish

This means every replica has current federation state for fast leader failover. On leadership change, the new leader begins publishing immediately — no cold-start delay.

**Competing consumers on the shared subscription** are avoided by having non-leaders use their own ephemeral subscriptions. If non-leaders shared the leader's subscription, messages would be split across replicas nondeterministically.

**Version vector conflicts** from leadership changes are handled naturally: the version vector is per-cluster (not per-replica), so a new leader's first snapshot carries the correct cluster-level version.

---

## Data Plane: No Change

Federation only replaces the **control plane service discovery transport** (how Istiods learn about remote services). The data plane is unchanged:

- Cross-cluster traffic goes through **east-west gateways** via HBONE (port 15008)
- Gateway addresses are communicated as `WireNetworkGateway` in sync messages
- Gateway reachability requires VNet peering, public LB, or an overlay (Tailscale)

```
+--------------+                                    +--------------+
|  AKS Cl. 1   |                                    |  AKS Cl. 2   |
|              |    Control Plane (Service Bus)     |              |
|  Istiod -----+-------- pub/sub -------------------+----- Istiod  |
|              |    *.servicebus.windows.net        |              |
|              |                                    |              |
|  ztunnel     |    Data Plane (unchanged)          |  ztunnel     |
|  -------- E/W GW ======================== E/W GW  --------       |
|              |    VNet Peering / Public LB /      |              |
|              |    Tailscale overlay / VPN         |              |
+--------------+                                    +--------------+
```

---

## SAN Augmentation

**Problem:** When a service exists both locally and via federation, ztunnel receives the local service's empty `SubjectAltNames`. For cross-network traffic, ztunnel builds a double HBONE tunnel (outer to east-west gateway, inner to final destination). The inner tunnel uses `SubjectAltNames` as `final_sans` for TLS verification — empty SANs means unconditional failure.

**Why there's no ztunnel fallback:** `service_sans()` returns only the service's `SubjectAltNames` field. The `workload_and_services_san()` method (which includes workload SPIFFE identity) is used for `upstream_sans` (outer tunnel) but NOT for `final_sans` (inner tunnel).

**Why standard multi-cluster isn't affected:** Standard ambient multi-cluster uses flat networking (same network). Traffic goes directly to the remote pod — no east-west gateway, no double HBONE, no `final_sans` check. Federation always uses multi-network, which means always double HBONE, which means service SANs are required.

> **Note:** This is likely a gap in upstream ambient multi-network. Any multi-network setup
> using standard Kubernetes services (which never populate `SubjectAltNames`) would hit the
> same failure. Consider filing an upstream ztunnel issue.

**Solution:** `augmentServiceWithFederationSANs()` merges federation service SANs into matching local services before they are sent to ztunnel. Verified with 20/20 cross-cluster curl requests succeeding through double HBONE.

---

## Configuration

| Env Variable | Purpose | Example |
|---|---|---|
| `PILOT_ENABLE_FEDERATION` | Feature gate | `true` |
| `PILOT_FEDERATION_LOCAL_NETWORK` | Local network name for gateway lookup | `network1` |
| `PILOT_SERVICEBUS_CONNECTION_STRING` | Service Bus connection string | `Endpoint=sb://istio-fed.servicebus.windows.net/;...` |
| `PILOT_SERVICEBUS_NAMESPACE` | FQDN (for Workload Identity auth) | `istio-fed.servicebus.windows.net` |
| `PILOT_SERVICEBUS_TOPIC` | Topic name | `istio-service-sync` |
| `PILOT_SERVICEBUS_SUBSCRIPTION` | Subscription name (defaults to cluster ID) | `cluster1` |
| `PILOT_SERVICEBUS_AUTO_CREATE_SUBSCRIPTION` | Auto-create subscription on startup | `true` |
| `PILOT_SERVICEBUS_SNAPSHOT_INTERVAL` | Full-sync publish interval | `5m` |

**Authentication:** Prefer **Azure Workload Identity** (federated OIDC) over connection strings. Each AKS cluster's Istiod ServiceAccount is federated to an Azure Managed Identity with `Azure Service Bus Data Sender` and `Azure Service Bus Data Receiver` roles. No secrets to rotate.

---

## Implementation

### File Map

| File | Purpose |
|---|---|
| `pilot/pkg/serviceregistry/federation/servicebus.go` | `ServiceBusTransport` — Azure Service Bus pub/sub |
| `pilot/pkg/serviceregistry/federation/sync.go` | `SyncProtocol` — federation orchestrator |
| `pilot/pkg/serviceregistry/federation/store.go` | `federationStore` — remote service data store |
| `pilot/pkg/serviceregistry/federation/types.go` | `SyncMessage`, wire types |
| `pilot/pkg/serviceregistry/federation/tombstone.go` | Service tombstone manager |
| `pilot/pkg/serviceregistry/federation/version.go` | Version vector tracking |
| `pilot/pkg/features/ambient.go` | Feature flags |
| `pilot/pkg/bootstrap/servicecontroller.go` | `initFederationSync` bootstrap wiring |
| `pilot/pkg/serviceregistry/kube/controller/ambient/federation.go` | KRT collections + ambient index + SAN augmentation |
| `pilot/pkg/model/service.go` | `FederationAmbientIndex`, `RegisterFederationCollections` |
| `pilot/pkg/model/push_context.go` | `FederationUpdate` trigger reason |

### Status

| Phase | Status |
|---|---|
| Core implementation (transport, protocol, store, KRT, feature flags) | Done |
| 3-cluster kind test (c1/c2/c3, shared CA, MetalLB E/W gateways) | Done |
| Cross-cluster data path (double HBONE + SAN augmentation) | Done |
| Delete/redeploy cycle verification | Done |
| Infrastructure automation (Bicep, Workload Identity) | Planned |
| AKS scale testing (10-cluster, 100-cluster simulation) | Planned |
| Failure scenario testing (Service Bus outage, split-brain) | Planned |

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| **Service Bus SPOF** | Cross-cluster discovery fails | Premium tier with geo-DR. Local services unaffected. |
| **Message ordering** | Stale updates regress state | Version vectors drop stale messages. |
| **Cost at scale** | 100 clusters x snapshots + events | Standard tier: ~$0.0135/M ops. 100 clusters at 5m intervals = pennies/day. |
| **Latency** | ~5-20 ms pub/sub overhead | Acceptable for service discovery (not in request path). |
| **Vendor lock-in** | Ties to Azure | Transport layer is swappable — any AMQP 1.0 broker, Kafka, or NATS. |
| **Subscription count** | 100 clusters x 3 replicas = 300 subs | Service Bus supports 2,000 per topic. `autoDeleteOnIdle` cleans up orphans. |
| **Message size** | Large full-sync snapshots | 1,000 services x ~200 bytes = 200 KB, within Standard tier's 256 KB limit. |

---

## Future Extensions

- **Multi-region namespaces** — per-region Service Bus with auto-forwarding to reduce cross-region latency
- **Alternative transports** — Kafka (Confluent/MSK), NATS JetStream for non-Azure environments
- **Selective subscription** — SQL filter rules for namespace/label-scoped service discovery
- **Claim-check pattern** — Blob Storage for clusters with thousands of services

---

## Appendix: Regional vs. Global Namespace

| Dimension | Single Global Namespace | Regional Namespaces + Bridge |
|---|---|---|
| **Topology** | All clusters to 1 namespace | N regional namespaces; bridge syncs between regions |
| **Intra-region latency** | ~40-60ms (cross-region penalty) | ~1-5ms (local namespace) |
| **Blast radius** | Namespace outage = all clusters lose federation | Regional outage = only cross-region sync affected |
| **Cost** | 1 namespace (~$668/mo Premium) | N namespaces; Standard tier viable for smaller regions |
| **Complexity** | Minimal | Bridge deployment per region pair |
| **Subscription fan-out** | 100 subs on 1 topic | ~20 subs per regional topic + bridge subs |
| **Data sovereignty** | All metadata routes through one region | Metadata stays in-region until bridged |

### Recommendation by Scale

| Scale | Best fit | Why |
|---|---|---|
| 3 regions or fewer, 30 clusters or fewer | **Single global** | Simplicity wins. One namespace handles the load. |
| 3-5 regions, 30-100 clusters | **Regional + bridge** | Blast radius and intra-region latency matter. |
| 5+ regions or data sovereignty | **Regional + bridge** | Can't route EU metadata through a US namespace. |

The bridge is a subscriber on one regional namespace that republishes to another — the existing `ServiceBusTransport` code can be reused directly.

# Service Bus Federation vs Upstream Istio — Merge Plan (Updated April 2026)

## Executive Summary

**Status update:** PR `istio/istio#59211` has been **merged** into upstream master (`6c76e33806`). Eight additional PRs have landed in the ambient/multicluster area since then (see §New Post-59211 Changes below).

The local `registry-svc-bus` branch is based on `upstream/release-1.29`. It needs to be ported to a post-59211 upstream base (master or `aks-release-1.30+`). The federation design is architecturally sound and survives the upstream refactor. The attachment points need re-seating.

### Key conclusions (unchanged from original analysis)

- The upstream refactor does not invalidate the Service Bus federation design.
- It does invalidate the old ambient multicluster attachment points the local branch currently uses.
- The right merge strategy is a **fresh branch** from post-59211 upstream, not a blind rebase.
- First adopt the upstream multicluster substrate, then re-attach the Service Bus federation layer at the seams that remain stable: `ambient.Options`, the global service/workload joins, and bootstrap wiring.

### What changed since the plan was written

| Item | March 2026 (plan written) | April 2026 (now) |
|---|---|---|
| PR #59211 | Open, not yet merged | **Merged** (`6c76e33806`) |
| Post-59211 upstream changes | None | 8 PRs touching ambient/multicluster (see below) |
| Local branch base | release-1.29 | Still release-1.29 (unchanged) |
| Local federation code | 3,933 lines across 8 Go files | Same (no changes since March) |
| krt API | `krt.NewIndex` standard | New: `Index.AsCollection()` method added (#59594) |
| Network handling | `a.Network(ctx)` on index | Refactored: `FetchLocalNetworkID` on `NetworkCollections` (#59661) |
| Deleted upstream files | Expected by plan | Confirmed deleted: `ambient/multicluster/`, `remotesecrets.go`, `collectioncache.go` |

### New post-59211 changes that affect the plan

| Commit | PR | Impact on federation |
|---|---|---|
| `004ed5a94d` | krt: allow fetching an index directly (#59594) | **Medium** — `Index.AsCollection()` is now available. The Synthetic EDS bridge in the local branch manually calls `krt.NewCollection` on index results; can simplify using this. |
| `664c48dc8e` | ambient: minor refactor to network handling (#59661) | **Medium** — `a.Network(ctx)` replaced with `a.networks.FetchLocalNetworkID(ctx)`. Local code in `multicluster.go` that calls `a.Network(ctx)` for the SyntheticSplitHorizonWorkloads filter needs updating. |
| `1334b393da` | fix: cross-network WE panic (#59321) | **Low** — Changes `workloads.go` internals (guard for nil network gateways). No federation code touches this path directly. |
| `703223e924` | fix ambient waypoint networks race (#59603) | **Low** — Race fix in waypoint network handling. Not in federation's path. |
| `b9fd5ebcea` | Ambient: Add ingress_use_waypoint flag (#59671) | **None** — New service field, no conflict with federation. |
| `141b8787b3` | Support ns level ingress-use-waypoint label (#59147) | **None** — New namespace label support. |
| `b2438c9edc` | agentgateway collections (#59258) | **None** — New agentgateway feature, orthogonal. |
| `f20c17a860` | add new app protocols (#59259) | **None** — Protocol enum additions. |

## What Upstream PR 59211 Actually Changes

PR 59211 is not just a cleanup. It changes the ownership model for ambient multicluster. **This section is unchanged from the original plan and remains accurate.**

### High-level changes

- Consolidates ambient multicluster handling into the shared `pkg/kube/multicluster` package.
- Removes the ambient-owned multicluster package and its local cluster store/secret handling model.
- Creates the multicluster controller at the top level and passes it down into ambient.
- Unifies the `Cluster` abstraction so it supports both callback-based and KRT-based cluster lifecycle handling.
- Moves nested collection helpers into the shared multicluster package.
- Reworks ambient collection assembly to consume shared per-cluster KRT collections.

### Files that matter most upstream

- [pkg/kube/multicluster/secretcontroller.go](pkg/kube/multicluster/secretcontroller.go)
- [pkg/kube/multicluster/cluster.go](pkg/kube/multicluster/cluster.go)
- [pkg/kube/multicluster/clusterstore.go](pkg/kube/multicluster/clusterstore.go)
- [pkg/kube/multicluster/collections.go](pkg/kube/multicluster/collections.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/ambientindex.go](pilot/pkg/serviceregistry/kube/controller/ambient/ambientindex.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go](pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/workloads.go](pilot/pkg/serviceregistry/kube/controller/ambient/workloads.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/services.go](pilot/pkg/serviceregistry/kube/controller/ambient/services.go)

## What the Local Service Bus Branch Does

The local branch adds Azure Service Bus based federation as a separate service discovery layer for ambient multicluster.

### Core federation pieces

- [pilot/pkg/serviceregistry/federation/servicebus.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/federation/servicebus.go)
- [pilot/pkg/serviceregistry/federation/sync.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/federation/sync.go)
- [pilot/pkg/serviceregistry/federation/store.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/federation/store.go)
- [pilot/pkg/serviceregistry/federation/types.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/federation/types.go)
- [pilot/pkg/serviceregistry/federation/version.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/federation/version.go)

### Ambient integration points

- [pilot/pkg/serviceregistry/kube/controller/ambient/federation_source.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/federation_source.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/federation.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/federation.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go)
- [pilot/pkg/bootstrap/servicecontroller.go](/Users/mjac/abc/istio/pilot/pkg/bootstrap/servicecontroller.go)
- [pilot/pkg/bootstrap/federation.go](/Users/mjac/abc/istio/pilot/pkg/bootstrap/federation.go)
- [pilot/pkg/model/service.go](/Users/mjac/abc/istio/pilot/pkg/model/service.go)

### Design summary

- Outbound publishing is leader-only.
- Inbound processing is replica-wide.
- Federation data is injected as domain objects, not Kubernetes objects.
- `FederationSource` exposes static KRT collections for services and workloads.
- Ambient merges those collections into the global service/workload graph.

This remains a sound design after the upstream refactor.

## Main Compatibility Assessment

### What remains valid

These parts should survive with limited changes:

- Service Bus transport and protocol logic.
- `FederationSource` as a KRT-backed bridge into ambient.
- Bootstrap wiring that creates federation before the kube registry is initialized.
- The `FederationAmbientIndex` interface in model.

Concretely, these files are mostly reusable:

- [pilot/pkg/serviceregistry/federation/servicebus.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/federation/servicebus.go)
- [pilot/pkg/serviceregistry/federation/sync.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/federation/sync.go)
- [pilot/pkg/serviceregistry/federation/store.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/federation/store.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/federation_source.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/federation_source.go)
- [pilot/pkg/bootstrap/federation.go](/Users/mjac/abc/istio/pilot/pkg/bootstrap/federation.go)
- [pilot/pkg/model/service.go](/Users/mjac/abc/istio/pilot/pkg/model/service.go)

### What does not remain valid as-is

The local branch still sits on the old ambient-owned multicluster substrate. That is the part that upstream deletes or centralizes.

Most of the conflict will come from these areas:

- ambient-owned cluster store usage
- ambient-owned secret/controller setup
- ambient-local nested collection helpers
- ambient-specific remote cluster collection lifecycle

Concretely, these local assumptions will need to be rebuilt on top of the shared controller model:

- [pilot/pkg/serviceregistry/kube/controller/ambient/ambientindex.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/ambientindex.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go)

## Most Important Semantic Risk

The highest-risk issue is not transport, bootstrap, or Service Bus state handling.

It is collection ordering inside ambient.

In the local branch, outbound federation intentionally computes SANs from Kubernetes-backed workloads before federated workloads are joined into the global workload collection.

That is visible here:

- [pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/federation.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/federation.go)

Two invariants must survive the rebase:

1. `localServices` must remain pre-federation.
2. `localWorkloadsByServiceKey` must be built before federated workloads are joined.

If either slips, likely failures include:

- outbound federation loops
- duplicate SAN propagation
- mixed local/federated identity sets
- confusing split-horizon behavior

## Why the Federation Design Still Fits the Upstream Model

The upstream refactor makes real Kubernetes remote clusters more structured. It does not make Service Bus federation a real Kubernetes cluster.

That is good.

The current local design keeps federation as an external source of domain objects merged into ambient via KRT collections. That is still the correct layering.

The wrong migration would be to force Service Bus data through the shared multicluster controller as if it were a remote kube cluster.

The right migration is:

- keep real kube multicluster on the new shared controller
- keep Service Bus federation as a direct KRT collection source
- join federation into ambient after the kube-derived collections are assembled

## Operational Assumptions That Must Be Preserved

The migration is not only code movement. The real runtime assumptions still matter.

### Important runtime prerequisites

- Azure Service Bus namespace and topic provisioning
- bootstrap subscription behavior
- per-replica inbound subscriptions
- leader-only outbound publication
- system namespace network labeling

The deployment harness is here:

- [kind-gossip/provision-servicebus.sh](/Users/mjac/abc/kind-gossip/provision-servicebus.sh)

One critical known requirement is that the Istio system namespace must carry the correct network label. If that disappears during migration validation, VIP resolution can fail even when the code looks correct.

## Recommended Merge Strategy

### Summary

Do not try to rebase the full local branch directly across the upstream refactor.

Instead:

1. move to a post-59211 upstream base (commit `6c76e33806` or later on `upstream/master`)
2. restore the new multicluster substrate first
3. reattach the federation layer at the stable seams
4. validate collection ordering and split-horizon behavior aggressively

**Updated note (April 2026):** Since #59211 is now merged, the target base is simply `upstream/master` HEAD. However, 8 additional PRs have landed post-59211. Two require minor adjustments to federation code:
- `a.Network(ctx)` → `a.networks.FetchLocalNetworkID(ctx)` in the SyntheticSplitHorizonWorkloads filter (#59661)
- Consider using `Index.AsCollection()` for the Synthetic EDS bridge instead of manual `krt.NewCollection` wrapping (#59594)

### Detailed plan

#### Phase 1: Freeze the current working implementation

- Tag or otherwise preserve the current local working behavior.
- Treat the local branch as two stacked changesets:
  - substrate changes in ambient multicluster internals
  - federation-layer changes in Service Bus transport, sync, ambient bridge, and bootstrap wiring

#### Phase 2: Adopt upstream 59211 first

- Start from `upstream/master` HEAD (commit `70078a95a9` or later — #59211 is already merged at `6c76e33806`).
- Do not preserve the old ambient-owned multicluster files as the source of truth.
- Let upstream own:
  - cluster lifecycle
  - cluster store semantics
  - nested KRT collection helpers
  - secret/controller wiring

#### Phase 3: Rebuild ambient on the new substrate

- Reshape ambient construction to consume `MultiClusterController` from the shared path.
- Rebuild any current local logic that depends on ambient-local `ClusterStore`, `remoteClusters`, or ambient-owned secret handling.

This is the structural merge.

#### Phase 4: Reattach federation at stable seams

Preserve and reintroduce:

- `FederationSource` injection into ambient options
- bootstrap creation in [pilot/pkg/bootstrap/servicecontroller.go](/Users/mjac/abc/istio/pilot/pkg/bootstrap/servicecontroller.go)
- sync initialization in [pilot/pkg/bootstrap/federation.go](/Users/mjac/abc/istio/pilot/pkg/bootstrap/federation.go)
- interface boundary in [pilot/pkg/model/service.go](/Users/mjac/abc/istio/pilot/pkg/model/service.go)

#### Phase 5: Re-implement the collection joins

In the upstream-shaped ambient multicluster file (`multicluster.go`), the insertion points are now at specific upstream line numbers (as of `upstream/master` HEAD):

**Insert point A** — after `LocalWorkloadServices` (line ~195) and before the status block:
```go
LocalWorkloadServices := builder.ServicesCollection(...)
// >>> INSERT: store local-only collection for outbound federation
a.localServices = LocalWorkloadServices
```

**Insert point B** — after `GlobalMergedWorkloadServices` (line ~268) and before `GlobalWorkloadServicesWithClusterByCluster`:
```go
GlobalMergedWorkloadServices := krt.MapCollection(...)
// >>> INSERT: merge federation services
if options.FederationSource != nil {
    fedSvcs := options.FederationSource.Services()
    GlobalMergedWorkloadServices = krt.JoinCollection(
        []krt.Collection[model.ServiceInfo]{GlobalMergedWorkloadServices, fedSvcs},
        opts.WithName("GlobalMergedWithFederationServices")...,
    )
}
```

**Insert point C** — after `MergedGlobalWorkloadsCollection()` returns (line ~310) and before `GlobalWorkloadServiceIndex` (line ~311):
```go
GlobalWorkloads := MergedGlobalWorkloadsCollection(...)
// >>> INSERT: index local workloads BEFORE federation merge
a.localWorkloadsByServiceKey = krt.NewIndex[string, model.WorkloadInfo](
    GlobalWorkloads, "localService", func(o model.WorkloadInfo) []string {
        return maps.Keys(o.Workload.Services)
    },
)
// >>> INSERT: merge federation workloads
var fedWls krt.Collection[model.WorkloadInfo]
if options.FederationSource != nil {
    fedWls = options.FederationSource.Workloads()
    GlobalWorkloads = krt.JoinCollection(
        []krt.Collection[model.WorkloadInfo]{GlobalWorkloads, fedWls},
        opts.With(krt.WithName("GlobalWithFederationWorkloads"), krt.WithJoinUnchecked())...,
    )
}
GlobalWorkloadServiceIndex := krt.NewIndex[string, model.WorkloadInfo](...)
```

**Insert point D** — SplitHorizonWorkloads join (line ~391). Append `fedWls` to the components slice:
```go
splitHorizonComponents := []krt.Collection[model.WorkloadInfo]{
    coalescedWorkloads,
    networkLocalWorkloads,
}
if fedWls != nil {
    splitHorizonComponents = append(splitHorizonComponents, fedWls)
}
SplitHorizonWorkloads := krt.JoinCollection(splitHorizonComponents, ...)
```

**Insert point E** — Synthetic EDS bridge (after `SplitHorizonWorkloadServiceIndex`, line ~407). The SyntheticSplitHorizonWorkloads filter needs updating for post-#59661:
```go
// Use a.networks.FetchLocalNetworkID(ctx) instead of the old a.Network(ctx)
if wi.Workload.Network == a.networks.FetchLocalNetworkID(ctx).String() {
    return nil
}
```

These five insertion points are the complete set. All other federation code (transport, protocol, store, federation_source.go, bootstrap wiring) copies over without modification.

#### Phase 6: Re-establish outbound federation guards

Recreate the local-only data paths for:

- `AllLocalNetworkGlobalServicesWithSANs()`
- `ServiceWithSANs()`
- `RegisterGlobalServiceHandler()`

Those must continue to ignore previously federated data.

#### Phase 7: Validate split-horizon and gateway behavior

Federation workloads are already-formed domain objects and should continue to bypass the Kubernetes coalescing path.

That needs explicit revalidation against the upstream collection graph, especially for:

- remote network gateways
- waypoint-visible service identities
- split-horizon routing

#### Phase 8: Trim dead substrate code

Delete any local code that only existed because ambient used to own multicluster internals.

That includes local copies of:

- collection helper caches
- ambient-local cluster stores
- remote secret handling logic that upstream now centralizes

## Conflict Forecast

### High conflict risk

- [pilot/pkg/serviceregistry/kube/controller/ambient/ambientindex.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/ambientindex.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/workloads.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/workloads.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/services.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/services.go)

### Medium conflict risk

- [pilot/pkg/bootstrap/servicecontroller.go](/Users/mjac/abc/istio/pilot/pkg/bootstrap/servicecontroller.go)
- [pilot/pkg/bootstrap/federation.go](/Users/mjac/abc/istio/pilot/pkg/bootstrap/federation.go)
- [pilot/pkg/model/service.go](/Users/mjac/abc/istio/pilot/pkg/model/service.go)

### Lower conflict risk

- [pilot/pkg/serviceregistry/federation/servicebus.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/federation/servicebus.go)
- [pilot/pkg/serviceregistry/federation/sync.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/federation/sync.go)
- [pilot/pkg/serviceregistry/federation/store.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/federation/store.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/federation_source.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/federation_source.go)

## Verification Plan

### Checkpoint 1: Upstream substrate only

Verify ambient multicluster works without federation enabled after the substrate move.

Focus areas:

- remote cluster discovery
- waypoint resolution
- split-horizon workload generation

### Checkpoint 2: Federation injection restored

Verify:

- `FederationSource` is created before kube registry startup
- `SyncProtocol` can obtain a non-nil ambient index
- startup ordering is stable

### Checkpoint 3: Service merge semantics

Verify federated services appear in the final global service collection before split-horizon service derivation.

### Checkpoint 4: Workload merge semantics

Verify federated workloads:

- are merged after local/remote kube workloads
- remain visible to split-horizon and waypoint logic
- are not accidentally coalesced away

### Checkpoint 5: Outbound safety

Verify SAN computation still excludes already-federated workloads and services.

### Checkpoint 6: Runtime HA

Verify:

- per-replica inbound subscriptions converge on the same state
- leader-only outbound publication remains intact
- leader transitions do not regress version behavior

### Checkpoint 7: Real topology validation

Re-run the real Service Bus topology with at least three clusters and validate:

- remote VIP resolution
- network gateway behavior
- waypoint failover semantics

## Reviewer Reading Order

If someone needs to understand the migration quickly, read these files first:

1. [pilot/pkg/serviceregistry/federation/servicebus.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/federation/servicebus.go)
2. [pilot/pkg/serviceregistry/federation/sync.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/federation/sync.go)
3. [pilot/pkg/serviceregistry/kube/controller/ambient/federation_source.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/federation_source.go)
4. [pilot/pkg/serviceregistry/kube/controller/ambient/federation.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/federation.go)
5. [pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go)
6. [pkg/kube/multicluster/secretcontroller.go](/Users/mjac/abc/istio/pkg/kube/multicluster/secretcontroller.go)
7. [pkg/kube/multicluster/collections.go](/Users/mjac/abc/istio/pkg/kube/multicluster/collections.go)

## Final Recommendation

Do not do a long conflict-heavy rebase of `registry-svc-bus` directly onto a post-59211 upstream head.

Create a fresh branch from `upstream/master` (which already contains #59211 and 8 follow-up PRs) and transplant the federation layer there.

Specifically:

- keep the upstream multicluster substrate
- preserve Service Bus federation as an external KRT-backed source
- reattach federation through ambient options and final service/workload joins (5 insertion points)
- explicitly preserve the local-only SAN computation invariants
- update `a.Network(ctx)` → `a.networks.FetchLocalNetworkID(ctx)` for the Synthetic filter (#59661)
- consider adopting `Index.AsCollection()` for the Synthetic EDS bridge (#59594)

That path is more work up front, but it is more likely to produce a clean result and a branch that remains maintainable after the next upstream ambient refactor.

**Estimated effort:** The federation layer is ~3,933 lines of new Go code (unchanged, copies directly) plus ~120 lines of integration code at 5 insertion points in `multicluster.go` and field additions to `ambientindex.go`. Two minor API adaptations for post-59211 changes. Total: 1-2 days of focused work.
# Service Bus Federation vs Upstream Istio PR 59211

## Executive Summary

The local `registry-svc-bus` work is layered onto the pre-59211 ambient multicluster substrate, while upstream `istio/istio#59211` deletes that ambient-owned multicluster package and moves ambient onto the shared `pkg/kube/multicluster` controller and nested-collection helpers.

The main conclusion is:

- The upstream refactor does not invalidate the Service Bus federation design.
- It does invalidate the old ambient multicluster attachment points the local branch currently uses.
- The right merge strategy is not a blind rebase.
- First adopt the upstream multicluster substrate, then re-attach the Service Bus federation layer at the seams that remain stable: `ambient.Options`, the global service/workload joins, and bootstrap wiring.

In practice, most of the merge pain is structural, not conceptual.

## What Upstream PR 59211 Actually Changes

PR 59211 is not just a cleanup. It changes the ownership model for ambient multicluster.

### High-level changes

- Consolidates ambient multicluster handling into the shared `pkg/kube/multicluster` package.
- Removes the ambient-owned multicluster package and its local cluster store/secret handling model.
- Creates the multicluster controller at the top level and passes it down into ambient.
- Unifies the `Cluster` abstraction so it supports both callback-based and KRT-based cluster lifecycle handling.
- Moves nested collection helpers into the shared multicluster package.
- Reworks ambient collection assembly to consume shared per-cluster KRT collections.

### Files that matter most upstream

- [pkg/kube/multicluster/secretcontroller.go](/Users/mjac/abc/istio/pkg/kube/multicluster/secretcontroller.go)
- [pkg/kube/multicluster/cluster.go](/Users/mjac/abc/istio/pkg/kube/multicluster/cluster.go)
- [pkg/kube/multicluster/clusterstore.go](/Users/mjac/abc/istio/pkg/kube/multicluster/clusterstore.go)
- [pkg/kube/multicluster/collections.go](/Users/mjac/abc/istio/pkg/kube/multicluster/collections.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/ambientindex.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/ambientindex.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/multicluster.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/workloads.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/workloads.go)
- [pilot/pkg/serviceregistry/kube/controller/ambient/services.go](/Users/mjac/abc/istio/pilot/pkg/serviceregistry/kube/controller/ambient/services.go)

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

1. move to a post-59211 upstream base
2. restore the new multicluster substrate first
3. reattach the federation layer at the stable seams
4. validate collection ordering and split-horizon behavior aggressively

### Detailed plan

#### Phase 1: Freeze the current working implementation

- Tag or otherwise preserve the current local working behavior.
- Treat the local branch as two stacked changesets:
  - substrate changes in ambient multicluster internals
  - federation-layer changes in Service Bus transport, sync, ambient bridge, and bootstrap wiring

#### Phase 2: Adopt upstream 59211 first

- Start from a commit that already contains the upstream shared multicluster controller refactor.
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

In the upstream-shaped ambient multicluster file:

- join federated services into the final global service collection before split-horizon service derivation
- join federated workloads after the kube-derived local/remote workload collections are assembled
- keep federation as direct collection joins, not nested per-cluster collections

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

Create a fresh branch from a post-59211 upstream commit and transplant the federation layer there.

Specifically:

- keep the upstream multicluster substrate
- preserve Service Bus federation as an external KRT-backed source
- reattach federation through ambient options and final service/workload joins
- explicitly preserve the local-only SAN computation invariants

That path is more work up front, but it is more likely to produce a clean result and a branch that remains maintainable after the next upstream ambient refactor.
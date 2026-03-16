# Service Bus Federation vs Upstream Istio PR #59211 — Deep Dive Analysis (Opus 4.6)

## 1. Executive Summary

Upstream PR `istio/istio#59211` ("multicluster: consolidate ambient and standard multicluster controllers") is a **structural ownership transfer** — ambient multicluster no longer owns its own secret-watching, client-building, cluster-store, or nested-collection infrastructure. Everything migrates to the shared `pkg/kube/multicluster` package and is passed *into* ambient via a new `MultiClusterController` field on `Options`.

The local `registry-svc-bus` branch's federation layer is architecturally sound and conceptually orthogonal to this refactor. No federation protocol logic (Service Bus transport, sync protocol, debounced message handling, version vectors, federation store) is affected. What *is* affected is the **wiring** — the 4-5 specific attachment points where federation's `FederationSource` plugs into the ambient index's collection assembly pipeline.

**Bottom line:** The federation design survives. The attachment points need to be re-seated on the new substrate. A blind rebase will produce ~8 conflict zones, most of which are mechanical. Two are structurally subtle and require careful ordering.

---

## 2. What Upstream PR #59211 Actually Does

### 2.1 The ownership model change

Before #59211, ambient *owned* multicluster:
- `ambient/multicluster/` package: `Cluster`, `ClusterStore`, `ClientBuilder`, `RemoteClusterCollections`
- `ambient/remotesecrets.go`: secret watching, `buildRemoteClustersCollection()`, `processSecretEvent()`
- `ambient/collectioncache.go`: `collectionCacheByCluster[T]` for per-cluster collection memoization
- `ambient/multicluster.go`: `nestedCollectionFromLocalAndRemote()`, `informerIndexByCluster()`, `nestedCollectionIndexByCluster()` — local functions

After #59211, shared `pkg/kube/multicluster` owns everything:
- `multicluster.Controller` gains: `Clusters() krt.Collection[*Cluster]`, `ConfigCluster() *Cluster`, `ClusterStore() *ClusterStore`
- `multicluster.Cluster` gains: `Namespaces()`, `Pods()`, `Services()`, `EndpointSlices()`, `Nodes()`, `Gateways()` — KRT collections per-cluster
- `multicluster.ControllerOptions` replaces positional params
- New `multicluster/collections.go`: `NestedCollectionFromLocalAndRemote()`, `NestedManyCollectionsFromLocalAndRemote()`, `NestedCollectionIndexByCluster()`
- `ClusterStore` gains `AllReady()`, `RecomputeTrigger`, `triggerRecomputeOnSync()` — all lifted from the old ambient package

### 2.2 What is deleted

| Deleted file/symbol | Line count | Replacement |
|---|---|---|
| `ambient/multicluster/cluster.go` | 363 lines | `pkg/kube/multicluster/cluster.go` gains KRT collections, `Run()` builds them |
| `ambient/multicluster/clusterstore.go` | 224 lines | `pkg/kube/multicluster/clusterstore.go` gains `AllReady()`, `RecomputeTrigger` |
| `ambient/remotesecrets.go` | 258 lines | `Controller.buildClustersCollection()` + `Controller.Clusters()` in secretcontroller.go |
| `ambient/remotesecrets_test.go` | 855 lines | Tests moved to `pkg/kube/multicluster/secretcontroller_test.go` |
| `ambient/collectioncache.go` | 80 lines | Replaced by `collectionCacheByClusterMany[T]` in `collections.go` |
| `nestedCollectionFromLocalAndRemote()` | ~65 lines | `multicluster.NestedCollectionFromLocalAndRemote()` (with `*Controller` as first arg) |
| `nestedCollectionIndexByCluster()` | ~12 lines | `multicluster.NestedCollectionIndexByCluster()` |
| `informerIndexByCluster()` | ~12 lines | Same (merged into `NestedCollectionIndexByCluster`) |

**Total removed**: ~1400 net lines from ambient package.

### 2.3 What changes in `ambientindex.go`

The `Options` struct loses:
- `Client kubeclient.Client` — removed; client comes from `MultiClusterController.ConfigCluster().Client`
- `IsConfigCluster bool` — removed; always true, ambient only created on config cluster
- `ClientBuilder multicluster.ClientBuilder` — removed; shared controller handles this
- `RemoteClientConfigOverrides []func(*rest.Config)` — removed; passed to `ControllerOptions.ConfigOverrides`

The `Options` struct gains:
- `MultiClusterController *multicluster.Controller`

The `index` struct loses:
- `cs *multicluster.ClusterStore`
- `clientBuilder multicluster.ClientBuilder`
- `secrets krt.Collection[*corev1.Secret]`
- `remoteClusters krt.Collection[*multicluster.Cluster]`
- `remoteClientConfigOverrides []func(*rest.Config)`

The `index` struct gains:
- `mcController *multicluster.Controller`

The `New()` function's collection assembly changes:
- All "local cluster" informers (Namespaces, Pods, Services, EndpointSlices, Nodes, Gateways) now come from `a.mcController.ConfigCluster().Pods()` etc. instead of being built inline
- The `client` used for write-clients and delayed informers comes from `LocalCluster.Client` instead of `options.Client`
- The `features.EnableAmbientMultiNetwork && options.IsConfigCluster` guard simplifies to `features.EnableAmbientMultiNetwork`

### 2.4 What changes in `multicluster.go` (the buildGlobalCollections function)

The signature loses `configOverrides ...func(*rest.Config)`. The body loses the `buildRemoteClustersCollection()` call. Instead, `a.mcController.Clusters()` is used everywhere clusters were previously passed.

All calls to `nestedCollectionFromLocalAndRemote(local, clusters, ...)` become `multicluster.NestedCollectionFromLocalAndRemote(a.mcController, local, ...)`.

The workloads path (`MergedGlobalWorkloadsCollection`) switches from manual `krt.NewStaticCollection` + `krt.NewManyCollection` + `RegisterBatch` to the new `multicluster.NestedManyCollectionsFromLocalAndRemote()` helper, which encapsulates the same pattern.

### 2.5 What changes in `workloads.go`

`MergedGlobalWorkloadsCollection` loses the `clusters krt.Collection[*multicluster.Cluster]` parameter and gains `ctrl *multicluster.Controller`. The entire manual cache machinery (`podWorkloadInfosCache`, `workloadEntryWorkloadInfosCache`, etc. — 5 caches) is deleted. The `clusters.Register(...)` delete-handler is deleted. The `GlobalWorkloadInfosWithCluster` static collection + manual `RegisterBatch` is replaced by `multicluster.NestedManyCollectionsFromLocalAndRemote(ctrl, localCollections, ...)`.

### 2.6 What changes in `services.go`

`GlobalNestedWorkloadServicesCollection` loses `clusters krt.Collection[*multicluster.Cluster]` and gains `ctrl *multicluster.Controller`. Internal call changes from `nestedCollectionFromLocalAndRemote(local, clusters, ...)` to `multicluster.NestedCollectionFromLocalAndRemote(ctrl, local, ...)`.

### 2.7 What changes in `controller.go` (kube controller)

`Options` gains `MultiClusterController *multicluster.Controller`. The ambient index construction drops `Client`, `IsConfigCluster`, `ClientBuilder`, `RemoteClientConfigOverrides` and adds `MultiClusterController: options.MultiClusterController`.

---

## 3. How the Local Federation Layer Works (Current Branch)

### 3.1 Architecture overview

```
                       ┌────────────────────────────────────────────────────────┐
                       │              Bootstrap (server startup)                │
                       │                                                       │
                       │  1. FederationSource = NewFederationSource(clusterID)  │
                       │  2. KubeOptions.FederationSource = fedSource           │
                       │  3. initFederationSync():                              │
                       │     a. ServiceBusTransport(connStr/namespace)          │
                       │     b. SyncProtocol(transport, fedSource, ambientIdx)  │
                       │     c. Leader election → BecomeLeader()/StopLeading()  │
                       └────────────────────────────────────────────────────────┘
                                      │
                       ┌──────────────▼──────────────┐
                       │     Ambient Index (KRT)      │
                       │                              │
                       │  Options.FederationSource ──►│
                       │                              │
                       │  multicluster.go L283:       │
                       │   JoinCollection(            │
                       │     GlobalMergedServices,    │
                       │     fedSource.Services()     │
                       │   )                          │
                       │                              │
                       │  multicluster.go L343:       │
                       │   localWorkloadsByServiceKey  │
                       │   = Index(GlobalWorkloads)   │ ◄── BEFORE federation merge
                       │                              │
                       │  multicluster.go L355:       │
                       │   JoinCollection(            │
                       │     GlobalWorkloads,         │
                       │     fedSource.Workloads()    │
                       │   )                          │
                       └──────────────────────────────┘
```

### 3.2 Critical ordering invariant

The **ordering** of collection assembly in `multicluster.go` is load-bearing:

1. `LocalWorkloadServices` is built (pre-multicluster-merge local k8s services)
2. `a.localServices = LocalWorkloadServices` — stored for outbound federation to read local-only data
3. `GlobalMergedWorkloadServices` is built (multicluster merge across k8s clusters)
4. **Federation services merged** via `krt.JoinCollection` — appended at L283
5. `GlobalWorkloads` is built (multicluster merge of all workloads)
6. `a.localWorkloadsByServiceKey` = index on `GlobalWorkloads` — **BEFORE** federation workload merge (L343)
7. **Federation workloads merged** via `krt.JoinCollection` at L355

Steps 2, 6 are the critical invariants. If federation data leaks into `localServices` or `localWorkloadsByServiceKey`, the outbound sync re-broadcasts received federation data, creating an infinite loop.

### 3.3 Files that are entirely new (won't conflict)

These files don't exist upstream and are pure additions:

| File | Lines | Purpose |
|---|---|---|
| `federation/servicebus.go` | 660 | Azure Service Bus transport |
| `federation/sync.go` | 618 | Sync protocol (debounce, version vectors, leader logic) |
| `federation/store.go` | 513 | Federation state store (split-horizon workloads, VIP injection) |
| `federation/types.go` | 182 | Wire format types, ServiceInfo ↔ WireServiceInfo conversion |
| `federation/version.go` | 79 | Version vector implementation |
| `federation/types_test.go` | 103 | Round-trip tests |
| `ambient/federation.go` | 176 | FederationAmbientIndex implementation (SAN enrichment, handlers) |
| `ambient/federation_source.go` | 105 | StaticCollection bridge |
| `bootstrap/federation.go` | 117 | Server-side init (transport, protocol, leader election) |
| `features/ambient.go` additions | ~30 | Feature flags and env vars |

### 3.4 Files that modify upstream code (will conflict or need reshaping)

| File | Nature of change | Conflict severity |
|---|---|---|
| `ambient/ambientindex.go` | Added `FederationSource *FederationSource` on Options, `localServices`/`localWorkloadsByServiceKey` on index struct | **HIGH** — struct completely restructured upstream |
| `ambient/multicluster.go` | L222 `a.localServices=...`, L283-297 federation service join, L343 `localWorkloadsByServiceKey`, L351-359 federation workload join | **HIGH** — function signature changed, `clusters` param removed, helper functions relocated |
| `bootstrap/servicecontroller.go` | L45-49 FederationSource creation, L70-73 initFederationSync call | **LOW** — additions only, small section |
| `model/service.go` | `FederationAmbientIndex` interface, `GlobalServiceHandler` type | **LOW** — pure additions |
| `aggregate/controller.go` | `AmbientIndexGetter` interface, `GetAmbientIndex()` method | **LOW** — pure additions |
| `controller/controller.go` | `FederationSource` on Options, `AmbientIndex()` method, Options passthrough | **MEDIUM** — Options struct changed upstream (gains `MultiClusterController`) |

---

## 4. Precise Conflict Forecast

### 4.1 `ambient/ambientindex.go` — HIGH conflict

**Local changes:**
- `Index` interface: adds `model.FederationAmbientIndex` embed (L68)
- `index` struct: adds `localServices krt.Collection[model.ServiceInfo]` (L145), `localWorkloadsByServiceKey krt.Index[string, model.WorkloadInfo]` (L148)
- `Options` struct: adds `FederationSource *FederationSource` (L175-177)
- `New()` function: unchanged (federation wiring is in `multicluster.go`)

**Upstream changes:**
- `index` struct: removes `cs`, `clientBuilder`, `secrets`, `kubeconfigs`, `remoteClusters`, `remoteClientConfigOverrides`; adds `mcController`, `meshConfig`
- `Options` struct: removes `Client`, `IsConfigCluster`, `ClientBuilder`, `RemoteClientConfigOverrides`; adds `MultiClusterController`
- `New()` function: completely rewritten — informers from `ConfigCluster()`, client from `LocalCluster.Client`
- Import path changes: `ambient/multicluster` → `pkg/kube/multicluster`

**Resolution:** Apply upstream struct changes, then re-add federation fields. The federation fields (`localServices`, `localWorkloadsByServiceKey`, `FederationSource`) are simple additions that don't conflict conceptually with any upstream removal. The `Index` interface embed is also a pure addition.

### 4.2 `ambient/multicluster.go` — HIGH conflict (most complex)

**Local changes:**
- L67-73: `buildRemoteClustersCollection()` call (upstream deletes this)
- L222: `a.localServices = LocalWorkloadServices` (federation stores local-only collection)
- L278-297: Federation service merge block
- L305: `GobalWorkloadServicesWithClusterByCluster` (typo preserved from upstream — note: upstream *fixes* this typo to `GlobalWorkloadServicesWithClusterByCluster` in #59211)
- L343: `a.localWorkloadsByServiceKey` index creation
- L351-359: Federation workload merge block

**Upstream changes:**
- `buildGlobalCollections` signature loses `configOverrides ...func(*rest.Config)`, removes `buildRemoteClustersCollection()` call
- All `nestedCollectionFromLocalAndRemote(local, clusters, ...)` → `multicluster.NestedCollectionFromLocalAndRemote(a.mcController, local, ...)`
- All `nestedCollectionIndexByCluster()` → `multicluster.NestedCollectionIndexByCluster()`
- `options.Client` → `localCluster.Client`
- Workload path restructured: no more manual caches, uses `multicluster.NestedManyCollectionsFromLocalAndRemote()`
- Typo fix: `GobalWorkloadServicesWithClusterByCluster` → `GlobalWorkloadServicesWithClusterByCluster`

**Resolution strategy:**
1. Accept all upstream mechanical changes (function locations, import paths, signature changes)
2. In the post-merge `buildGlobalCollections()`:
   - Re-insert `a.localServices = LocalWorkloadServices` at the same logical point (after `ServicesCollection()`, before global merge)
   - Re-insert federation service merge block after `GlobalMergedWorkloadServices` is computed
   - Re-insert `a.localWorkloadsByServiceKey` index **after** `MergedGlobalWorkloadsCollection()` returns but **before** the federation workload join
   - Re-insert federation workload join after the index

The key insight: `MergedGlobalWorkloadsCollection`'s return value is what we index and then join with. Upstream simply restructured *how* that collection is built internally (NestedManyCollections), but the return type is the same: `krt.Collection[model.WorkloadInfo]`. So the federation join point is stable.

### 4.3 `ambient/workloads.go` — LOW conflict

Local branch doesn't modify this file for federation. The federation workload join happens in `multicluster.go`, not here. Upstream restructures this file heavily but there's no local change to conflict.

### 4.4 `ambient/services.go` — LOW conflict

`GlobalNestedWorkloadServicesCollection` signature changes (`clusters` → `ctrl`). Local branch doesn't modify this function for federation. The federation service join happens after this function returns, in `multicluster.go`.

### 4.5 `controller/controller.go` — MEDIUM conflict

Local adds `FederationSource *FederationSource` to `Options` (L168-170) and `AmbientIndex()` method. Upstream adds `MultiClusterController *multicluster.Controller` to `Options` and changes the ambient index construction. These are independent additions to the same struct — straightforward merge.

### 4.6 `bootstrap/servicecontroller.go` — LOW conflict

Local additions (FederationSource creation, initFederationSync call) are in isolated blocks. No structural upstream changes in this file.

---

## 5. Semantic Risks and Invariant Analysis

### 5.1 The local-services invariant

**Risk: CRITICAL.** The outbound federation path calls `a.AllLocalNetworkGlobalServicesWithSANs()`, which reads `a.localServices`. This collection must contain only locally-discovered services (not federation-received ones), otherwise data loops back.

**Status:** This invariant is maintained by the assignment `a.localServices = LocalWorkloadServices` in `multicluster.go`, which happens before the `krt.JoinCollection(GlobalMergedWorkloadServices, fedSvcs)`. The upstream changes don't affect the ordering of `ServicesCollection()` vs global merge vs federation merge — the function still builds local first, then global, then (we re-add) federation. **Safe.**

### 5.2 The localWorkloadsByServiceKey invariant

**Risk: CRITICAL.** `ServiceWithSANs()` in `federation.go` uses `a.localWorkloadsByServiceKey` to compute SANs from local-only workloads. If this index included federation workloads, outbound SAN data would be corrupted.

**Status:** The index must be built on `GlobalWorkloads` (multicluster k8s workloads) *before* the `krt.JoinCollection(GlobalWorkloads, fedWls)`. In the upstream code, `MergedGlobalWorkloadsCollection()` now uses `NestedManyCollectionsFromLocalAndRemote()` internally, but it still returns a single `krt.Collection[model.WorkloadInfo]`. We insert the index and federation join at the call site in `multicluster.go`, not inside the function. **Safe, as long as insertion order is preserved.**

### 5.3 The XDS push registration

**Risk: LOW.** The federation services `RegisterBatch` at L290-297 pushes XDS updates when federation services change. This uses `PushXdsAddress(a.XDSUpdater, ...)` — which is unchanged upstream. **Safe.**

### 5.4 The `options.Client` → `localCluster.Client` migration

**Risk: LOW for federation.** The status queue section uses `options.Client` to create write-clients. Upstream changes this to `localCluster.Client`. Federation doesn't create write-clients. **No impact.**

### 5.5 Leader election and bootstrap

**Risk: NONE.** The federation leader election (in `bootstrap/federation.go`) and Service Bus transport are completely outside the ambient index's collection pipeline. They interact only through the `FederationAmbientIndex` interface (which reads `localServices` and `localWorkloadsByServiceKey`) and the `FederationSource`'s static collections (which push into the pipeline). Neither of these interfaces changes upstream.

### 5.6 The `krt.WithJoinUnchecked()` option

**Risk: LOW.** Federation workloads are joined with `krt.WithJoinUnchecked()` because federation workloads may have synthetic UIDs that collide with local workload UIDs. This option is a local addition. The join target changes from inline `GlobalWorkloads` to the output of `MergedGlobalWorkloadsCollection()`, but the semantics are identical. **Safe.**

### 5.7 The `cloneServiceInfoPreservingMetadata` helper

**Risk: LOW.** Used in `federation.go` for SAN enrichment. This is a local addition to `services.go`. It doesn't interact with any code changed by upstream. **Safe.**

---

## 6. Merge Strategy

### 6.1 Recommended approach: Fresh branch from post-#59211 upstream, cherry-pick federation

**Why not rebase?** A rebase would produce conflict clusters in `ambientindex.go` and `multicluster.go` where git can't determine the correct resolution because large blocks were deleted upstream (the entire ambient multicluster package) and different blocks were added locally (federation joins). Git's 3-way merge would produce a mess.

**Why fresh branch?** The federation changes are a clean layer:
- ~2400 lines of new files that don't exist upstream
- ~50 lines of modifications to shared files (Options structs, bootstrap wiring)
- ~30 lines of collection joins inserted into `multicluster.go`

It's faster and safer to start from clean upstream, add the new files, and re-wire the joins.

### 6.2 Step-by-step plan

#### Phase 1: Set up base
```bash
git fetch upstream
git checkout -b registry-svc-bus-v2 upstream/master  # or whichever branch has #59211 merged
```

#### Phase 2: Copy over pure-addition files (no merge needed)
```
pilot/pkg/serviceregistry/federation/       # entire directory
pilot/pkg/serviceregistry/kube/controller/ambient/federation.go
pilot/pkg/serviceregistry/kube/controller/ambient/federation_source.go
pilot/pkg/bootstrap/federation.go
```

#### Phase 3: Add interface definitions
- `model/service.go`: Add `GlobalServiceHandler` type and `FederationAmbientIndex` interface
- `aggregate/controller.go`: Add `AmbientIndexGetter` interface and `GetAmbientIndex()` method
- `ambient/ambientindex.go`: Embed `model.FederationAmbientIndex` in `Index` interface

#### Phase 4: Add fields to structs
- `ambient/ambientindex.go` → `index` struct: Add `localServices krt.Collection[model.ServiceInfo]`, `localWorkloadsByServiceKey krt.Index[string, model.WorkloadInfo]`
- `ambient/ambientindex.go` → `Options` struct: Add `FederationSource *FederationSource`
- `controller/controller.go` → `Options` struct: Add `FederationSource *ambient.FederationSource`, passthrough to `ambient.Options`
- `controller/controller.go`: Add `AmbientIndex() model.FederationAmbientIndex` method

#### Phase 5: Wire bootstrap
- `bootstrap/servicecontroller.go`: Add FederationSource creation and initFederationSync call
- `features/ambient.go`: Add federation feature flags and env vars

#### Phase 6: Re-wire the federation joins in `multicluster.go` (the critical step)

In the post-#59211 `buildGlobalCollections()`, locate these waypoints and insert federation code:

**Insert point A** — after `LocalWorkloadServices` is created and before the global merge:
```go
LocalWorkloadServices := a.builder.ServicesCollection(...)
// >>> INSERT: store local-only collection for outbound federation
a.localServices = LocalWorkloadServices
```

**Insert point B** — after `GlobalMergedWorkloadServices` is computed:
```go
GlobalMergedWorkloadServices := krt.MapCollection(...)
// >>> INSERT: merge federation services
if options.FederationSource != nil {
    fedSvcs := options.FederationSource.Services()
    GlobalMergedWorkloadServices = krt.JoinCollection(
        []krt.Collection[model.ServiceInfo]{GlobalMergedWorkloadServices, fedSvcs},
        opts.WithName("GlobalMergedWithFederationServices")...,
    )
    fedSvcs.RegisterBatch(krt.BatchedEventFilter(
        func(a model.ServiceInfo) *workloadapi.Service { return a.Service },
        PushXdsAddress(a.XDSUpdater, model.ServiceInfo.ResourceName),
    ), false)
}
```

**Insert point C** — after `MergedGlobalWorkloadsCollection()` returns and before `GlobalWorkloadServiceIndex`:
```go
GlobalWorkloads := MergedGlobalWorkloadsCollection(...)
// >>> INSERT: index local workloads BEFORE federation merge
a.localWorkloadsByServiceKey = krt.NewIndex[string, model.WorkloadInfo](GlobalWorkloads, "localService", func(o model.WorkloadInfo) []string {
    return maps.Keys(o.Workload.Services)
})
// >>> INSERT: merge federation workloads
if options.FederationSource != nil {
    fedWls := options.FederationSource.Workloads()
    GlobalWorkloads = krt.JoinCollection(
        []krt.Collection[model.WorkloadInfo]{GlobalWorkloads, fedWls},
        opts.With(krt.WithName("GlobalWithFederationWorkloads"), krt.WithJoinUnchecked())...,
    )
}
GlobalWorkloadServiceIndex := krt.NewIndex[string, model.WorkloadInfo](GlobalWorkloads, "service", ...)
```

#### Phase 7: Update imports
- Change any local references from `ambient/multicluster.Cluster` to `pkg/kube/multicluster.Cluster`
- Update `federation_source.go` imports if needed (unlikely — it uses `krt` and `model`, not multicluster types)

#### Phase 8: Verify and test
- Run ambient unit tests: `go test ./pilot/pkg/serviceregistry/kube/controller/ambient/...`
- Run federation unit tests: `go test ./pilot/pkg/serviceregistry/federation/...`
- Run bootstrap tests
- Run multicluster integration tests
- Manual test with kind clusters + Service Bus

---

## 7. Detailed File-Level Impact Matrix

| File | Local changes | Upstream #59211 changes | Merge action |
|---|---|---|---|
| `ambient/ambientindex.go` | Add `FederationAmbientIndex` embed, `localServices`/`localWorkloadsByServiceKey` fields, `FederationSource` option | Remove `Client`/`IsConfigCluster`/`ClientBuilder`/etc from Options, add `MultiClusterController`, restructure `New()` | Accept upstream, re-add federation fields |
| `ambient/multicluster.go` | Add `localServices` storage, federation service/workload joins, `localWorkloadsByServiceKey` index | Remove `buildRemoteClustersCollection()`, rename helpers to `multicluster.XYZ()`, restructure workload path | Accept upstream, re-insert federation blocks at correct waypoints |
| `ambient/federation.go` | **New file** — FederationAmbientIndex impl | N/A | Copy as-is |
| `ambient/federation_source.go` | **New file** — StaticCollection bridge | N/A | Copy as-is |
| `ambient/services.go` | No direct federation changes | `clusters` param → `ctrl *multicluster.Controller` | Accept upstream |
| `ambient/workloads.go` | No direct federation changes | Complete restructuring (caches removed, `NestedManyCollections`) | Accept upstream |
| `ambient/networks.go` | No federation changes | Import path change | Accept upstream |
| `ambient/waypoints.go` | No federation changes | Signature change | Accept upstream |
| `ambient/nodes.go` | No federation changes | Import path change | Accept upstream |
| `ambient/collectioncache.go` | Not modified by federation | **Deleted** upstream | Accept deletion |
| `ambient/remotesecrets.go` | Not modified by federation | **Deleted** upstream | Accept deletion |
| `ambient/remotesecrets_test.go` | Not modified by federation | **Deleted** upstream | Accept deletion |
| `ambient/multicluster/` package | Not modified by federation | **Deleted** upstream | Accept deletion |
| `controller/controller.go` | Add `FederationSource` on Options, `AmbientIndex()` method | Add `MultiClusterController` on Options, restructure ambient init | Accept both |
| `controller/multicluster.go` | No federation changes | Pass `MultiClusterController` to options | Accept upstream |
| `controller/fake.go` | No federation changes | Create multicluster controller in tests | Accept upstream |
| `bootstrap/server.go` | No federation changes | `ControllerOptions{}` struct construction | Accept upstream |
| `bootstrap/servicecontroller.go` | Add FederationSource creation, initFederationSync call | No changes | Apply federation additions |
| `bootstrap/federation.go` | **New file** | N/A | Copy as-is |
| `model/service.go` | Add `FederationAmbientIndex`, `GlobalServiceHandler` | No changes in relevant area | Apply additions |
| `aggregate/controller.go` | Add `AmbientIndexGetter`, `GetAmbientIndex()` | No changes | Apply additions |
| `features/ambient.go` | Add federation flags/env vars | No changes in relevant area | Apply additions |
| `federation/` package | **Entirely new** (~2100 lines) | N/A | Copy as-is |
| `pkg/kube/multicluster/cluster.go` | No local changes | Major expansion (KRT collections, `Run()` changes) | Accept upstream |
| `pkg/kube/multicluster/clusterstore.go` | No local changes | Gains `AllReady()`, `RecomputeTrigger` | Accept upstream |
| `pkg/kube/multicluster/secretcontroller.go` | No local changes | `ControllerOptions`, `buildClustersCollection()`, `Clusters()`, `ConfigCluster()` | Accept upstream |
| `pkg/kube/multicluster/collections.go` | N/A | **New upstream file** — shared nested collection helpers | Accept upstream |

---

## 8. Verification Checkpoints

After completing the merge, verify each of these:

### 8.1 Compilation
```bash
go build ./pilot/...
go build ./pkg/...
```

### 8.2 Invariant checks (manual code review)
- [ ] `a.localServices` is assigned from `LocalWorkloadServices` BEFORE any `krt.JoinCollection` with federation
- [ ] `a.localWorkloadsByServiceKey` indexes the output of `MergedGlobalWorkloadsCollection()` BEFORE any `krt.JoinCollection` with `fedWls`
- [ ] `FederationSource` is on `ambient.Options`, not on `multicluster.ControllerOptions`
- [ ] `ambient.Index` still embeds `model.FederationAmbientIndex`
- [ ] `federation.go`'s `AllLocalNetworkGlobalServicesWithSANs()` reads `a.localServices` (not `a.services.Collection`)
- [ ] `federation.go`'s `ServiceWithSANs()` reads `a.localWorkloadsByServiceKey` (not `a.workloads.ByServiceKey`)

### 8.3 Test execution
```bash
# Federation unit tests
go test -race ./pilot/pkg/serviceregistry/federation/...

# Ambient index tests (including multicluster)
go test -race ./pilot/pkg/serviceregistry/kube/controller/ambient/...

# Shared multicluster tests
go test -race ./pkg/kube/multicluster/...

# Bootstrap tests
go test -race ./pilot/pkg/bootstrap/...

# Integration
go test -race ./tests/integration/ambient/...
```

### 8.4 Runtime verification
- Enable federation (`PILOT_ENABLE_FEDERATION=true`) with 2+ kind clusters
- Verify outbound sync: leader cluster publishes local services to Service Bus
- Verify inbound sync: follower cluster receives and materializes federation services/workloads
- Verify split-horizon: ztunnel receives correct address lists with network gateway tunneling
- Verify no data loop: published service data doesn't contain received federation data

---

## 9. Things That Do NOT Need to Change

These components are insulated from the upstream refactor:

- **Service Bus transport** (`servicebus.go`): Pure Azure SDK usage, no Istio multicluster dependencies
- **Sync protocol** (`sync.go`): Interacts with ambient via `FederationAmbientIndex` interface only
- **Federation store** (`store.go`): Pure data transformation (split-horizon workloads, VIP injection)
- **Wire types** (`types.go`): Serialization format, no structural dependencies
- **Version vectors** (`version.go`): Pure data structure
- **Leader election** (`bootstrap/federation.go`): Uses standard Istio leader election, no multicluster types
- **Federation source** (`federation_source.go`): Uses `krt.StaticCollection`, no multicluster types

---

## 10. Recommendations

1. **Use the fresh-branch approach** (Phase 1-8 above) rather than attempting a rebase of the existing `registry-svc-bus` branch. The upstream deletions are too large for git to merge cleanly.

2. **Keep federation as a clean layer.** The current design's strength is that federation plugs in via two simple mechanisms:
   - `FederationSource` on Options (dependency injection)
   - `krt.JoinCollection` at 2 specific points in the pipeline (services and workloads)
   
   This layered approach should be maintained even as upstream evolves.

3. **Watch for follow-up PRs.** The upstream PR description mentions: *"I think I'll have a follow-up PR for removing Ambient Index creation from within Kube Controller and just making it a separate part of server.go."* This would further change where `ambient.Options` is constructed, but would not affect the federation join points inside `buildGlobalCollections()`.

4. **Consider upstreaming FederationSource as a generic extension point.** The `StaticCollection` bridge pattern is reusable for any external service discovery system, not just Service Bus. If Istio ever wants a plugin model for service discovery, this is the pattern.

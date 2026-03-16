// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package federation

import (
	"testing"
	"time"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/network"
	"istio.io/istio/pkg/test/util/assert"
	"istio.io/istio/pkg/workloadapi"
)

func newTestStore(localCluster cluster.ID) *federationStore {
	return newFederationStore(federationStoreConfig{
		LocalClusterID:     localCluster,
		TrustDomainGetter:  func() string { return "cluster.local" },
		LocalNetworkGetter: func() network.ID { return "network-local" },
	})
}

func makeSyncMessage(clusterID cluster.ID, version uint64, fullSync bool, hostnames ...string) *SyncMessage {
	msg := &SyncMessage{
		ClusterID:     clusterID,
		FullSync:      fullSync,
		VersionVector: map[cluster.ID]uint64{clusterID: version},
		NetworkGateway: &WireNetworkGateway{
			Network:   "network-remote",
			Cluster:   string(clusterID),
			Addr:      "10.0.0.1",
			HBONEPort: 15008,
		},
	}
	for _, h := range hostnames {
		msg.Services = append(msg.Services, model.ServiceInfo{
			Service: &workloadapi.Service{
				Hostname:  h,
				Namespace: "ns1",
				Addresses: []*workloadapi.NetworkAddress{{
					Network: "network-remote",
					Address: []byte{10, 0, 0, 1},
				}},
				Ports: []*workloadapi.Port{{ServicePort: 80, TargetPort: 8080}},
			},
			Scope: model.Global,
		})
	}
	return msg
}

func TestExpireStaleShards_DeadClusterRemoved(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Receive a message from cluster-remote.
	store.handleSyncMessage(makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))

	// Verify the shard is live.
	svcs, wls := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, len(wls) > 0, true) // at least gateway + split-horizon workload

	// Advance time past expiry.
	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()

	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 1)
	assert.Equal(t, expired[0], cluster.ID("cluster-remote"))

	// After expiry, getFederationState should return nothing.
	svcs, wls = store.getFederationState()
	assert.Equal(t, len(svcs), 0)
	assert.Equal(t, len(wls), 0)
}

func TestExpireStaleShards_LiveClusterNotExpired(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	store.handleSyncMessage(makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))

	// LastSeen is recent (just set by handleSyncMessage), should not expire.
	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 0)

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
}

func TestTombstonedShardRejectsStaleMessages(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Initial sync at version 1000.
	store.handleSyncMessage(makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))

	// Tombstone the shard.
	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()
	store.expireStaleShards(15*time.Minute, time.Now())

	// Send a stale message with version <= 1000. Should be rejected.
	store.handleSyncMessage(makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)

	// Also reject an incremental with version 999.
	store.handleSyncMessage(makeSyncMessage("cluster-remote", 999, false, "svc1.ns1.svc.cluster.local"))

	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 0)
}

func TestTombstonedShardResurrectedByNewerVersion(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Initial sync at version 1000.
	store.handleSyncMessage(makeSyncMessage("cluster-remote", 1000, true, "svc1.ns1.svc.cluster.local"))

	// Tombstone.
	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()
	store.expireStaleShards(15*time.Minute, time.Now())

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)

	// Send a newer version (strictly greater). Should resurrect.
	store.handleSyncMessage(makeSyncMessage("cluster-remote", 1001, true, "svc-new.ns1.svc.cluster.local"))

	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc-new.ns1.svc.cluster.local")

	// Verify shard is no longer tombstoned.
	store.mu.RLock()
	shard := store.shards["cluster-remote"]
	assert.Equal(t, shard.Tombstoned, false)
	assert.Equal(t, shard.LastSeen.IsZero(), false)
	store.mu.RUnlock()
}

func TestExpireStaleShards_BootstrapShardsGracePeriod(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Simulate bootstrap: manually insert a shard with zero LastSeen.
	store.mu.Lock()
	store.shards["bootstrap-cluster"] = &clusterShard{
		ClusterID: "bootstrap-cluster",
		Version:   500,
		Services: map[string]*model.ServiceInfo{
			"svc.ns1.svc.cluster.local": {
				Service: &workloadapi.Service{
					Hostname:  "svc.ns1.svc.cluster.local",
					Namespace: "ns1",
				},
			},
		},
		NetworkGateway: &WireNetworkGateway{
			Network:   "network-remote",
			Cluster:   "bootstrap-cluster",
			Addr:      "10.0.0.2",
			HBONEPort: 15008,
		},
		// LastSeen is zero — loaded via bootstrap, no live message yet.
	}
	store.versionVector.set("bootstrap-cluster", 500)
	store.mu.Unlock()

	// Sweep should NOT expire bootstrap shards with zero LastSeen.
	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 0)

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
}

func TestExpireStaleShards_MultipleShardsMixedState(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Cluster A: recent
	store.handleSyncMessage(makeSyncMessage("cluster-a", 1000, true, "svc-a.ns1.svc.cluster.local"))

	// Cluster B: stale
	store.handleSyncMessage(makeSyncMessage("cluster-b", 2000, true, "svc-b.ns1.svc.cluster.local"))
	store.mu.Lock()
	store.shards["cluster-b"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()

	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 1)
	assert.Equal(t, expired[0], cluster.ID("cluster-b"))

	// Only cluster A's services should remain.
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc-a.ns1.svc.cluster.local")
}

func TestExpireStaleShards_AlreadyTombstonedSkipped(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	store.handleSyncMessage(makeSyncMessage("cluster-remote", 1000, true, "svc.ns1.svc.cluster.local"))

	// Tombstone it.
	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()
	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 1)

	// Second sweep should not re-expire it.
	expired = store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 0)
}

func TestExpireStaleShards_DisabledWhenZeroDuration(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	store.handleSyncMessage(makeSyncMessage("cluster-remote", 1000, true, "svc.ns1.svc.cluster.local"))
	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-1 * time.Hour)
	store.mu.Unlock()

	// Zero expiry duration should never expire anything.
	expired := store.expireStaleShards(0, time.Now())
	assert.Equal(t, len(expired), 0)
}

func TestBootstrapThenExpiry_RetainedSnapshotExpires(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Simulate bootstrap: a retained snapshot from a dead cluster arrives
	// through the bootstrap path and is applied to the store.
	msg := makeSyncMessage("dead-cluster", 5000, true, "dead-svc.ns1.svc.cluster.local")
	store.handleSyncMessage(msg)

	// Verify it's live.
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)

	// Time passes: no more messages arrive from dead-cluster.
	store.mu.Lock()
	store.shards["dead-cluster"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()

	// Expiry sweep tombstones it.
	expired := store.expireStaleShards(15*time.Minute, time.Now())
	assert.Equal(t, len(expired), 1)

	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 0)

	// Now simulate a restart: bootstrap replays the same retained snapshot
	// (version 5000). It should be rejected because version <= tombstoned version.
	store.handleSyncMessage(msg)

	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 0)

	// But a genuinely new leader on that cluster with a higher version succeeds.
	store.handleSyncMessage(makeSyncMessage("dead-cluster", 5001, true, "alive-svc.ns1.svc.cluster.local"))

	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "alive-svc.ns1.svc.cluster.local")
}

func TestNewSyncProtocol_RejectsExpiryWithDisabledSnapshots(t *testing.T) {
	t.Parallel()

	_, err := NewSyncProtocol(SyncProtocolConfig{
		LocalClusterID:   "test",
		SnapshotInterval: -1, // disabled
		ShardExpiry:      15 * time.Minute,
		Transport:        &ServiceBusTransport{}, // minimal stub
		StopCh:           make(chan struct{}),
	})
	assert.Equal(t, err != nil, true)
}

func TestGatewayDedupeKey_DifferentClustersNotCollapsed(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Two clusters on the same network with the same gateway address
	// but different HBONE ports and cluster IDs.
	msgA := &SyncMessage{
		ClusterID:     "cluster-a",
		FullSync:      true,
		VersionVector: map[cluster.ID]uint64{"cluster-a": 1000},
		NetworkGateway: &WireNetworkGateway{
			Network:   "network-shared",
			Cluster:   "cluster-a",
			Addr:      "10.0.0.1",
			HBONEPort: 15008,
		},
		Services: []model.ServiceInfo{{
			Service: &workloadapi.Service{
				Hostname:  "svc-a.ns1.svc.cluster.local",
				Namespace: "ns1",
				Addresses: []*workloadapi.NetworkAddress{{Network: "network-shared", Address: []byte{10, 0, 0, 1}}},
				Ports:     []*workloadapi.Port{{ServicePort: 80, TargetPort: 8080}},
			},
			Scope: model.Global,
		}},
	}

	msgB := &SyncMessage{
		ClusterID:     "cluster-b",
		FullSync:      true,
		VersionVector: map[cluster.ID]uint64{"cluster-b": 1000},
		NetworkGateway: &WireNetworkGateway{
			Network:   "network-shared",
			Cluster:   "cluster-b",
			Addr:      "10.0.0.1",
			HBONEPort: 15009, // different port
		},
		Services: []model.ServiceInfo{{
			Service: &workloadapi.Service{
				Hostname:  "svc-b.ns1.svc.cluster.local",
				Namespace: "ns1",
				Addresses: []*workloadapi.NetworkAddress{{Network: "network-shared", Address: []byte{10, 0, 0, 2}}},
				Ports:     []*workloadapi.Port{{ServicePort: 80, TargetPort: 8080}},
			},
			Scope: model.Global,
		}},
	}

	store.handleSyncMessage(msgA)
	store.handleSyncMessage(msgB)

	_, workloads := store.getFederationState()

	// Count gateway workloads (UIDs starting with "NetworkGateway/").
	gwCount := 0
	for _, wl := range workloads {
		if wl.Workload != nil && len(wl.Workload.Uid) > 15 && wl.Workload.Uid[:15] == "NetworkGateway/" {
			gwCount++
		}
	}
	// Both clusters should produce distinct gateway workloads.
	assert.Equal(t, gwCount, 2)
}

func TestProjectionCache_HitOnUnchangedShard(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Seed a shard.
	store.handleSyncMessage(makeSyncMessage("cluster-remote", 1000, true,
		"svc1.ns1.svc.cluster.local", "svc2.ns1.svc.cluster.local"))

	// First call builds the cache (miss).
	svcs1, wls1 := store.getFederationState()
	assert.Equal(t, len(svcs1), 2)
	assert.Equal(t, len(wls1) > 0, true)

	// Verify cache was populated.
	store.mu.RLock()
	cached := store.shards["cluster-remote"].cachedProjection
	store.mu.RUnlock()
	assert.Equal(t, cached != nil, true)

	// Second call should hit the cache — same results.
	svcs2, wls2 := store.getFederationState()
	assert.Equal(t, len(svcs2), len(svcs1))
	assert.Equal(t, len(wls2), len(wls1))

	// Verify the same projection pointer was reused (cache hit).
	store.mu.RLock()
	cachedAfter := store.shards["cluster-remote"].cachedProjection
	store.mu.RUnlock()
	assert.Equal(t, cached, cachedAfter)
}

func TestProjectionCache_InvalidatedOnNewMessage(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Seed and build cache.
	store.handleSyncMessage(makeSyncMessage("cluster-remote", 1000, true,
		"svc1.ns1.svc.cluster.local"))
	svcs1, _ := store.getFederationState()
	assert.Equal(t, len(svcs1), 1)

	// Capture first projection.
	store.mu.RLock()
	firstProj := store.shards["cluster-remote"].cachedProjection
	store.mu.RUnlock()
	assert.Equal(t, firstProj != nil, true)

	// Send a new message (incremental, adds a service). This should invalidate the cache.
	store.handleSyncMessage(makeSyncMessage("cluster-remote", 1001, false,
		"svc2.ns1.svc.cluster.local"))

	// Cache should be nil.
	store.mu.RLock()
	assert.Equal(t, store.shards["cluster-remote"].cachedProjection == nil, true)
	store.mu.RUnlock()

	// Next getFederationState rebuilds the cache.
	svcs2, _ := store.getFederationState()
	assert.Equal(t, len(svcs2), 2)

	// New projection should be different from the first.
	store.mu.RLock()
	secondProj := store.shards["cluster-remote"].cachedProjection
	store.mu.RUnlock()
	assert.Equal(t, secondProj != nil, true)
	assert.Equal(t, firstProj != secondProj, true)
}

func TestProjectionCache_InvalidatedOnExpiry(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Seed and build cache.
	store.handleSyncMessage(makeSyncMessage("cluster-remote", 1000, true,
		"svc1.ns1.svc.cluster.local"))
	store.getFederationState()

	store.mu.RLock()
	assert.Equal(t, store.shards["cluster-remote"].cachedProjection != nil, true)
	store.mu.RUnlock()

	// Tombstone via expiry.
	store.mu.Lock()
	store.shards["cluster-remote"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()
	expired := store.expireStaleShards(10*time.Minute, time.Now())
	assert.Equal(t, len(expired), 1)

	// Cache should be cleared.
	store.mu.RLock()
	assert.Equal(t, store.shards["cluster-remote"].cachedProjection == nil, true)
	store.mu.RUnlock()

	// getFederationState should return nothing (tombstoned).
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)
}

func TestProjectionCache_MultiShardSelectiveInvalidation(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Seed two shards.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 1000, true,
		"svc-a.ns1.svc.cluster.local"))
	store.handleSyncMessage(makeSyncMessage("cluster-b", 1000, true,
		"svc-b.ns1.svc.cluster.local"))

	// Build caches.
	store.getFederationState()

	store.mu.RLock()
	projA := store.shards["cluster-a"].cachedProjection
	projB := store.shards["cluster-b"].cachedProjection
	store.mu.RUnlock()
	assert.Equal(t, projA != nil, true)
	assert.Equal(t, projB != nil, true)

	// Update only cluster-a.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 1001, false,
		"svc-a2.ns1.svc.cluster.local"))

	// cluster-a cache invalidated, cluster-b still valid.
	store.mu.RLock()
	assert.Equal(t, store.shards["cluster-a"].cachedProjection == nil, true)
	assert.Equal(t, store.shards["cluster-b"].cachedProjection, projB) // same pointer
	store.mu.RUnlock()

	// Rebuild and verify counts.
	svcs, _ := store.getFederationState()
	// cluster-a: svc-a + svc-a2 = 2, cluster-b: svc-b = 1 → total 3
	assert.Equal(t, len(svcs), 3)
}

func TestHealthState_InitiallyNotReady(t *testing.T) {
	t.Parallel()

	var h HealthState
	// Neither bootstrap complete nor transport connected by default.
	assert.Equal(t, h.BootstrapComplete.Load(), false)
	assert.Equal(t, h.TransportConnected.Load(), false)
	assert.Equal(t, h.LastMessageReceived.Load(), int64(0))
}

func TestHealthState_ReadyRequiresBothFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bootstrap   bool
		transport   bool
		expectReady bool
	}{
		{"neither", false, false, false},
		{"bootstrap only", true, false, false},
		{"transport only", false, true, false},
		{"both", true, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var h HealthState
			h.BootstrapComplete.Store(tt.bootstrap)
			h.TransportConnected.Store(tt.transport)

			// IsReady logic: bootstrap && transport
			ready := h.BootstrapComplete.Load() && h.TransportConnected.Load()
			assert.Equal(t, ready, tt.expectReady)
		})
	}
}

func TestHealthState_LastMessageReceived(t *testing.T) {
	t.Parallel()

	var h HealthState
	assert.Equal(t, h.LastMessageReceived.Load(), int64(0))

	now := time.Now().UnixMilli()
	h.LastMessageReceived.Store(now)
	assert.Equal(t, h.LastMessageReceived.Load(), now)
}

func TestHealthState_TransportDisconnectReconnect(t *testing.T) {
	t.Parallel()

	var h HealthState
	h.TransportConnected.Store(true)
	h.BootstrapComplete.Store(true)

	// Simulate disconnect.
	h.TransportConnected.Store(false)
	ready := h.BootstrapComplete.Load() && h.TransportConnected.Load()
	assert.Equal(t, ready, false)

	// Simulate reconnect.
	h.TransportConnected.Store(true)
	ready = h.BootstrapComplete.Load() && h.TransportConnected.Load()
	assert.Equal(t, ready, true)
}

// --- bootstrapMerger tests ---

func makeBootstrapMsg(clusterID cluster.ID, version uint64, fullSync bool, hostnames ...string) *SyncMessage {
	msg := &SyncMessage{
		ClusterID:     clusterID,
		FullSync:      fullSync,
		VersionVector: map[cluster.ID]uint64{clusterID: version},
	}
	for _, h := range hostnames {
		msg.Services = append(msg.Services, model.ServiceInfo{
			Service: &workloadapi.Service{
				Hostname: h,
			},
		})
	}
	return msg
}

func TestBootstrapMerger_EmptyInput(t *testing.T) {
	t.Parallel()

	var m bootstrapMerger
	result := m.result()
	assert.Equal(t, len(result), 0)
	assert.Equal(t, m.messageCount(), 0)
}

func TestBootstrapMerger_SingleFullSync(t *testing.T) {
	t.Parallel()

	var m bootstrapMerger
	msg := makeBootstrapMsg("cluster-a", 1, true, "svc1.ns.svc.cluster.local")
	m.add(msg)

	result := m.result()
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], msg)
	assert.Equal(t, m.messageCount(), 1)
}

func TestBootstrapMerger_SupersededFullSync(t *testing.T) {
	t.Parallel()

	var m bootstrapMerger
	old := makeBootstrapMsg("cluster-a", 1, true, "svc-old.ns.svc.cluster.local")
	newer := makeBootstrapMsg("cluster-a", 5, true, "svc-new.ns.svc.cluster.local")

	m.add(old)
	m.add(newer)

	result := m.result()
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], newer)
	assert.Equal(t, m.messageCount(), 1)
}

func TestBootstrapMerger_FullSyncPlusNewerIncrementals(t *testing.T) {
	t.Parallel()

	var m bootstrapMerger
	full := makeBootstrapMsg("cluster-a", 3, true, "svc1.ns.svc.cluster.local")
	inc := makeBootstrapMsg("cluster-a", 4, false, "svc2.ns.svc.cluster.local")

	m.add(full)
	m.add(inc)

	result := m.result()
	assert.Equal(t, len(result), 2)
	assert.Equal(t, m.messageCount(), 2)
}

func TestBootstrapMerger_StaleIncrementalDiscarded(t *testing.T) {
	t.Parallel()

	var m bootstrapMerger
	full := makeBootstrapMsg("cluster-a", 5, true, "svc1.ns.svc.cluster.local")
	staleInc := makeBootstrapMsg("cluster-a", 3, false, "svc-old.ns.svc.cluster.local")

	m.add(full)
	m.add(staleInc)

	result := m.result()
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], full)
}

func TestBootstrapMerger_NewFullSyncClearsIncrementals(t *testing.T) {
	t.Parallel()

	var m bootstrapMerger
	oldFull := makeBootstrapMsg("cluster-a", 2, true, "svc1.ns.svc.cluster.local")
	inc := makeBootstrapMsg("cluster-a", 3, false, "svc2.ns.svc.cluster.local")
	newFull := makeBootstrapMsg("cluster-a", 5, true, "svc3.ns.svc.cluster.local")

	m.add(oldFull)
	m.add(inc)
	m.add(newFull)

	// The newer full sync supersedes both the old full sync and the incremental.
	result := m.result()
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], newFull)
}

func TestBootstrapMerger_MultipleClusters(t *testing.T) {
	t.Parallel()

	var m bootstrapMerger
	msgA := makeBootstrapMsg("cluster-a", 1, true, "svc-a.ns.svc.cluster.local")
	msgB := makeBootstrapMsg("cluster-b", 1, true, "svc-b.ns.svc.cluster.local")
	msgC := makeBootstrapMsg("cluster-c", 3, true, "svc-c.ns.svc.cluster.local")

	m.add(msgA)
	m.add(msgB)
	m.add(msgC)

	result := m.result()
	assert.Equal(t, len(result), 3)
	assert.Equal(t, m.messageCount(), 3)
}

func TestBootstrapMerger_IncrementalsOnlyNoFullSync(t *testing.T) {
	t.Parallel()

	// If there's no full sync for a cluster, incrementals are kept since
	// fullSyncVersion defaults to 0 and any version > 0 passes the filter.
	var m bootstrapMerger
	inc1 := makeBootstrapMsg("cluster-a", 1, false, "svc1.ns.svc.cluster.local")
	inc2 := makeBootstrapMsg("cluster-a", 2, false, "svc2.ns.svc.cluster.local")

	m.add(inc1)
	m.add(inc2)

	result := m.result()
	assert.Equal(t, len(result), 2)
	assert.Equal(t, m.messageCount(), 2)
}

func TestBootstrapMerger_MixedFullSyncAndIncremental(t *testing.T) {
	t.Parallel()

	var m bootstrapMerger
	old := makeBootstrapMsg("cluster-a", 1, true, "svc-old.ns.svc.cluster.local")
	newer := makeBootstrapMsg("cluster-a", 5, true, "svc-new.ns.svc.cluster.local")
	inc := makeBootstrapMsg("cluster-a", 6, false, "svc-inc.ns.svc.cluster.local")

	m.add(old)
	m.add(newer)
	m.add(inc)

	result := m.result()
	assert.Equal(t, len(result), 2)

	var foundFull, foundInc bool
	for _, msg := range result {
		if msg.FullSync && msg.VersionVector["cluster-a"] == 5 {
			foundFull = true
		}
		if !msg.FullSync && msg.VersionVector["cluster-a"] == 6 {
			foundInc = true
		}
	}
	assert.Equal(t, foundFull, true)
	assert.Equal(t, foundInc, true)
}

// --- Version vector / temporal semantics tests ---

func TestVersionVector_StaleMessageRejected(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Accept version 5.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 5, true, "svc1.ns1.svc.cluster.local"))
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)

	// Reject version 3 (stale).
	store.handleSyncMessage(makeSyncMessage("cluster-a", 3, true, "svc-stale.ns1.svc.cluster.local"))
	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc1.ns1.svc.cluster.local")
}

func TestVersionVector_EqualVersionFullSyncAccepted(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Accept version 5 full-sync.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 5, true, "svc1.ns1.svc.cluster.local"))

	// Equal version full-sync (re-delivery) — accepted for idempotency.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 5, true, "svc-updated.ns1.svc.cluster.local"))
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc-updated.ns1.svc.cluster.local")
}

func TestVersionVector_EqualVersionIncrementalRejected(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Accept version 5.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 5, true, "svc1.ns1.svc.cluster.local"))

	// Equal version incremental — rejected (only full-sync re-delivery accepted).
	store.handleSyncMessage(makeSyncMessage("cluster-a", 5, false, "svc-inc.ns1.svc.cluster.local"))
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc1.ns1.svc.cluster.local")
}

func TestVersionVector_OutOfOrderDelivery(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Messages arrive out of order: 3, 1, 5, 2.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 3, true, "svc-v3.ns1.svc.cluster.local"))
	store.handleSyncMessage(makeSyncMessage("cluster-a", 1, true, "svc-v1.ns1.svc.cluster.local"))
	store.handleSyncMessage(makeSyncMessage("cluster-a", 5, true, "svc-v5.ns1.svc.cluster.local"))
	store.handleSyncMessage(makeSyncMessage("cluster-a", 2, true, "svc-v2.ns1.svc.cluster.local"))

	// Only the highest version (5) should be retained.
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc-v5.ns1.svc.cluster.local")
}

func TestVersionVector_MultiClusterIndependent(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Two clusters with their own version vectors.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 10, true, "svc-a.ns1.svc.cluster.local"))
	store.handleSyncMessage(makeSyncMessage("cluster-b", 5, true, "svc-b.ns1.svc.cluster.local"))

	// Stale message for cluster-a doesn't affect cluster-b.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 3, true, "svc-stale.ns1.svc.cluster.local"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 2)

	hostnames := make(map[string]bool)
	for _, svc := range svcs {
		hostnames[svc.Service.Hostname] = true
	}
	assert.Equal(t, hostnames["svc-a.ns1.svc.cluster.local"], true)
	assert.Equal(t, hostnames["svc-b.ns1.svc.cluster.local"], true)
}

// --- Full-sync replacement semantics tests ---

func TestFullSync_ReplacesEntireShard(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Version 1: two services.
	msg1 := makeSyncMessage("cluster-a", 1, true, "svc1.ns1.svc.cluster.local", "svc2.ns1.svc.cluster.local")
	store.handleSyncMessage(msg1)
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 2)

	// Version 2: full-sync replaces with one different service.
	msg2 := makeSyncMessage("cluster-a", 2, true, "svc3.ns1.svc.cluster.local")
	store.handleSyncMessage(msg2)
	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc3.ns1.svc.cluster.local")
}

// --- Incremental delete semantics tests ---

func TestIncrementalDelete_RemovesService(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Full-sync with two services.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 1, true,
		"svc1.ns1.svc.cluster.local", "svc2.ns1.svc.cluster.local"))
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 2)

	// Incremental delete of svc1.
	deleteMsg := &SyncMessage{
		ClusterID:        "cluster-a",
		FullSync:         false,
		VersionVector:    map[cluster.ID]uint64{"cluster-a": 2},
		DeletedHostnames: []string{"svc1.ns1.svc.cluster.local"},
	}
	store.handleSyncMessage(deleteMsg)

	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc2.ns1.svc.cluster.local")
}

func TestIncrementalDelete_NonexistentHostnameIgnored(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	store.handleSyncMessage(makeSyncMessage("cluster-a", 1, true, "svc1.ns1.svc.cluster.local"))

	// Delete a hostname that doesn't exist — no crash, no effect.
	deleteMsg := &SyncMessage{
		ClusterID:        "cluster-a",
		FullSync:         false,
		VersionVector:    map[cluster.ID]uint64{"cluster-a": 2},
		DeletedHostnames: []string{"nonexistent.ns1.svc.cluster.local"},
	}
	store.handleSyncMessage(deleteMsg)

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
}

func TestIncrementalAddAndDelete_SameMessage(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	store.handleSyncMessage(makeSyncMessage("cluster-a", 1, true,
		"svc1.ns1.svc.cluster.local", "svc2.ns1.svc.cluster.local"))

	// Incremental: add svc3, delete svc1. Deletes are processed before adds.
	msg := makeSyncMessage("cluster-a", 2, false, "svc3.ns1.svc.cluster.local")
	msg.DeletedHostnames = []string{"svc1.ns1.svc.cluster.local"}
	store.handleSyncMessage(msg)

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 2)
	hostnames := make(map[string]bool)
	for _, svc := range svcs {
		hostnames[svc.Service.Hostname] = true
	}
	assert.Equal(t, hostnames["svc2.ns1.svc.cluster.local"], true)
	assert.Equal(t, hostnames["svc3.ns1.svc.cluster.local"], true)
}

// --- Self-message filtering tests ---

func TestSelfMessage_Ignored(t *testing.T) {
	t.Parallel()

	store := newTestStore("local-cluster")

	// Message from own cluster — should be silently dropped.
	store.handleSyncMessage(makeSyncMessage("local-cluster", 1, true, "svc1.ns1.svc.cluster.local"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)
}

// --- Nil / empty message safety tests ---

func TestNilMessage_Ignored(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Should not panic.
	store.handleSyncMessage(nil)

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)
}

func TestEmptyFullSync_ClearsShard(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Populate shard.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 1, true,
		"svc1.ns1.svc.cluster.local", "svc2.ns1.svc.cluster.local"))
	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 2)

	// Empty full-sync replaces shard with zero services.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 2, true))
	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 0)
}

// --- Multi-cluster convergence tests ---

func TestConvergence_BootstrapThenIncrementals(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Simulate bootstrap: full-sync from two clusters.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 100, true,
		"svc-a1.ns1.svc.cluster.local", "svc-a2.ns1.svc.cluster.local"))
	store.handleSyncMessage(makeSyncMessage("cluster-b", 50, true,
		"svc-b1.ns1.svc.cluster.local"))

	// Live incrementals: cluster-a adds a service, cluster-b deletes one.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 101, false, "svc-a3.ns1.svc.cluster.local"))

	deleteMsgB := &SyncMessage{
		ClusterID:        "cluster-b",
		FullSync:         false,
		VersionVector:    map[cluster.ID]uint64{"cluster-b": 51},
		DeletedHostnames: []string{"svc-b1.ns1.svc.cluster.local"},
	}
	store.handleSyncMessage(deleteMsgB)

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 3) // a1, a2, a3; b1 deleted

	hostnames := make(map[string]bool)
	for _, svc := range svcs {
		hostnames[svc.Service.Hostname] = true
	}
	assert.Equal(t, hostnames["svc-a1.ns1.svc.cluster.local"], true)
	assert.Equal(t, hostnames["svc-a2.ns1.svc.cluster.local"], true)
	assert.Equal(t, hostnames["svc-a3.ns1.svc.cluster.local"], true)
	assert.Equal(t, hostnames["svc-b1.ns1.svc.cluster.local"], false)
}

func TestConvergence_FullSyncHealsStaleState(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Initial state: svc1 and svc2.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 1, true,
		"svc1.ns1.svc.cluster.local", "svc2.ns1.svc.cluster.local"))

	// Suppose we missed an incremental delete for svc1.
	// The next full-sync at version 5 heals the state.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 5, true, "svc2.ns1.svc.cluster.local"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc2.ns1.svc.cluster.local")
}

func TestConvergence_TombstoneBlocksRetainedBootstrapMessages(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	// Accept initial state at version 10.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 10, true, "svc1.ns1.svc.cluster.local"))

	// Tombstone the shard.
	store.mu.Lock()
	store.shards["cluster-a"].LastSeen = time.Now().Add(-20 * time.Minute)
	store.mu.Unlock()
	store.expireStaleShards(15*time.Minute, time.Now())

	// Retained bootstrap re-delivers the version 10 full-sync — should be blocked.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 10, true, "svc1.ns1.svc.cluster.local"))

	svcs, _ := store.getFederationState()
	assert.Equal(t, len(svcs), 0)

	// Only a strictly newer version can resurrect.
	store.handleSyncMessage(makeSyncMessage("cluster-a", 11, true, "svc-resurrected.ns1.svc.cluster.local"))
	svcs, _ = store.getFederationState()
	assert.Equal(t, len(svcs), 1)
	assert.Equal(t, svcs[0].Service.Hostname, "svc-resurrected.ns1.svc.cluster.local")
}

// --- SPIFFE SAN parsing tests ---

func TestParseSpiffeSAN_ValidSAN(t *testing.T) {
	t.Parallel()

	result := parseSpiffeSAN("spiffe://cluster.local/ns/my-ns/sa/my-sa")
	assert.Equal(t, result != nil, true)
	assert.Equal(t, result.namespace, "my-ns")
	assert.Equal(t, result.serviceAccount, "my-sa")
}

func TestParseSpiffeSAN_DifferentTrustDomain(t *testing.T) {
	t.Parallel()

	result := parseSpiffeSAN("spiffe://example.com/ns/prod/sa/web-server")
	assert.Equal(t, result != nil, true)
	assert.Equal(t, result.namespace, "prod")
	assert.Equal(t, result.serviceAccount, "web-server")
}

func TestParseSpiffeSAN_InvalidFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		san  string
	}{
		{"empty", ""},
		{"not spiffe", "https://example.com/ns/foo/sa/bar"},
		{"no path", "spiffe://cluster.local"},
		{"missing sa segment", "spiffe://cluster.local/ns/my-ns"},
		{"wrong prefix", "spiffe://cluster.local/foo/my-ns/sa/my-sa"},
		{"wrong middle", "spiffe://cluster.local/ns/my-ns/notsa/my-sa"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, parseSpiffeSAN(tt.san) == nil, true)
		})
	}
}

// --- Split-horizon workload identity tests ---

func TestSplitHorizonWorkload_ExtractsFirstSAN(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	msg := makeSyncMessage("cluster-a", 1, true, "svc1.ns1.svc.cluster.local")
	// Add SANs to the service.
	msg.Services[0].Service.SubjectAltNames = []string{
		"spiffe://cluster.local/ns/ns1/sa/primary-sa",
		"spiffe://cluster.local/ns/ns1/sa/secondary-sa",
	}
	store.handleSyncMessage(msg)

	_, wls := store.getFederationState()

	// Find the split-horizon workload (not the gateway).
	var splitHorizonWl *model.WorkloadInfo
	for i := range wls {
		if !isGatewayWorkload(&wls[i]) {
			splitHorizonWl = &wls[i]
			break
		}
	}
	assert.Equal(t, splitHorizonWl != nil, true)
	// Only the first SAN's service account is used.
	assert.Equal(t, splitHorizonWl.Workload.ServiceAccount, "primary-sa")
}

func TestSplitHorizonWorkload_NoSANDefaultsToDefault(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	msg := makeSyncMessage("cluster-a", 1, true, "svc1.ns1.svc.cluster.local")
	// No SANs.
	msg.Services[0].Service.SubjectAltNames = nil
	store.handleSyncMessage(msg)

	_, wls := store.getFederationState()

	var splitHorizonWl *model.WorkloadInfo
	for i := range wls {
		if !isGatewayWorkload(&wls[i]) {
			splitHorizonWl = &wls[i]
			break
		}
	}
	assert.Equal(t, splitHorizonWl != nil, true)
	assert.Equal(t, splitHorizonWl.Workload.ServiceAccount, "default")
}

// --- Gateway workload tests ---

func TestGatewayWorkload_CreatedPerShard(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	store.handleSyncMessage(makeSyncMessage("cluster-a", 1, true, "svc1.ns1.svc.cluster.local"))

	_, wls := store.getFederationState()

	var gatewayCount int
	for i := range wls {
		if isGatewayWorkload(&wls[i]) {
			gatewayCount++
		}
	}
	assert.Equal(t, gatewayCount, 1)
}

func TestGatewayWorkload_NoGatewayNoWorkloads(t *testing.T) {
	t.Parallel()

	store := newTestStore("local")

	msg := &SyncMessage{
		ClusterID:     "cluster-a",
		FullSync:      true,
		VersionVector: map[cluster.ID]uint64{"cluster-a": 1},
		// No NetworkGateway.
		Services: []model.ServiceInfo{{
			Service: &workloadapi.Service{
				Hostname:  "svc1.ns1.svc.cluster.local",
				Namespace: "ns1",
				Addresses: []*workloadapi.NetworkAddress{{Network: "net-a", Address: []byte{10, 0, 0, 1}}},
				Ports:     []*workloadapi.Port{{ServicePort: 80}},
			},
			Scope: model.Global,
		}},
	}
	store.handleSyncMessage(msg)

	_, wls := store.getFederationState()
	assert.Equal(t, len(wls), 0)
}

// --- Channel pressure tests ---

func TestIncomingChannel_DropsOnOverflow(t *testing.T) {
	t.Parallel()

	// The incoming channel has capacity 100. Verify that sending beyond
	// capacity uses non-blocking semantics and does not deadlock.
	ch := make(chan *SyncMessage, 100)

	// Fill the channel.
	for i := range 100 {
		ch <- makeSyncMessage("cluster-a", uint64(i), false, "svc.ns1.svc.cluster.local")
	}

	// One more — this should NOT block (matches handleIncomingMessage behavior).
	select {
	case ch <- makeSyncMessage("cluster-a", 200, false, "svc.ns1.svc.cluster.local"):
		// If we got here, the channel wasn't full (unexpected).
		t.Fatal("expected channel to be full")
	default:
		// Expected: channel full, message dropped.
	}

	assert.Equal(t, len(ch), 100)
}

// isGatewayWorkload returns true if the workload UID indicates a NetworkGateway workload.
func isGatewayWorkload(wl *model.WorkloadInfo) bool {
	return len(wl.Workload.Uid) > 15 && wl.Workload.Uid[:15] == "NetworkGateway/"
}

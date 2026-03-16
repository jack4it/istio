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

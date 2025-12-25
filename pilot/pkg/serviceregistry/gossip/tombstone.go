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

package gossip

import (
	"sync"
	"time"

	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/log"
)

const (
	// tombstoneTTL is the duration after which tombstones are pruned.
	tombstoneTTL = 5 * time.Minute

	// tombstonePruneInterval is how often the background pruning runs.
	tombstonePruneInterval = 1 * time.Minute
)

var tombstoneLog = log.RegisterScope("gossip-tombstone", "Gossip tombstone manager")

// tombstoneStore manages deletion markers to prevent resurrection of deleted
// resources from stale peers.
type tombstoneStore struct {
	mu         sync.RWMutex
	tombstones map[string]Tombstone
	stopCh     chan struct{}
}

// newTombstoneStore creates a new tombstone store and starts background pruning.
func newTombstoneStore(stopCh chan struct{}) *tombstoneStore {
	ts := &tombstoneStore{
		tombstones: make(map[string]Tombstone),
		stopCh:     stopCh,
	}
	go ts.pruneLoop()
	return ts
}

// Add records a tombstone for the given service key (hostname).
func (ts *tombstoneStore) add(key string, clusterID cluster.ID, version uint64) Tombstone {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	tombstone := Tombstone{
		Key:           key,
		DeletedAtUnix: time.Now().Unix(),
		Version:       version,
		ClusterID:     clusterID,
	}
	ts.tombstones[tombstoneKey(key, clusterID)] = tombstone
	tombstoneLog.Debugf("Added tombstone for %s from cluster %s at version %d", key, clusterID, version)
	return tombstone
}

// IsDeleted checks if a resource is tombstoned and if the tombstone version
// is greater than or equal to the given version.
func (ts *tombstoneStore) isDeleted(key string, clusterID cluster.ID, version uint64) bool {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	tombstone, ok := ts.tombstones[tombstoneKey(key, clusterID)]
	if !ok {
		return false
	}
	return tombstone.Version >= version
}

// AddFromRemote adds a tombstone received from a remote peer.
func (ts *tombstoneStore) addFromRemote(tombstone Tombstone) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	key := tombstoneKey(tombstone.Key, tombstone.ClusterID)
	existing, ok := ts.tombstones[key]
	// Only update if the remote tombstone is newer
	if !ok || tombstone.Version > existing.Version {
		ts.tombstones[key] = tombstone
		tombstoneLog.Debugf("Added remote tombstone for %s from cluster %s at version %d",
			tombstone.Key, tombstone.ClusterID, tombstone.Version)
	}
}

// GetAll returns all current tombstones.
func (ts *tombstoneStore) getAll() []Tombstone {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	result := make([]Tombstone, 0, len(ts.tombstones))
	for _, t := range ts.tombstones {
		result = append(result, t)
	}
	return result
}

// pruneLoop periodically removes expired tombstones.
func (ts *tombstoneStore) pruneLoop() {
	ticker := time.NewTicker(tombstonePruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ts.prune()
		case <-ts.stopCh:
			return
		}
	}
}

// prune removes tombstones older than tombstoneTTL.
func (ts *tombstoneStore) prune() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	cutoff := time.Now().Add(-tombstoneTTL).Unix()
	pruned := 0
	for key, tombstone := range ts.tombstones {
		if tombstone.DeletedAtUnix < cutoff {
			delete(ts.tombstones, key)
			pruned++
		}
	}
	if pruned > 0 {
		tombstoneLog.Debugf("Pruned %d expired tombstones", pruned)
	}
}

// tombstoneKey generates a unique key for a tombstone combining resource key and cluster.
func tombstoneKey(resourceKey string, clusterID cluster.ID) string {
	return string(clusterID) + "/" + resourceKey
}

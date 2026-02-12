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
	"sync"
	"time"

	"istio.io/istio/pkg/cluster"
)

// versionVector implements a vector clock for eventual consistency.
// It tracks versions per cluster to enable conflict-free merging of updates.
type versionVector struct {
	mu       sync.RWMutex
	versions map[cluster.ID]uint64
}

// newVersionVector creates a new empty version vector.
func newVersionVector() *versionVector {
	return &versionVector{
		versions: make(map[cluster.ID]uint64),
	}
}

// Increment increases the version for the given cluster and returns the new version.
// Uses max(current+1, currentTimeMillis) to ensure versions are always higher
// after leader transitions across Istiod replicas. Within a single leader's
// lifetime, +1 handles rapid-fire calls in the same millisecond. Across leaders,
// the time component guarantees the new leader's versions exceed the old leader's
// counter — since UnixMilli in 2026 (~1.77×10¹²) dwarfs any counter value.
func (v *versionVector) increment(clusterID cluster.ID) uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	next := v.versions[clusterID] + 1
	now := uint64(time.Now().UnixMilli())
	if now > next {
		next = now
	}
	v.versions[clusterID] = next
	return next
}

// Get returns the current version for the given cluster.
func (v *versionVector) get(clusterID cluster.ID) uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.versions[clusterID]
}

// Set sets the version for the given cluster.
func (v *versionVector) set(clusterID cluster.ID, version uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.versions[clusterID] = version
}

// Copy returns a copy of the version vector as a map.
func (v *versionVector) copy() map[cluster.ID]uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	result := make(map[cluster.ID]uint64, len(v.versions))
	for k, val := range v.versions {
		result[k] = val
	}
	return result
}

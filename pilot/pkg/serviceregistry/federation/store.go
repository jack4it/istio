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
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"istio.io/istio/pilot/pkg/model"
	labelutil "istio.io/istio/pilot/pkg/serviceregistry/util/label"
	"istio.io/istio/pilot/pkg/util/protoconv"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/config/schema/kind"
	"istio.io/istio/pkg/log"
	"istio.io/istio/pkg/network"
	"istio.io/istio/pkg/util/protomarshal"
	"istio.io/istio/pkg/workloadapi"
	"k8s.io/apimachinery/pkg/types"
)

var storeLog = log.RegisterScope("federation-store", "Federation data store")

// federationStore is an internal data store for federation-synced services.
// It stores the synchronized state from remote clusters and provides
// methods to extract data for KRT collections.
type federationStore struct {
	mu sync.RWMutex

	// shards contains the synchronized state from remote clusters.
	shards map[cluster.ID]*clusterShard

	// versionVector tracks versions for conflict resolution.
	versionVector *versionVector

	// localClusterID is the local cluster's ID.
	localClusterID cluster.ID

	// trustDomainGetter returns the mesh trust domain.
	// Used to set the trust domain on split-horizon workloads.
	trustDomainGetter func() string

	// localNetworkGetter returns the local network ID.
	// Used to add local network VIPs to remote services so ztunnel can find them.
	localNetworkGetter func() network.ID
}

// federationStoreConfig contains configuration for the federation store.
type federationStoreConfig struct {
	// LocalClusterID is the local cluster's identifier.
	LocalClusterID cluster.ID

	// TrustDomainGetter returns the mesh trust domain.
	TrustDomainGetter func() string

	// LocalNetworkGetter returns the local network ID.
	LocalNetworkGetter func() network.ID
}

// newFederationStore creates a new federation data store.
func newFederationStore(cfg federationStoreConfig) *federationStore {
	return &federationStore{
		shards:             make(map[cluster.ID]*clusterShard),
		versionVector:      newVersionVector(),
		localClusterID:     cfg.LocalClusterID,
		trustDomainGetter:  cfg.TrustDomainGetter,
		localNetworkGetter: cfg.LocalNetworkGetter,
	}
}

// handleSyncMessage processes a sync message from a peer.
func (s *federationStore) handleSyncMessage(msg *SyncMessage) {
	if msg == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	clusterID := msg.ClusterID
	if clusterID == s.localClusterID {
		// Don't process our own messages
		return
	}

	// Check if this is newer than what we have
	remoteVersion, ok := msg.VersionVector[clusterID]
	if !ok {
		remoteVersion = 0
	}
	localVersion := s.versionVector.get(clusterID)

	// Reject messages with version <= the recorded version. This covers both
	// normal stale-message filtering and tombstone protection: a tombstoned
	// shard retains its version, so retained snapshots from a dead cluster
	// cannot resurrect it. Only a strictly newer version can clear a tombstone.
	if remoteVersion <= localVersion {
		// Exception: allow full-sync with equal version (re-delivery of same snapshot),
		// but NOT when the shard is tombstoned — a tombstoned shard can only be
		// resurrected by a strictly newer version.
		if !(msg.FullSync && remoteVersion == localVersion && !s.isShardTombstoned(clusterID)) {
			storeLog.Debugf("Ignoring stale message from cluster %s (remote=%d, local=%d, tombstoned=%v)",
				clusterID, remoteVersion, localVersion, s.isShardTombstoned(clusterID))
			messagesDroppedStale.Increment()
			return
		}
	}

	storeLog.Infof("Processing sync message from cluster %s (full=%v, version=%d, gateway=%v)",
		clusterID, msg.FullSync, remoteVersion, msg.NetworkGateway)

	// Get or create shard for this cluster.
	// If the shard exists and is tombstoned, a strictly newer version resurrects it.
	shard, exists := s.shards[clusterID]
	if !exists || msg.FullSync || shard.Tombstoned {
		if shard != nil && shard.Tombstoned {
			storeLog.Infof("Resurrecting tombstoned cluster %s with version %d (was %d)",
				clusterID, remoteVersion, shard.Version)
			shardsResurrected.Increment()
		}
		shard = &clusterShard{
			ClusterID: clusterID,
			Services:  make(map[string]*model.ServiceInfo),
		}
		s.shards[clusterID] = shard
	}

	// Invalidate projection cache — the shard is about to be mutated.
	shard.cachedProjection = nil

	// Refresh liveness timestamp on every accepted message.
	shard.LastSeen = time.Now()

	// Store the network gateway info from this cluster
	if msg.NetworkGateway != nil {
		shard.NetworkGateway = msg.NetworkGateway
		storeLog.Debugf("Stored network gateway for cluster %s: network=%s, addr=%s, hbonePort=%d",
			clusterID, msg.NetworkGateway.Network, msg.NetworkGateway.Addr, msg.NetworkGateway.HBONEPort)
	}

	// Delete services listed in incremental messages
	for _, hostname := range msg.DeletedHostnames {
		if _, ok := shard.Services[hostname]; ok {
			delete(shard.Services, hostname)
			storeLog.Debugf("Deleted service %s from cluster %s", hostname, clusterID)
		}
	}

	// Process services
	for i := range msg.Services {
		svc := &msg.Services[i]
		key := svc.Service.Hostname
		shard.Services[key] = svc
	}

	// Update version vector
	s.versionVector.set(clusterID, remoteVersion)
	shard.Version = remoteVersion

	// Log current state of all shards
	var shardSummary []string
	for cid, sh := range s.shards {
		if sh.Tombstoned {
			shardSummary = append(shardSummary, fmt.Sprintf("%s:tombstoned", cid))
		} else {
			shardSummary = append(shardSummary, fmt.Sprintf("%s:%d", cid, len(sh.Services)))
		}
	}
	storeLog.Debugf("After sync: shards=%v", shardSummary)
}

// isShardTombstoned returns whether a cluster's shard is currently tombstoned.
// Must be called with s.mu held (read or write).
func (s *federationStore) isShardTombstoned(clusterID cluster.ID) bool {
	if shard, ok := s.shards[clusterID]; ok {
		return shard.Tombstoned
	}
	return false
}

// expireStaleShards checks all shards and tombstones any that have not received
// an accepted message within the given expiry duration. Tombstoned shards retain
// their Version so that stale messages cannot resurrect them.
// Returns the cluster IDs that were newly tombstoned (for logging/metrics).
func (s *federationStore) expireStaleShards(expiry time.Duration, now time.Time) []cluster.ID {
	if expiry <= 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var expired []cluster.ID
	for cid, shard := range s.shards {
		if shard.Tombstoned {
			continue
		}
		// Shards with zero LastSeen were loaded during bootstrap and have not
		// received a live message yet. Use the shard creation as a grace period:
		// don't expire until at least one full expiry window has passed since startup.
		if shard.LastSeen.IsZero() {
			continue
		}
		if now.Sub(shard.LastSeen) > expiry {
			storeLog.Warnf("Tombstoning shard for cluster %s: no message in %v (last seen %v)",
				cid, now.Sub(shard.LastSeen).Round(time.Second), shard.LastSeen.Format(time.RFC3339))
			shard.Tombstoned = true
			shard.Services = nil
			shard.NetworkGateway = nil
			shard.cachedProjection = nil
			expired = append(expired, cid)
			shardsExpired.Increment()
		}
	}
	return expired
}

// recordShardGauges updates the live/tombstoned shard gauge metrics.
func (s *federationStore) recordShardGauges() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var live, tombstoned int
	for _, shard := range s.shards {
		if shard.Tombstoned {
			tombstoned++
		} else {
			live++
		}
	}
	shardsLive.Record(float64(live))
	shardsTombstoned.Record(float64(tombstoned))
}

// getFederationState returns all services and workloads from the federation store.
// This is used to populate KRT collections for ambient index integration.
//
// Each shard's derived services and workloads are cached in a per-shard projection.
// Only shards whose data changed since the last call are recomputed. The final
// aggregation (concatenation + cross-shard gateway deduplication) is always done,
// but it is cheap compared to the per-service proto cloning and workload construction
// that the projection cache avoids.
func (s *federationStore) getFederationState() ([]model.ServiceInfo, []model.WorkloadInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the local network ID for adding local VIPs
	var localNetwork string
	if s.localNetworkGetter != nil {
		localNetwork = s.localNetworkGetter().String()
	}

	var services []model.ServiceInfo
	var workloads []model.WorkloadInfo

	// Track which gateways we've already created workloads for.
	// Key includes network, address, port, and cluster to avoid collapsing
	// two remote clusters that share a network and gateway address but have
	// different HBONE ports or identities.
	seenGateways := make(map[string]bool)

	for _, shard := range s.shards {
		// Skip tombstoned shards — their services and gateways have been cleared.
		if shard.Tombstoned {
			continue
		}

		// Use cached projection if available, otherwise rebuild it.
		proj := shard.cachedProjection
		if proj == nil {
			projectionCacheMisses.Increment()
			proj = s.buildShardProjection(shard, localNetwork)
			shard.cachedProjection = proj
		} else {
			projectionCacheHits.Increment()
		}

		services = append(services, proj.Services...)

		// Append workloads with cross-shard gateway deduplication.
		for i := range proj.Workloads {
			wl := &proj.Workloads[i]
			// Gateway workloads have UIDs like "NetworkGateway/network/addr/port".
			// Split-horizon workloads are never duplicated across shards.
			if strings.HasPrefix(wl.Workload.Uid, "NetworkGateway/") {
				if seenGateways[wl.Workload.Uid] {
					continue
				}
				seenGateways[wl.Workload.Uid] = true
			}
			workloads = append(workloads, *wl)
		}
	}

	return services, workloads
}

// buildShardProjection computes the derived services and workloads for a single
// shard. This is the expensive path: it clones protos, adds local network VIPs,
// and constructs split-horizon and gateway workloads.
func (s *federationStore) buildShardProjection(shard *clusterShard, localNetwork string) *shardProjection {
	proj := &shardProjection{}

	// Create a NetworkGateway workload for this shard's gateway.
	if shard.NetworkGateway != nil {
		gwWorkload := s.createNetworkGatewayWorkload(shard.NetworkGateway)
		if gwWorkload != nil {
			proj.Workloads = append(proj.Workloads, *gwWorkload)
		}
	}

	for _, svc := range shard.Services {
		if svc == nil || svc.Service == nil {
			continue
		}

		// Add local network VIPs to the service so ztunnel can find it.
		localizedSvc := s.addLocalNetworkVIPs(svc, localNetwork)
		proj.Services = append(proj.Services, *localizedSvc)

		// Create split-horizon workloads for services that have a network gateway.
		if shard.NetworkGateway != nil {
			gw := model.NetworkGateway{
				Network:   network.ID(shard.NetworkGateway.Network),
				Cluster:   cluster.ID(shard.NetworkGateway.Cluster),
				Addr:      shard.NetworkGateway.Addr,
				HBONEPort: shard.NetworkGateway.HBONEPort,
				ServiceAccount: types.NamespacedName{
					Name:      shard.NetworkGateway.ServiceAccount,
					Namespace: shard.NetworkGateway.Namespace,
				},
			}
			wlInfo := s.createSplitHorizonWorkload(localizedSvc, gw)
			if wlInfo != nil {
				proj.Workloads = append(proj.Workloads, *wlInfo)
			}
		}
	}

	return proj
}

// createSplitHorizonWorkload creates a synthetic workload that routes to a network gateway.
// This enables cross-network traffic routing for services discovered via federation.
func (s *federationStore) createSplitHorizonWorkload(svcInfo *model.ServiceInfo, gateway model.NetworkGateway) *model.WorkloadInfo {
	if svcInfo == nil || svcInfo.Service == nil {
		return nil
	}
	svc := svcInfo.Service
	svcNamespacedName := svc.Namespace + "/" + svc.Hostname

	uid := generateSplitHorizonWorkloadUID(gateway.Network.String(), gatewayResourceName(gateway), svcNamespacedName)

	hboneMtlsPort := gateway.HBONEPort
	if hboneMtlsPort == 0 {
		hboneMtlsPort = 15008
	}

	// Get trust domain for workload identity (defaults to "cluster.local")
	trustDomain := pickTrustDomain(s.trustDomainGetter)

	// Extract service account from the service's SubjectAltNames for mTLS identity verification.
	// The SANs are SPIFFE URIs like "spiffe://cluster.local/ns/<ns>/sa/<sa>".
	// The ztunnel uses the workload's ServiceAccount+Namespace to construct the expected SAN.
	serviceAccount := "default"
	if len(svc.SubjectAltNames) > 0 {
		// Parse the first SAN to extract the service account.
		// Format: spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>
		if parts := parseSpiffeSAN(svc.SubjectAltNames[0]); parts != nil {
			serviceAccount = parts.serviceAccount
		}
	}

	wl := &workloadapi.Workload{
		Uid:            uid,
		Name:           uid,
		Namespace:      svc.Namespace,
		Network:        gateway.Network.String(),
		TrustDomain:    trustDomain,
		ServiceAccount: serviceAccount,
		Capacity:       &wrapperspb.UInt32Value{Value: 1},
		WorkloadType:   workloadapi.WorkloadType_POD,
		TunnelProtocol: workloadapi.TunnelProtocol_HBONE,
		NetworkGateway: &workloadapi.GatewayAddress{
			Destination:   &workloadapi.GatewayAddress_Address{},
			HboneMtlsPort: hboneMtlsPort,
		},
		ClusterId: gateway.Cluster.String(),
		Services: map[string]*workloadapi.PortList{
			svcNamespacedName: {
				Ports: svc.Ports,
			},
		},
	}

	// Set up the network gateway address
	address, err := netip.ParseAddr(gateway.Addr)
	if err != nil {
		// Assume address is a hostname
		wl.NetworkGateway.Destination = &workloadapi.GatewayAddress_Hostname{
			Hostname: &workloadapi.NamespacedHostname{
				Namespace: gateway.ServiceAccount.Namespace,
				Hostname:  gateway.Addr,
			},
		}
	} else {
		wl.NetworkGateway.Destination = &workloadapi.GatewayAddress_Address{
			Address: &workloadapi.NetworkAddress{
				Network: gateway.Network.String(),
				Address: address.AsSlice(),
			},
		}
	}

	addr := &workloadapi.Address{Type: &workloadapi.Address_Workload{Workload: wl}}
	marshaled := protoconv.MessageToAny(addr)

	storeLog.Debugf("Created split-horizon workload: uid=%s, gateway=%s:%d, svc=%s",
		uid, gateway.Addr, hboneMtlsPort, svcNamespacedName)

	return &model.WorkloadInfo{
		Workload:         wl,
		Source:           kind.KubernetesGateway,
		Labels:           labelutil.AugmentLabels(nil, gateway.Cluster, "", "", gateway.Network),
		MarshaledAddress: marshaled,
		AsAddress:        model.AddressInfo{Address: addr, Marshaled: marshaled},
	}
}

// createNetworkGatewayWorkload creates a workload representing the remote network gateway.
// This is needed so ztunnel knows how to reach the gateway when routing to split-horizon workloads.
// The UID format matches what ztunnel expects: NetworkGateway/network/addr/port
func (s *federationStore) createNetworkGatewayWorkload(wireGw *WireNetworkGateway) *model.WorkloadInfo {
	if wireGw == nil {
		return nil
	}

	hbonePort := wireGw.HBONEPort
	if hbonePort == 0 {
		hbonePort = 15008
	}

	uid := fmt.Sprintf("NetworkGateway/%s/%s/%d", wireGw.Network, wireGw.Addr, hbonePort)
	trustDomain := pickTrustDomain(s.trustDomainGetter)

	// Use service account from wire gateway, default to "istio-eastwestgateway"
	serviceAccount := wireGw.ServiceAccount
	if serviceAccount == "" {
		serviceAccount = "istio-eastwestgateway"
	}

	// Use namespace from wire gateway, default to "istio-system"
	namespace := wireGw.Namespace
	if namespace == "" {
		namespace = "istio-system"
	}

	wl := &workloadapi.Workload{
		Uid:            uid,
		Name:           uid,
		Namespace:      namespace,
		Network:        wireGw.Network,
		TrustDomain:    trustDomain,
		ClusterId:      wireGw.Cluster,
		ServiceAccount: serviceAccount,
	}

	// Set the gateway address
	address, err := netip.ParseAddr(wireGw.Addr)
	if err != nil {
		// If it's a hostname, set it as the hostname
		wl.Hostname = wireGw.Addr
	} else {
		// If it's an IP, add it to addresses
		wl.Addresses = append(wl.Addresses, address.AsSlice())
	}

	addr := &workloadapi.Address{Type: &workloadapi.Address_Workload{Workload: wl}}
	marshaled := protoconv.MessageToAny(addr)

	storeLog.Debugf("Created network gateway workload: uid=%s, addr=%s:%d, sa=%s/%s",
		uid, wireGw.Addr, hbonePort, namespace, serviceAccount)

	return &model.WorkloadInfo{
		Workload:         wl,
		Source:           kind.KubernetesGateway,
		Labels:           labelutil.AugmentLabels(nil, cluster.ID(wireGw.Cluster), "", "", network.ID(wireGw.Network)),
		MarshaledAddress: marshaled,
		AsAddress:        model.AddressInfo{Address: addr, Marshaled: marshaled},
	}
}

// addLocalNetworkVIPs adds VIP entries with the local network for a remote service.
// This is necessary because ztunnel looks up services using the source workload's network.
// For example, if a service has VIP network1/10.110.55.89 and we're on network2,
// ztunnel will look up network2/10.110.55.89 which wouldn't match without this transformation.
func (s *federationStore) addLocalNetworkVIPs(svc *model.ServiceInfo, localNetwork string) *model.ServiceInfo {
	if svc == nil || svc.Service == nil || localNetwork == "" {
		return svc
	}

	// Check if any addresses need local network VIPs
	needsLocalVIPs := false
	for _, addr := range svc.Service.Addresses {
		if addr.Network != localNetwork && addr.Network != "" {
			needsLocalVIPs = true
			break
		}
	}

	if !needsLocalVIPs {
		return svc
	}

	// Clone the service to avoid modifying the stored version
	newSvc := protomarshal.Clone(svc.Service)

	// Rebuild addresses from scratch — the clone already copied the original
	// slice, and we need to add local-network variants while deduplicating.
	newSvc.Addresses = make([]*workloadapi.NetworkAddress, 0, len(svc.Service.Addresses)*2)
	seenIPs := make(map[string]bool, len(svc.Service.Addresses)*2)
	for _, addr := range svc.Service.Addresses {
		key := addr.Network + "/" + string(addr.Address)
		if seenIPs[key] {
			continue
		}
		seenIPs[key] = true
		newSvc.Addresses = append(newSvc.Addresses, addr)

		// Add a local network version if the address is from a different network
		if addr.Network != localNetwork && addr.Network != "" {
			localKey := localNetwork + "/" + string(addr.Address)
			if !seenIPs[localKey] {
				seenIPs[localKey] = true
				localAddr := &workloadapi.NetworkAddress{
					Network: localNetwork,
					Address: addr.Address,
				}
				newSvc.Addresses = append(newSvc.Addresses, localAddr)
				addrStr, _ := netip.AddrFromSlice(addr.Address)
				storeLog.Debugf("Added local network VIP: %s/%s for service %s",
					localNetwork, addrStr.String(), svc.Service.Hostname)
			}
		}
	}

	// Recompute marshaled address
	newAddr := &workloadapi.Address{
		Type: &workloadapi.Address_Service{
			Service: newSvc,
		},
	}
	marshaled := protoconv.MessageToAny(newAddr)

	return &model.ServiceInfo{
		Service:          newSvc,
		LabelSelector:    svc.LabelSelector,
		PortNames:        svc.PortNames,
		Source:           svc.Source,
		Scope:            svc.Scope,
		Waypoint:         svc.Waypoint,
		CreationTime:     svc.CreationTime,
		MarshaledAddress: marshaled,
		AsAddress: model.AddressInfo{
			Address:   newAddr,
			Marshaled: marshaled,
		},
	}
}

func gatewayResourceName(gateway model.NetworkGateway) string {
	if gateway.Cluster != "" {
		return gateway.Cluster.String() + "/" + gateway.Addr
	}
	return gateway.Addr
}

func generateSplitHorizonWorkloadUID(networkID, gatewayName, service string) string {
	return networkID + "/SplitHorizonWorkload/" + gatewayName + "/" + service
}

func pickTrustDomain(trustDomainGetter func() string) string {
	if trustDomainGetter != nil {
		if td := trustDomainGetter(); td != "" {
			return td
		}
	}
	return "cluster.local"
}

// spiffeParts holds the components parsed from a SPIFFE URI.
type spiffeParts struct {
	namespace      string
	serviceAccount string
}

// parseSpiffeSAN extracts namespace and service account from a SPIFFE URI.
// Format: spiffe://<trust-domain>/ns/<namespace>/sa/<service-account>
func parseSpiffeSAN(san string) *spiffeParts {
	const prefix = "spiffe://"
	if !strings.HasPrefix(san, prefix) {
		return nil
	}
	// Strip "spiffe://<trust-domain>/"
	rest := san[len(prefix):]
	idx := strings.Index(rest, "/")
	if idx < 0 {
		return nil
	}
	rest = rest[idx+1:] // "ns/<namespace>/sa/<service-account>"
	parts := strings.SplitN(rest, "/", 4)
	if len(parts) != 4 || parts[0] != "ns" || parts[2] != "sa" {
		return nil
	}
	return &spiffeParts{
		namespace:      parts[1],
		serviceAccount: parts[3],
	}
}

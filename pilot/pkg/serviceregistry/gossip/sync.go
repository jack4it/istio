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
	"fmt"
	"strings"
	"time"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/log"
	"istio.io/istio/pkg/network"
)

var syncLog = log.RegisterScope("gossip-sync", "Gossip sync protocol")

// nodeSuffix is appended to cluster ID to form the Serf node name.
const nodeSuffix = "-istiod"

// NodeName returns the Serf node name for a given cluster ID.
// This ensures consistent naming across the codebase.
func NodeName(clusterID cluster.ID) string {
	return string(clusterID) + nodeSuffix
}

// extractClusterID extracts the cluster ID from a peer node name.
// Node names are formatted as "{clusterID}-istiod".
func extractClusterID(peerName string) cluster.ID {
	if strings.HasSuffix(peerName, nodeSuffix) {
		return cluster.ID(strings.TrimSuffix(peerName, nodeSuffix))
	}
	// Fallback: return the whole name if format doesn't match
	return cluster.ID(peerName)
}

// SyncProtocol handles the synchronization protocol between peers.
// It broadcasts local service changes to peers and processes incoming messages.
type SyncProtocol struct {
	// store is the internal data store for gossip-synced services.
	store *gossipStore

	// serfCluster handles peer connections and messaging.
	serfCluster *serfCluster

	// localClusterID is the local cluster's ID.
	localClusterID cluster.ID

	// versionVector tracks local versions for outbound sync messages.
	versionVector *versionVector

	// tombstones manages deletion markers for outbound sync.
	tombstones *tombstoneStore

	// ambientIndexGetter returns the ambient index for:
	// - Registering gossip collections
	// - Getting local global services to broadcast
	// - Receiving service change notifications
	// This is a getter because the index may not be available at construction time.
	ambientIndexGetter func() model.GossipAmbientIndex

	// ambientIndex is the cached ambient index once retrieved.
	ambientIndex model.GossipAmbientIndex

	// gossipServices holds gossip-federated services for ambient index integration.
	gossipServices krt.StaticCollection[model.ServiceInfo]

	// gossipWorkloads holds gossip-federated workloads for ambient index integration.
	gossipWorkloads krt.StaticCollection[model.WorkloadInfo]

	// localNetworkGatewayGetter returns the local network gateway.
	localNetworkGatewayGetter func() *model.NetworkGateway

	// xdsUpdater triggers XDS pushes when gossip collections change.
	xdsUpdater model.XDSUpdater

	// stopCh signals shutdown.
	stopCh chan struct{}
}

// SyncProtocolConfig contains configuration for the sync protocol.
type SyncProtocolConfig struct {
	LocalClusterID cluster.ID

	// AmbientIndexGetter returns the ambient index for direct integration.
	// This is a getter to support deferred initialization - the ambient index
	// may not be available until after the k8s registry is fully registered.
	// Used to register gossip collections and access local global services.
	AmbientIndexGetter func() model.GossipAmbientIndex

	// LocalNetworkGatewayGetter returns the local cluster's network gateway.
	// This is synced to remote clusters so they can route traffic back.
	LocalNetworkGatewayGetter func() *model.NetworkGateway

	// XDSUpdater triggers XDS pushes when gossip collections change.
	// This is required to notify ztunnel about new services/workloads.
	XDSUpdater model.XDSUpdater

	// TrustDomainGetter returns the mesh trust domain.
	// Used to set the trust domain on split-horizon workloads for HBONE mTLS.
	TrustDomainGetter func() string

	// Gossip networking configuration
	NodeName      string
	BindAddr      string
	BindPort      int
	AdvertiseAddr string
	AdvertisePort int
	EncryptionKey []byte

	StopCh chan struct{}
}

// NewSyncProtocol creates a new sync protocol handler.
// Note: Call Start() to begin gossip networking and register with service controllers.
func NewSyncProtocol(cfg SyncProtocolConfig) (*SyncProtocol, error) {
	// Create internal components for tracking outbound sync state
	versionVector := newVersionVector()
	tombstones := newTombstoneStore(cfg.StopCh)

	// Create a local network getter from the gateway getter
	localNetworkGetter := func() network.ID {
		if cfg.LocalNetworkGatewayGetter == nil {
			return ""
		}
		gw := cfg.LocalNetworkGatewayGetter()
		if gw == nil {
			return ""
		}
		return gw.Network
	}

	// Create the gossip store for holding synced remote services
	store := newGossipStore(gossipStoreConfig{
		LocalClusterID:     cfg.LocalClusterID,
		TrustDomainGetter:  cfg.TrustDomainGetter,
		LocalNetworkGetter: localNetworkGetter,
		StopCh:             cfg.StopCh,
	})

	// Create KRT StaticCollections for gossip services and workloads.
	// These will be registered with the ambient index for lookup integration.
	gossipServices := krt.NewStaticCollection[model.ServiceInfo](
		nil, // synced - will be set when initial sync completes
		nil, // initial values
		krt.WithName("GossipServices"),
		krt.WithStop(cfg.StopCh),
	)
	gossipWorkloads := krt.NewStaticCollection[model.WorkloadInfo](
		nil, // synced
		nil, // initial values
		krt.WithName("GossipWorkloads"),
		krt.WithStop(cfg.StopCh),
	)

	sp := &SyncProtocol{
		store:                     store,
		localClusterID:            cfg.LocalClusterID,
		versionVector:             versionVector,
		tombstones:                tombstones,
		ambientIndexGetter:        cfg.AmbientIndexGetter,
		gossipServices:            gossipServices,
		gossipWorkloads:           gossipWorkloads,
		localNetworkGatewayGetter: cfg.LocalNetworkGatewayGetter,
		xdsUpdater:                cfg.XDSUpdater,
		stopCh:                    cfg.StopCh,
	}

	// Create Serf cluster with handlers
	serfCfg := serfConfig{
		NodeName:      cfg.NodeName,
		ClusterID:     cfg.LocalClusterID,
		BindAddr:      cfg.BindAddr,
		BindPort:      cfg.BindPort,
		AdvertiseAddr: cfg.AdvertiseAddr,
		AdvertisePort: cfg.AdvertisePort,
		MessageHandler: func(msg *SyncMessage) {
			sp.handleIncomingMessage(msg)
		},
		MemberJoinHandler: func(peerName string) {
			sp.handlePeerConnect(peerName)
		},
		MemberLeaveHandler: func(peerName string) {
			sp.handlePeerDisconnect(peerName)
		},
		EncryptionKey: cfg.EncryptionKey,
		StopCh:        cfg.StopCh,
	}

	sc, err := newSerfCluster(serfCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create serf cluster: %w", err)
	}

	// Wire up the full sync builder
	sc.setFullSyncBuilder(sp.buildFullSyncMessage)
	sp.serfCluster = sc

	return sp, nil
}

// Start begins gossip networking, registers for service change events,
// and registers gossip collections with the ambient index.
// The order is important to avoid races:
// 1. Start serf cluster (so we can send/receive messages)
// 2. Register gossip collections with ambient index
// 3. Register for service change events (so we can broadcast changes)
// 4. Send initial full sync (after collections have populated)
func (sp *SyncProtocol) Start() error {
	// 1. Start serf cluster first - must be running before we can broadcast
	if err := sp.serfCluster.start(); err != nil {
		return fmt.Errorf("failed to start serf cluster: %w", err)
	}
	syncLog.Info("Serf cluster started")

	// Get ambient index - may be nil if not yet available or not enabled
	if sp.ambientIndexGetter != nil {
		sp.ambientIndex = sp.ambientIndexGetter()
	}

	if sp.ambientIndex != nil {
		// 2. Register gossip collections with ambient index
		// This enables gossip services/workloads to be included in ambient lookups
		sp.ambientIndex.RegisterGossipCollections(sp.gossipServices, sp.gossipWorkloads)
		syncLog.Info("Registered gossip collections with ambient index")

		// 3. Register for push-based global service change events
		// Now that serf is running, we can safely broadcast changes
		sp.ambientIndex.RegisterGlobalServiceHandler(func(prev, curr *model.ServiceInfo, event model.Event) {
			switch event {
			case model.EventAdd:
				sp.handleServiceAdd(curr)
			case model.EventUpdate:
				sp.handleServiceUpdate(curr)
			case model.EventDelete:
				if prev != nil && prev.Service != nil {
					// Use hostname as the key (same as store.go)
					sp.handleServiceDelete(prev.Service.Hostname)
				}
			}
		})
		syncLog.Info("Registered for push-based global service events")

		// 4. Send initial full sync now that everything is registered.
		// This catches any services that were already in the collection when
		// the handler was registered (since KRT only fires events for changes).
		// This is a goroutine to avoid blocking Start() - services may still be syncing.
		go func() {
			// Wait for peers to establish connections before sending full sync.
			// The memberlist needs time to complete UDP/TCP handshakes.
			for i := 0; i < 12; i++ { // Wait up to 60 seconds total
				time.Sleep(5 * time.Second)
				peerCount := sp.serfCluster.memberCount()
				syncLog.Infof("Checking peer status: %d peers connected", peerCount)
				if peerCount > 1 { // At least one peer besides ourselves
					break
				}
			}
			syncLog.Info("Sending delayed initial full sync after KRT population")
			msg := sp.buildFullSyncMessage()
			sp.broadcast(msg)
		}()
	} else {
		syncLog.Warn("No ambient index available - gossip sync will only exchange peer metadata")
	}

	syncLog.Info("Sync protocol started")
	return nil
}

// Stop gracefully stops the sync protocol.
func (sp *SyncProtocol) Stop() error {
	if err := sp.serfCluster.leave(); err != nil {
		syncLog.Warnf("Error leaving serf cluster: %v", err)
	}
	return sp.serfCluster.shutdown()
}

// broadcast sends a sync message to all connected peers.
func (sp *SyncProtocol) broadcast(msg *SyncMessage) {
	if err := sp.serfCluster.broadcastServiceSync(msg); err != nil {
		syncLog.Warnf("Failed to broadcast message: %v", err)
	}
}

// handlePeerConnect is called when a new peer connects.
// It sends a full sync message with all local global services.
func (sp *SyncProtocol) handlePeerConnect(peerName string) {
	syncLog.Infof("Peer connected: %s, sending full sync", peerName)

	msg := sp.buildFullSyncMessage()
	sp.broadcast(msg)
}

// handlePeerDisconnect is called when a peer leaves or fails.
// For now we just log it - services remain in the registry since the peer may rejoin.
// Future enhancement: track stale clusters and clean up after extended absence.
func (sp *SyncProtocol) handlePeerDisconnect(peerName string) {
	// Extract cluster ID from peer name (format: "{clusterID}-istiod")
	peerClusterID := extractClusterID(peerName)

	syncLog.Warnf("Peer disconnected: %s (cluster=%s) - services retained pending reconnection",
		peerName, peerClusterID)

	// Note: We intentionally don't remove services here.
	// The peer might rejoin shortly (network blip, pod restart, etc.).
	// Removing services immediately could cause unnecessary traffic disruption.
	//
	// For production, consider:
	// 1. Starting a timer to mark services stale after extended absence
	// 2. Notifying the registry to flag services from this cluster
	// 3. Cleaning up only after confirmed permanent departure
}

// getLocalNetworkGateway returns the local network gateway info for syncing.
func (sp *SyncProtocol) getLocalNetworkGateway() *WireNetworkGateway {
	if sp.localNetworkGatewayGetter == nil {
		return nil
	}
	gw := sp.localNetworkGatewayGetter()
	if gw == nil {
		return nil
	}
	wireGw := &WireNetworkGateway{
		Network:        gw.Network.String(),
		Cluster:        gw.Cluster.String(),
		Addr:           gw.Addr,
		HBONEPort:      gw.HBONEPort,
		ServiceAccount: gw.ServiceAccount.Name,
		Namespace:      gw.ServiceAccount.Namespace,
	}
	return wireGw
}

// buildFullSyncMessage creates a full sync message with all local global services.
func (sp *SyncProtocol) buildFullSyncMessage() *SyncMessage {
	var services []model.ServiceInfo

	if sp.ambientIndex != nil {
		// Use AllLocalNetworkGlobalServicesWithSANs to include SPIFFE identities
		// so remote clusters know what identity to expect when connecting
		services = sp.ambientIndex.AllLocalNetworkGlobalServicesWithSANs()
	}

	// Increment version for this sync
	version := sp.versionVector.increment(sp.localClusterID)

	msg := &SyncMessage{
		VersionVector:  sp.versionVector.copy(),
		FullSync:       true,
		ClusterID:      sp.localClusterID,
		NetworkGateway: sp.getLocalNetworkGateway(),
		Services:       services,
		Tombstones:     sp.tombstones.getAll(),
	}

	syncLog.Infof("Built full sync message: %d services, version=%d",
		len(services), version)

	return msg
}

// handleServiceAdd handles a new global service being added locally.
func (sp *SyncProtocol) handleServiceAdd(svc *model.ServiceInfo) {
	if svc == nil {
		return
	}

	// Only sync global services
	if svc.Scope != model.Global {
		return
	}

	syncLog.Debugf("Broadcasting service add: %s", svc.Service.Hostname)

	// Get service with SANs populated
	svcWithSANs := sp.ambientIndex.ServiceWithSANs(svc)
	if svcWithSANs == nil {
		svcWithSANs = svc
	}

	version := sp.versionVector.increment(sp.localClusterID)

	msg := &SyncMessage{
		VersionVector:  sp.versionVector.copy(),
		FullSync:       false,
		ClusterID:      sp.localClusterID,
		NetworkGateway: sp.getLocalNetworkGateway(),
		Services:       []model.ServiceInfo{*svcWithSANs},
	}

	sp.broadcast(msg)
	syncLog.Debugf("Broadcasted service add at version %d", version)
}

// handleServiceUpdate handles a global service being updated locally.
func (sp *SyncProtocol) handleServiceUpdate(svc *model.ServiceInfo) {
	if svc == nil {
		return
	}

	// Only sync global services
	if svc.Scope != model.Global {
		return
	}

	syncLog.Debugf("Broadcasting service update: %s", svc.Service.Hostname)

	// Get service with SANs populated
	svcWithSANs := sp.ambientIndex.ServiceWithSANs(svc)
	if svcWithSANs == nil {
		svcWithSANs = svc
	}

	version := sp.versionVector.increment(sp.localClusterID)

	msg := &SyncMessage{
		VersionVector:  sp.versionVector.copy(),
		FullSync:       false,
		ClusterID:      sp.localClusterID,
		NetworkGateway: sp.getLocalNetworkGateway(),
		Services:       []model.ServiceInfo{*svcWithSANs},
	}

	sp.broadcast(msg)
	syncLog.Debugf("Broadcasted service update at version %d", version)
}

// handleServiceDelete handles a global service being deleted locally.
func (sp *SyncProtocol) handleServiceDelete(hostname string) {
	syncLog.Debugf("Broadcasting service delete: %s", hostname)

	version := sp.versionVector.increment(sp.localClusterID)

	tombstone := sp.tombstones.add(hostname, sp.localClusterID, version)

	msg := &SyncMessage{
		VersionVector: sp.versionVector.copy(),
		FullSync:      false,
		ClusterID:     sp.localClusterID,
		Tombstones:    []Tombstone{tombstone},
	}

	sp.broadcast(msg)
	syncLog.Debugf("Broadcasted service delete tombstone at version %d", version)
}

// handleIncomingMessage processes a message received from a peer.
func (sp *SyncProtocol) handleIncomingMessage(msg *SyncMessage) {
	if msg == nil {
		return
	}

	// Delegate to store for processing
	sp.store.handleSyncMessage(msg)

	// Update KRT collections with the new state from the store
	services, workloads := sp.store.getGossipState()
	sp.gossipServices.Reset(services)
	sp.gossipWorkloads.Reset(workloads)
	syncLog.Infof("Updated gossip collections: %d services, %d workloads", len(services), len(workloads))

	// Trigger XDS push to notify ztunnel about new services/workloads
	if sp.xdsUpdater != nil && (len(services) > 0 || len(workloads) > 0) {
		sp.xdsUpdater.ConfigUpdate(&model.PushRequest{
			Full:   true,
			Reason: model.NewReasonStats(model.GossipUpdate),
		})
		syncLog.Debugf("Triggered XDS push for gossip update")
	}
}

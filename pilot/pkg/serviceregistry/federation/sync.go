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
	"time"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/log"
	"istio.io/istio/pkg/network"
)

const (
	// debounceAfter is the quiet period after receiving a message before
	// flushing accumulated state to KRT collections and triggering an XDS push.
	// If more messages arrive within this window, the timer resets.
	debounceAfter = 200 * time.Millisecond

	// debounceMax is the maximum time to wait before flushing, regardless of
	// whether messages are still arriving. This bounds worst-case latency
	// during sustained high-throughput updates.
	debounceMax = 2 * time.Second
)

var syncLog = log.RegisterScope("federation-sync", "Federation sync protocol")

// SyncProtocol handles the synchronization protocol between istiod peers.
// It broadcasts local service changes via Service Bus and processes incoming messages.
type SyncProtocol struct {
	// store holds synced remote services from other clusters.
	store *federationStore

	// transport handles message delivery via Azure Service Bus.
	transport *ServiceBusTransport

	// localClusterID is the local cluster's ID.
	localClusterID cluster.ID

	// versionVector tracks local versions for outbound sync messages.
	versionVector *versionVector

	// tombstones manages deletion markers for outbound sync.
	tombstones *tombstoneStore

	// ambientIndexGetter returns the ambient index.
	// This is a getter because the index may not be available at construction time.
	ambientIndexGetter func() model.FederationAmbientIndex

	// ambientIndex is the cached ambient index once retrieved.
	ambientIndex model.FederationAmbientIndex

	// federationServices holds federated remote services for ambient index integration.
	federationServices krt.StaticCollection[model.ServiceInfo]

	// federationWorkloads holds federated remote workloads for ambient index integration.
	federationWorkloads krt.StaticCollection[model.WorkloadInfo]

	// localNetworkGatewayGetter returns the local network gateway.
	localNetworkGatewayGetter func() *model.NetworkGateway

	// xdsUpdater triggers XDS pushes when federation collections change.
	xdsUpdater model.XDSUpdater

	// incomingCh buffers incoming messages for debounced processing.
	// Messages are applied to the store immediately but KRT collection
	// resets and XDS pushes are batched.
	incomingCh chan *SyncMessage

	// stopCh signals shutdown.
	stopCh chan struct{}
}

// SyncProtocolConfig contains configuration for the sync protocol.
type SyncProtocolConfig struct {
	LocalClusterID cluster.ID

	// AmbientIndexGetter returns the ambient index for direct integration.
	// This is a getter to support deferred initialization — the ambient index
	// may not be available until after the k8s registry is fully registered.
	AmbientIndexGetter func() model.FederationAmbientIndex

	// LocalNetworkGatewayGetter returns the local cluster's network gateway.
	// This is synced to remote clusters so they can route traffic back.
	LocalNetworkGatewayGetter func() *model.NetworkGateway

	// XDSUpdater triggers XDS pushes when federation collections change.
	// This is required to notify ztunnel about new services/workloads.
	XDSUpdater model.XDSUpdater

	// TrustDomainGetter returns the mesh trust domain.
	// Used to set the trust domain on split-horizon workloads for HBONE mTLS.
	TrustDomainGetter func() string

	// Transport is the Service Bus transport for federation messaging.
	Transport *ServiceBusTransport

	StopCh chan struct{}
}

// NewSyncProtocol creates a new sync protocol handler.
// Note: Call Start() to begin receiving/publishing messages.
func NewSyncProtocol(cfg SyncProtocolConfig) (*SyncProtocol, error) {
	if cfg.Transport == nil {
		return nil, fmt.Errorf("Transport is required")
	}

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

	// Create the federation store for holding synced remote services
	store := newFederationStore(federationStoreConfig{
		LocalClusterID:     cfg.LocalClusterID,
		TrustDomainGetter:  cfg.TrustDomainGetter,
		LocalNetworkGetter: localNetworkGetter,
		StopCh:             cfg.StopCh,
	})

	// Create KRT StaticCollections for federation services and workloads.
	// These will be registered with the ambient index for lookup integration.
	federationServices := krt.NewStaticCollection[model.ServiceInfo](
		nil, // synced - will be set when initial sync completes
		nil, // initial values
		krt.WithName("FederationServices"),
		krt.WithStop(cfg.StopCh),
	)
	federationWorkloads := krt.NewStaticCollection[model.WorkloadInfo](
		nil, // synced
		nil, // initial values
		krt.WithName("FederationWorkloads"),
		krt.WithStop(cfg.StopCh),
	)

	sp := &SyncProtocol{
		store:                     store,
		localClusterID:            cfg.LocalClusterID,
		versionVector:             versionVector,
		tombstones:                tombstones,
		ambientIndexGetter:        cfg.AmbientIndexGetter,
		federationServices:        federationServices,
		federationWorkloads:       federationWorkloads,
		localNetworkGatewayGetter: cfg.LocalNetworkGatewayGetter,
		xdsUpdater:                cfg.XDSUpdater,
		incomingCh:                make(chan *SyncMessage, 100),
		stopCh:                    cfg.StopCh,
		transport:                 cfg.Transport,
	}

	// Late-bind the message handler — the transport is constructed before
	// SyncProtocol exists (in bootstrap), so it receives the handler here.
	cfg.Transport.SetMessageHandler(func(msg *SyncMessage) {
		sp.handleIncomingMessage(msg)
	})

	// Wire up the full sync builder for periodic snapshots
	cfg.Transport.SetFullSyncBuilder(sp.buildFullSyncMessage)

	return sp, nil
}

// Start begins federation messaging, registers for service change events,
// and registers federation collections with the ambient index.
// The order is important to avoid races:
// 1. Start transport (so we can send/receive messages)
// 2. Register federation collections with ambient index
// 3. Register for service change events (so we can broadcast changes)
// 4. Send initial full sync (after collections have populated)
func (sp *SyncProtocol) Start() error {
	// 1. Start transport first — must be running before we can broadcast
	if err := sp.transport.Start(); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}
	syncLog.Info("Transport started")

	// Start the debounced message processor. All incoming messages flow
	// through incomingCh and are batched before updating KRT collections.
	go sp.processIncomingDebounced()

	// Get ambient index — may be nil if not yet available or not enabled
	if sp.ambientIndexGetter != nil {
		sp.ambientIndex = sp.ambientIndexGetter()
	}

	if sp.ambientIndex != nil {
		// 2. Register federation collections with ambient index
		sp.ambientIndex.RegisterFederationCollections(sp.federationServices, sp.federationWorkloads)
		syncLog.Info("Registered federation collections with ambient index")

		// 3. Register for push-based global service change events
		sp.ambientIndex.RegisterGlobalServiceHandler(func(prev, curr *model.ServiceInfo, event model.Event) {
			switch event {
			case model.EventAdd:
				sp.handleServiceAdd(curr)
			case model.EventUpdate:
				sp.handleServiceUpdate(curr)
			case model.EventDelete:
				if prev != nil && prev.Service != nil {
					sp.handleServiceDelete(prev.Service.Hostname)
				}
			}
		})
		syncLog.Info("Registered for push-based global service events")

		// 4. Wait for bootstrap to complete, then send initial full sync.
		// The transport's two-phase bootstrap drains retained messages first,
		// so we don't broadcast until we've loaded remote state.
		go func() {
			select {
			case <-sp.transport.BootstrapDone():
				syncLog.Info("Bootstrap complete, sending initial full sync")
				msg := sp.buildFullSyncMessage()
				sp.broadcast(msg)
			case <-sp.stopCh:
				return
			}
		}()
	} else {
		syncLog.Warn("No ambient index available — federation sync will only exchange peer metadata")
	}

	syncLog.Info("Sync protocol started")
	return nil
}

// Stop gracefully stops the sync protocol.
func (sp *SyncProtocol) Stop() error {
	return sp.transport.Shutdown()
}

// broadcast sends a sync message to all peers via Service Bus.
func (sp *SyncProtocol) broadcast(msg *SyncMessage) {
	if err := sp.transport.Broadcast(msg); err != nil {
		syncLog.Warnf("Failed to broadcast message: %v", err)
	}
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

// processIncomingDebounced is the main loop for debounced message processing.
// It accumulates incoming messages and applies them to the store immediately
// (so version vectors stay current), but defers the expensive KRT collection
// reset and XDS push until a quiet period or max wait is reached.
//
// This provides two layers of batching:
//  1. Federation debounce (200ms quiet / 2s max) — batches store→KRT→push
//  2. Istiod's built-in XDS debounce (100ms/10s) — batches pushes to proxies
func (sp *SyncProtocol) processIncomingDebounced() {
	for {
		// Block until first message arrives.
		var msg *SyncMessage
		select {
		case msg = <-sp.incomingCh:
		case <-sp.stopCh:
			return
		}

		sp.store.handleSyncMessage(msg)
		count := 1

		// Start debounce timers.
		quietTimer := time.NewTimer(debounceAfter)
		maxTimer := time.NewTimer(debounceMax)

		// Accumulate more messages within the debounce window.
	drain:
		for {
			select {
			case msg = <-sp.incomingCh:
				sp.store.handleSyncMessage(msg)
				count++
				if !quietTimer.Stop() {
					<-quietTimer.C
				}
				quietTimer.Reset(debounceAfter)
			case <-quietTimer.C:
				break drain
			case <-maxTimer.C:
				break drain
			case <-sp.stopCh:
				quietTimer.Stop()
				maxTimer.Stop()
				return
			}
		}
		quietTimer.Stop()
		maxTimer.Stop()

		// Single batch update: rebuild KRT collections and trigger one XDS push.
		services, workloads := sp.store.getFederationState()
		sp.federationServices.Reset(services)
		sp.federationWorkloads.Reset(workloads)

		if sp.xdsUpdater != nil {
			sp.xdsUpdater.ConfigUpdate(&model.PushRequest{
				Full:   true,
				Reason: model.NewReasonStats(model.FederationUpdate),
			})
		}

		syncLog.Infof("Flushed debounced federation update: %d messages, %d services, %d workloads",
			count, len(services), len(workloads))
	}
}

// handleIncomingMessage enqueues a received message for debounced processing.
// The message is applied to the store in processIncomingDebounced, and KRT
// collections are only rebuilt once per debounce window.
func (sp *SyncProtocol) handleIncomingMessage(msg *SyncMessage) {
	if msg == nil {
		return
	}

	syncLog.Debugf("Received sync from cluster %s: %d services, fullSync=%v — enqueuing",
		msg.ClusterID, len(msg.Services), msg.FullSync)

	select {
	case sp.incomingCh <- msg:
	case <-sp.stopCh:
	}
}

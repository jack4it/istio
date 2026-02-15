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
	"sync/atomic"
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
	// These are provided externally (owned by FederationSource) and shared with the ambient index.
	federationServices krt.StaticCollection[model.ServiceInfo]

	// federationWorkloads holds federated remote workloads for ambient index integration.
	// These are provided externally (owned by FederationSource) and shared with the ambient index.
	federationWorkloads krt.StaticCollection[model.WorkloadInfo]

	// localNetworkGatewayGetter returns the local network gateway.
	localNetworkGatewayGetter func() *model.NetworkGateway

	// snapshotInterval controls how often a full-sync snapshot is published.
	snapshotInterval time.Duration

	// incomingCh buffers incoming messages for debounced processing.
	// Messages are applied to the store immediately but KRT collection
	// resets and XDS pushes are batched.
	incomingCh chan *SyncMessage

	// outgoingCh buffers local service change events for debounced broadcasting.
	// Events are deduplicated per hostname and batched into a single SyncMessage.
	outgoingCh chan outgoingEvent

	// isLeader is true when this replica holds the federation leader lease.
	// Only the leader publishes outbound messages (snapshots + service events).
	// All replicas process inbound messages to maintain complete federation state.
	isLeader atomic.Bool

	// leaderStopCh is closed when this replica loses leader status.
	// It signals the snapshot loop and outgoing debounce loop to stop.
	// Re-created each time leadership is acquired.
	leaderStopCh chan struct{}

	// stopCh signals shutdown.
	stopCh chan struct{}
}

// outgoingEvent represents a local service change to be broadcast to peers.
type outgoingEvent struct {
	service  *model.ServiceInfo // non-nil for add/update, nil for delete
	hostname string             // set for delete events
	event    model.Event
}

// SyncProtocolConfig contains configuration for the sync protocol.
type SyncProtocolConfig struct {
	LocalClusterID cluster.ID

	// SnapshotInterval controls how often a full-sync snapshot is published
	// to Service Bus. New istiods bootstrap from these retained snapshots.
	// Defaults to 5m if zero. Set negative to disable.
	SnapshotInterval time.Duration

	// AmbientIndexGetter returns the ambient index for direct integration.
	// This is a getter to support deferred initialization — the ambient index
	// may not be available until after the k8s registry is fully registered.
	AmbientIndexGetter func() model.FederationAmbientIndex

	// LocalNetworkGatewayGetter returns the local cluster's network gateway.
	// This is synced to remote clusters so they can route traffic back.
	LocalNetworkGatewayGetter func() *model.NetworkGateway

	// FederationServices is the StaticCollection for federated services.
	// Owned by FederationSource and shared with the ambient index via Options.
	// sync.go calls Reset() when inbound messages arrive; the ambient index
	// observes changes via krt's reactive pipeline.
	FederationServices krt.StaticCollection[model.ServiceInfo]

	// FederationWorkloads is the StaticCollection for federated workloads.
	// Same ownership model as FederationServices.
	FederationWorkloads krt.StaticCollection[model.WorkloadInfo]

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

	// Use externally provided federation collections (owned by FederationSource).
	// These are already wired into the ambient index via Options.FederationSources.
	federationServices := cfg.FederationServices
	federationWorkloads := cfg.FederationWorkloads

	snapshotInterval := cfg.SnapshotInterval
	if snapshotInterval == 0 {
		snapshotInterval = 5 * time.Minute
	}

	sp := &SyncProtocol{
		store:                     store,
		localClusterID:            cfg.LocalClusterID,
		versionVector:             versionVector,
		tombstones:                tombstones,
		ambientIndexGetter:        cfg.AmbientIndexGetter,
		federationServices:        federationServices,
		federationWorkloads:       federationWorkloads,
		localNetworkGatewayGetter: cfg.LocalNetworkGatewayGetter,
		snapshotInterval:          snapshotInterval,
		incomingCh:                make(chan *SyncMessage, 100),
		outgoingCh:                make(chan outgoingEvent, 100),
		stopCh:                    cfg.StopCh,
		transport:                 cfg.Transport,
	}

	// Late-bind the message handler — the transport is constructed before
	// SyncProtocol exists (in bootstrap), so it receives the handler here.
	cfg.Transport.SetMessageHandler(func(msg *SyncMessage) {
		sp.handleIncomingMessage(msg)
	})

	return sp, nil
}

// Start begins federation messaging and registers for service change events.
//
// All replicas:
//   - Start transport (receive inbound messages from remote clusters)
//   - Process incoming messages → store → KRT → XDS push
//   - Register federation collections with ambient index
//   - Register for global service change events (enqueue to outgoingCh)
//
// Leader only (activated via BecomeLeader):
//   - Outgoing debounce loop (drain outgoingCh → broadcast to Service Bus)
//   - Snapshot loop (periodic full-sync publish)
//
// This separation ensures all replicas have identical federation state for
// serving ztunnel/envoy, while only one replica publishes to Service Bus.
func (sp *SyncProtocol) Start() error {
	// 1. Start transport first — must be running before we can receive
	if err := sp.transport.Start(); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}
	syncLog.Info("Transport started")

	// 2. Start inbound message processor (all replicas)
	go sp.processIncomingDebounced()

	// Get ambient index — may be nil if not yet available or not enabled
	if sp.ambientIndexGetter != nil {
		sp.ambientIndex = sp.ambientIndexGetter()
	}

	if sp.ambientIndex != nil {
		// 3. Register for push-based global service change events.
		// Events are enqueued to outgoingCh on all replicas but only
		// broadcast by processOutgoingDebounced when isLeader is true.
		sp.ambientIndex.RegisterGlobalServiceHandler(func(prev, curr *model.ServiceInfo, event model.Event) {
			switch event {
			case model.EventAdd, model.EventUpdate:
				sp.handleServiceUpsert(curr)
			case model.EventDelete:
				if prev != nil && prev.Service != nil {
					sp.handleServiceDelete(prev.Service.Hostname)
				}
			}
		})
		syncLog.Info("Registered for push-based global service events")
	} else {
		syncLog.Warn("No ambient index available — federation sync will only exchange peer metadata")
	}

	syncLog.Info("Sync protocol started (awaiting leader election for outbound publishing)")
	return nil
}

// Stop gracefully stops the sync protocol.
func (sp *SyncProtocol) Stop() error {
	sp.StopLeading() // stop outbound if leading
	return sp.transport.Shutdown()
}

// BecomeLeader activates outbound publishing on this replica.
// Called by the leader election callback when this istiod wins the lease.
// It starts the outgoing debounce loop and the snapshot loop, and publishes
// an immediate full-sync snapshot so remote clusters see the new leader's state.
func (sp *SyncProtocol) BecomeLeader(leaderStop <-chan struct{}) {
	if sp.isLeader.Load() {
		return
	}

	sp.leaderStopCh = make(chan struct{})
	sp.isLeader.Store(true)
	syncLog.Info("This replica is now the federation leader — starting outbound publishing")

	// Start the outgoing debounce loop (drains outgoingCh → broadcast)
	go sp.processOutgoingDebounced()

	// Start the snapshot loop (periodic full sync publish)
	if sp.snapshotInterval > 0 {
		go sp.snapshotLoop()
	}

	// Bridge the external leaderStop to our internal leaderStopCh
	go func() {
		select {
		case <-leaderStop:
			sp.StopLeading()
		case <-sp.stopCh:
		}
	}()
}

// StopLeading deactivates outbound publishing on this replica.
// Called when this replica loses the leader lease.
func (sp *SyncProtocol) StopLeading() {
	if !sp.isLeader.CompareAndSwap(true, false) {
		return
	}
	syncLog.Info("This replica lost federation leadership — stopping outbound publishing")
	close(sp.leaderStopCh)
}

// snapshotLoop waits for bootstrap to complete, then publishes an initial
// full-sync snapshot and repeats at snapshotInterval. This provides the
// retained messages that new istiods drain during their own bootstrap.
// Only runs on the leader replica.
func (sp *SyncProtocol) snapshotLoop() {
	// Wait for bootstrap to complete before publishing snapshots.
	select {
	case <-sp.transport.BootstrapDone():
	case <-sp.leaderStopCh:
		return
	case <-sp.stopCh:
		return
	}

	// Publish initial snapshot immediately after bootstrap.
	syncLog.Info("Leader bootstrap complete, publishing initial full sync")
	sp.publishSnapshot()

	ticker := time.NewTicker(sp.snapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sp.publishSnapshot()
		case <-sp.leaderStopCh:
			syncLog.Info("Snapshot loop stopping (lost leadership)")
			return
		case <-sp.stopCh:
			syncLog.Info("Snapshot loop stopping (shutdown)")
			return
		}
	}
}

// publishSnapshot builds and publishes a full-sync snapshot message.
func (sp *SyncProtocol) publishSnapshot() {
	msg := sp.buildFullSyncMessage()
	if msg == nil {
		return
	}
	sp.broadcast(msg)
	syncLog.Infof("Published full-sync snapshot: %d services", len(msg.Services))
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

// handleServiceUpsert enqueues a service add or update for debounced broadcasting.
// Only the leader publishes outbound messages, so non-leader replicas drop events
// to avoid blocking on the bounded outgoingCh.
func (sp *SyncProtocol) handleServiceUpsert(svc *model.ServiceInfo) {
	if svc == nil || svc.Scope != model.Global {
		return
	}
	if !sp.isLeader.Load() {
		return
	}

	syncLog.Debugf("Enqueuing service upsert: %s", svc.Service.Hostname)

	select {
	case sp.outgoingCh <- outgoingEvent{service: svc, event: model.EventUpdate}:
	case <-sp.stopCh:
	}
}

// handleServiceDelete enqueues a service deletion for debounced broadcasting.
// Only the leader publishes outbound messages, so non-leader replicas drop events
// to avoid blocking on the bounded outgoingCh.
func (sp *SyncProtocol) handleServiceDelete(hostname string) {
	if !sp.isLeader.Load() {
		return
	}

	syncLog.Debugf("Enqueuing service delete: %s", hostname)

	select {
	case sp.outgoingCh <- outgoingEvent{hostname: hostname, event: model.EventDelete}:
	case <-sp.stopCh:
	}
}

// processOutgoingDebounced batches local service change events and broadcasts
// them as a single SyncMessage. Events are deduplicated per hostname (last
// event wins), so rapid-fire updates from deployments produce one message.
// Only runs on the leader replica; exits when leadership is lost.
func (sp *SyncProtocol) processOutgoingDebounced() {
	for {
		var ev outgoingEvent
		select {
		case ev = <-sp.outgoingCh:
		case <-sp.leaderStopCh:
			return
		case <-sp.stopCh:
			return
		}

		// Accumulate events, deduplicating by hostname (last event wins).
		pendingServices := make(map[string]*model.ServiceInfo)
		pendingDeletes := make(map[string]struct{})

		applyEvent := func(e outgoingEvent) {
			if e.event == model.EventDelete {
				delete(pendingServices, e.hostname)
				pendingDeletes[e.hostname] = struct{}{}
			} else {
				svc := e.service
				if sp.ambientIndex != nil {
					if enriched := sp.ambientIndex.ServiceWithSANs(svc); enriched != nil {
						svc = enriched
					}
				}
				hostname := string(svc.Service.Hostname)
				delete(pendingDeletes, hostname)
				pendingServices[hostname] = svc
			}
		}

		applyEvent(ev)

		quietTimer := time.NewTimer(debounceAfter)
		maxTimer := time.NewTimer(debounceMax)

	drain:
		for {
			select {
			case ev = <-sp.outgoingCh:
				applyEvent(ev)
				if !quietTimer.Stop() {
					<-quietTimer.C
				}
				quietTimer.Reset(debounceAfter)
			case <-quietTimer.C:
				break drain
			case <-maxTimer.C:
				break drain
			case <-sp.leaderStopCh:
				quietTimer.Stop()
				maxTimer.Stop()
				return
			case <-sp.stopCh:
				quietTimer.Stop()
				maxTimer.Stop()
				return
			}
		}
		quietTimer.Stop()
		maxTimer.Stop()

		if len(pendingServices) == 0 && len(pendingDeletes) == 0 {
			continue
		}

		version := sp.versionVector.increment(sp.localClusterID)

		var services []model.ServiceInfo
		for _, svc := range pendingServices {
			services = append(services, *svc)
		}

		var tombstones []Tombstone
		for hostname := range pendingDeletes {
			tombstones = append(tombstones, sp.tombstones.add(hostname, sp.localClusterID, version))
		}

		msg := &SyncMessage{
			VersionVector:  sp.versionVector.copy(),
			FullSync:       false,
			ClusterID:      sp.localClusterID,
			NetworkGateway: sp.getLocalNetworkGateway(),
			Services:       services,
			Tombstones:     tombstones,
		}

		sp.broadcast(msg)
		syncLog.Infof("Flushed debounced outgoing broadcast: %d services, %d deletes, version=%d",
			len(services), len(tombstones), version)
	}
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
		// No manual XDS push needed: krt's reactive pipeline propagates
		// collection changes through JoinCollection → indexes → RegisterBatch
		// → XDS push automatically.

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

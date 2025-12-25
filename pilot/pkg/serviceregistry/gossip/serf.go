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
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/serf/serf"

	"istio.io/istio/pilot/pkg/features"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/log"
)

var serfLog = log.RegisterScope("gossip-serf", "Serf-based gossip cluster")

const (
	// Event names for custom user events
	eventServiceSync   = "svc-sync"   // Full or incremental service sync
	eventServiceDelete = "svc-delete" // Service deletion (tombstone)

	// Query names for request/response patterns
	queryFullSync = "full-sync" // Request full state from peers

	// Serf configuration
	defaultGossipPort = 7946
	eventBufferSize   = 256
	reconnectTimeout  = 1 * time.Hour
	tombstoneTimeout  = 1 * time.Hour
	maxQueueDepth     = 4096
)

// serfCluster manages gossip-based cluster membership and message broadcasting
// using HashiCorp Serf. It provides:
// - Automatic cluster membership and failure detection
// - Reliable broadcast of service sync messages via user events
// - Request/response queries for full state synchronization
type serfCluster struct {
	mu sync.RWMutex

	// serf is the underlying Serf instance
	serf *serf.Serf

	// eventCh receives Serf events (membership changes, user events)
	eventCh chan serf.Event

	// localClusterID identifies this istiod's cluster
	localClusterID cluster.ID

	// localNodeName is this node's unique identifier
	localNodeName string

	// messageHandler processes incoming sync messages
	messageHandler func(msg *SyncMessage)

	// memberJoinHandler is called when a new member joins
	memberJoinHandler func(memberName string)

	// memberLeaveHandler is called when a member leaves
	memberLeaveHandler func(memberName string)

	// fullSyncBuilder builds the full sync message for query responses
	fullSyncBuilder func() *SyncMessage

	// stopCh signals shutdown
	stopCh chan struct{}

	// started indicates if the cluster has been started
	started bool
}

// serfConfig contains configuration for the Serf cluster.
type serfConfig struct {
	// NodeName is this node's unique identifier (e.g., pod name)
	NodeName string

	// ClusterID is this istiod's cluster identifier
	ClusterID cluster.ID

	// BindAddr is the address to bind for gossip (default: 0.0.0.0)
	BindAddr string

	// BindPort is the port to bind for gossip (default: 7946)
	BindPort int

	// AdvertiseAddr is the address to advertise to other nodes
	AdvertiseAddr string

	// AdvertisePort is the port to advertise (default: same as BindPort)
	AdvertisePort int

	// MessageHandler processes incoming sync messages
	MessageHandler func(msg *SyncMessage)

	// MemberJoinHandler is called when a member joins (optional)
	MemberJoinHandler func(memberName string)

	// MemberLeaveHandler is called when a member leaves (optional)
	MemberLeaveHandler func(memberName string)

	// EncryptionKey for encrypting gossip traffic (optional, 32 bytes for AES-256)
	EncryptionKey []byte

	// StopCh signals shutdown
	StopCh chan struct{}
}

// newserfCluster creates a new Serf-based gossip cluster.
func newSerfCluster(cfg serfConfig) (*serfCluster, error) {
	if cfg.NodeName == "" {
		return nil, fmt.Errorf("NodeName is required")
	}
	if cfg.MessageHandler == nil {
		return nil, fmt.Errorf("MessageHandler is required")
	}

	bindAddr := cfg.BindAddr
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}

	bindPort := cfg.BindPort
	if bindPort == 0 {
		bindPort = defaultGossipPort
	}

	advertiseAddr := cfg.AdvertiseAddr
	if advertiseAddr == "" {
		advertiseAddr = bindAddr
	}

	advertisePort := cfg.AdvertisePort
	if advertisePort == 0 {
		advertisePort = bindPort
	}

	eventCh := make(chan serf.Event, eventBufferSize)

	serfConfig := serf.DefaultConfig()
	serfConfig.NodeName = cfg.NodeName
	serfConfig.EventCh = eventCh

	// Tags for node metadata - other nodes can see these
	serfConfig.Tags = map[string]string{
		"cluster": string(cfg.ClusterID),
		"role":    "istiod",
	}

	// Memberlist (underlying gossip) configuration
	serfConfig.MemberlistConfig.BindAddr = bindAddr
	serfConfig.MemberlistConfig.BindPort = bindPort
	serfConfig.MemberlistConfig.AdvertiseAddr = advertiseAddr
	serfConfig.MemberlistConfig.AdvertisePort = advertisePort

	// Tune for WAN environment (cross-cluster gossip)
	// These settings are more conservative for higher-latency networks
	serfConfig.MemberlistConfig.TCPTimeout = 30 * time.Second
	serfConfig.MemberlistConfig.IndirectChecks = 5 // More indirect checks before suspecting
	serfConfig.MemberlistConfig.RetransmitMult = 6 // More retransmits for reliability
	serfConfig.MemberlistConfig.SuspicionMult = 10 // Longer suspicion period (helps with UDP issues)
	serfConfig.MemberlistConfig.SuspicionMaxTimeoutMult = 10
	serfConfig.MemberlistConfig.GossipNodes = 4
	serfConfig.MemberlistConfig.GossipInterval = 500 * time.Millisecond
	serfConfig.MemberlistConfig.ProbeInterval = 5 * time.Second // Less frequent probes
	serfConfig.MemberlistConfig.ProbeTimeout = 10 * time.Second // Longer timeout for probes
	// Disable dead node reclaim for more stability
	serfConfig.MemberlistConfig.DeadNodeReclaimTime = 5 * time.Minute

	// Queue configuration for user events
	serfConfig.QueueDepthWarning = maxQueueDepth / 2
	serfConfig.MaxQueueDepth = maxQueueDepth

	// Increase user event size limit (default 512 bytes is too small for ServiceInfo)
	// Serf's hard limit is 9KB due to UDP packet constraints
	serfConfig.UserEventSizeLimit = 9 * 1024 // 9KB (max allowed)

	// Timeouts for failed nodes
	serfConfig.ReconnectTimeout = reconnectTimeout
	serfConfig.TombstoneTimeout = tombstoneTimeout

	// Encryption if key is provided
	if len(cfg.EncryptionKey) > 0 {
		serfConfig.MemberlistConfig.SecretKey = cfg.EncryptionKey
	}

	// Disable coordinate updates (we don't need network coordinate estimation)
	serfConfig.DisableCoordinates = true

	// Create Serf instance
	s, err := serf.Create(serfConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create serf: %w", err)
	}

	sc := &serfCluster{
		serf:               s,
		eventCh:            eventCh,
		localClusterID:     cfg.ClusterID,
		localNodeName:      cfg.NodeName,
		messageHandler:     cfg.MessageHandler,
		memberJoinHandler:  cfg.MemberJoinHandler,
		memberLeaveHandler: cfg.MemberLeaveHandler,
		stopCh:             cfg.StopCh,
	}

	return sc, nil
}

// Start begins processing Serf events and optionally joins existing peers.
func (sc *serfCluster) start() error {
	sc.mu.Lock()
	if sc.started {
		sc.mu.Unlock()
		return nil
	}
	sc.started = true
	sc.mu.Unlock()

	// Start event processing loop
	go sc.eventLoop()

	// Start periodic status logging for debugging
	go sc.statusLoop()

	// Join initial peers from configuration
	peers := sc.parseStaticPeers()
	if len(peers) > 0 {
		if _, err := sc.join(peers); err != nil {
			serfLog.Warnf("Failed to join some initial peers: %v", err)
		}
	}

	serfLog.Infof("Serf cluster started: node=%s, cluster=%s", sc.localNodeName, sc.localClusterID)
	return nil
}

// parseStaticPeers parses the PILOT_GOSSIP_PEERS environment variable.
func (sc *serfCluster) parseStaticPeers() []string {
	if features.GossipPeers == "" {
		return nil
	}

	var peers []string
	for _, addr := range strings.Split(features.GossipPeers, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		// Ensure port is specified; add default if not
		if !strings.Contains(addr, ":") {
			addr = net.JoinHostPort(addr, strconv.Itoa(defaultGossipPort))
		}

		peers = append(peers, addr)
	}
	return peers
}

// Join connects to existing cluster members.
// Returns the number of nodes successfully contacted and any error.
func (sc *serfCluster) join(addrs []string) (int, error) {
	n, err := sc.serf.Join(addrs, true)
	if err != nil {
		return n, fmt.Errorf("serf join failed: %w", err)
	}
	serfLog.Infof("Joined %d peers", n)
	return n, nil
}

// Leave gracefully leaves the cluster.
func (sc *serfCluster) leave() error {
	return sc.serf.Leave()
}

// Shutdown forcefully shuts down the Serf agent.
func (sc *serfCluster) shutdown() error {
	return sc.serf.Shutdown()
}

// memberCount returns the number of alive members in the cluster.
func (sc *serfCluster) memberCount() int {
	count := 0
	for _, m := range sc.serf.Members() {
		if m.Status == serf.StatusAlive {
			count++
		}
	}
	return count
}

// BroadcastServiceSync broadcasts a service sync message to all cluster members.
// This uses Serf's Query mechanism for reliable delivery with acknowledgment.
func (sc *serfCluster) broadcastServiceSync(msg *SyncMessage) error {
	// Convert Services to wire format before serialization
	wireMsg := *msg
	wireMsg.WireServices = make([]WireServiceInfo, 0, len(msg.Services))
	for i := range msg.Services {
		wire := ToWireServiceInfo(&msg.Services[i])
		wireMsg.WireServices = append(wireMsg.WireServices, wire)
	}

	data, err := json.Marshal(wireMsg)
	if err != nil {
		return fmt.Errorf("failed to marshal sync message: %w", err)
	}

	// Use Query instead of UserEvent for better reliability over lossy networks.
	params := sc.serf.DefaultQueryParams()
	params.RequestAck = true
	params.Timeout = 5 * time.Second

	serfLog.Infof("Broadcasting %d services (fullSync=%v, size=%d bytes) to %d alive members",
		len(msg.Services), msg.FullSync, len(data), sc.memberCount())

	resp, err := sc.serf.Query(eventServiceSync, data, params)
	if err != nil {
		return fmt.Errorf("failed to broadcast query: %w", err)
	}

	// Process responses - peers will respond with their services for bidirectional sync
	go func() {
		ackCount := 0
		for r := range resp.ResponseCh() {
			if r.From != "" {
				ackCount++

				// Process response payload if it contains services
				if len(r.Payload) > 0 {
					var responseMsg SyncMessage
					if err := json.Unmarshal(r.Payload, &responseMsg); err != nil {
						serfLog.Warnf("Failed to unmarshal response from %s: %v", r.From, err)
						continue
					}

					// Don't process our own messages
					if responseMsg.ClusterID == sc.localClusterID {
						continue
					}

					serfLog.Debugf("Received sync response from %s (cluster %s): %d wireServices",
						r.From, responseMsg.ClusterID, len(responseMsg.WireServices))

					// Convert wire format to ServiceInfo
					convertWireServices(&responseMsg)

					if sc.messageHandler != nil && len(responseMsg.Services) > 0 {
						sc.messageHandler(&responseMsg)
					}
				}
			}
		}
		serfLog.Debugf("Query delivered to %d nodes", ackCount)
	}()

	return nil
}

// eventLoop processes Serf events.
func (sc *serfCluster) eventLoop() {
	for {
		select {
		case e := <-sc.eventCh:
			sc.handleEvent(e)
		case <-sc.stopCh:
			serfLog.Info("Event loop stopping")
			return
		}
	}
}

// statusLoop periodically logs cluster status for debugging.
func (sc *serfCluster) statusLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sc.logStatus()
		case <-sc.stopCh:
			return
		}
	}
}

// logStatus logs current cluster membership and stats.
func (sc *serfCluster) logStatus() {
	members := sc.serf.Members()
	stats := sc.serf.Stats()

	var alive, failed, left int
	var memberInfo []string
	for _, m := range members {
		switch m.Status {
		case serf.StatusAlive:
			alive++
			memberInfo = append(memberInfo, fmt.Sprintf("%s(%s:%d,cluster=%s)",
				m.Name, m.Addr, m.Port, m.Tags["cluster"]))
		case serf.StatusFailed:
			failed++
		case serf.StatusLeft:
			left++
		}
	}

	serfLog.Infof("Cluster status: members=%d (alive=%d, failed=%d, left=%d), health=%s, intent_queue=%s",
		len(members), alive, failed, left, stats["health_score"], stats["intent_queue"])
	if len(memberInfo) > 0 {
		serfLog.Infof("Alive members: %v", memberInfo)
	}
}

// handleEvent processes a single Serf event.
func (sc *serfCluster) handleEvent(e serf.Event) {
	serfLog.Debugf("Received serf event: type=%T", e)
	switch event := e.(type) {
	case serf.MemberEvent:
		sc.handleMemberEvent(event)
	case serf.UserEvent:
		sc.handleUserEvent(event)
	case *serf.Query:
		serfLog.Debugf("Processing Query event: name=%s, sourceNode=%s", event.Name, event.SourceNode())
		sc.handleQuery(event)
	default:
		serfLog.Debugf("Unhandled event type: %T", e)
	}
}

// handleMemberEvent processes membership changes.
func (sc *serfCluster) handleMemberEvent(event serf.MemberEvent) {
	for _, member := range event.Members {
		switch event.EventType() {
		case serf.EventMemberJoin:
			serfLog.Infof("Member joined: %s (cluster=%s)", member.Name, member.Tags["cluster"])
			if sc.memberJoinHandler != nil && member.Name != sc.localNodeName {
				sc.memberJoinHandler(member.Name)
			}

		case serf.EventMemberLeave, serf.EventMemberFailed:
			serfLog.Infof("Member left/failed: %s", member.Name)
			if sc.memberLeaveHandler != nil && member.Name != sc.localNodeName {
				sc.memberLeaveHandler(member.Name)
			}

		case serf.EventMemberReap:
			serfLog.Debugf("Member reaped: %s", member.Name)
		}
	}
}

// convertWireServices converts WireServices to model.ServiceInfo in the message.
// This is a shared helper to avoid duplicating conversion logic.
func convertWireServices(msg *SyncMessage) {
	msg.Services = make([]model.ServiceInfo, 0, len(msg.WireServices))
	for i := range msg.WireServices {
		wire := &msg.WireServices[i]
		if svc := wire.ToServiceInfo(); svc != nil {
			msg.Services = append(msg.Services, *svc)
			serfLog.Debugf("Converted service: %s", svc.Service.Hostname)
		} else {
			serfLog.Warnf("Failed to convert WireService: hostname=%s", wire.Hostname)
		}
	}
}

// handleUserEvent processes custom user events (service sync messages).
// Note: We primarily use Query-based sync now for better reliability,
// but UserEvents may still arrive from older peers or tombstones.
func (sc *serfCluster) handleUserEvent(event serf.UserEvent) {
	switch event.Name {
	case eventServiceDelete:
		var tombstone Tombstone
		if err := json.Unmarshal(event.Payload, &tombstone); err != nil {
			serfLog.Warnf("Failed to unmarshal tombstone: %v", err)
			return
		}

		// Don't process our own tombstones
		if tombstone.ClusterID == sc.localClusterID {
			return
		}

		serfLog.Debugf("Received tombstone from cluster %s: key=%s", tombstone.ClusterID, tombstone.Key)

		// Wrap tombstone in a SyncMessage for consistent handling
		if sc.messageHandler != nil {
			sc.messageHandler(&SyncMessage{
				ClusterID:  tombstone.ClusterID,
				Tombstones: []Tombstone{tombstone},
			})
		}

	default:
		serfLog.Debugf("Unknown user event: %s", event.Name)
	}
}

// handleQuery responds to queries from other nodes.
func (sc *serfCluster) handleQuery(query *serf.Query) {
	switch query.Name {
	case queryFullSync:
		sc.handleFullSyncQuery(query)
	case eventServiceSync:
		// Handle service sync delivered via Query (for better reliability)
		sc.handleServiceSyncQuery(query)
	default:
		serfLog.Debugf("Unknown query: %s", query.Name)
	}
}

// handleServiceSyncQuery processes service sync messages delivered via Query.
func (sc *serfCluster) handleServiceSyncQuery(query *serf.Query) {
	var msg SyncMessage
	if err := json.Unmarshal(query.Payload, &msg); err != nil {
		serfLog.Warnf("Failed to unmarshal sync message from query: %v", err)
		// Acknowledge even on error to prevent retries
		query.Respond(nil)
		return
	}

	// Don't process our own messages
	if msg.ClusterID == sc.localClusterID {
		query.Respond(nil)
		return
	}

	serfLog.Infof("Received query sync from cluster %s: %d wireServices, fullSync=%v",
		msg.ClusterID, len(msg.WireServices), msg.FullSync)

	// Convert wire format back to model.ServiceInfo
	convertWireServices(&msg)

	serfLog.Debugf("Processed query sync from cluster %s: %d services",
		msg.ClusterID, len(msg.Services))

	if sc.messageHandler != nil {
		sc.messageHandler(&msg)
	}

	// Respond with our own services so the sender gets our state
	// This enables bidirectional sync - when c2 queries c1, c1 responds with its services
	sc.mu.RLock()
	builder := sc.fullSyncBuilder
	sc.mu.RUnlock()

	if builder != nil {
		responseMsg := builder()
		if responseMsg != nil && len(responseMsg.Services) > 0 {
			// Convert Services to WireServices for wire transmission
			wireMsg := &SyncMessage{
				VersionVector:  responseMsg.VersionVector,
				FullSync:       responseMsg.FullSync,
				ClusterID:      responseMsg.ClusterID,
				NetworkGateway: responseMsg.NetworkGateway,
				Tombstones:     responseMsg.Tombstones,
				WireServices:   make([]WireServiceInfo, 0, len(responseMsg.Services)),
			}
			for i := range responseMsg.Services {
				wireMsg.WireServices = append(wireMsg.WireServices, ToWireServiceInfo(&responseMsg.Services[i]))
			}

			responseData, err := json.Marshal(wireMsg)
			if err != nil {
				serfLog.Warnf("Failed to marshal sync response: %v", err)
				query.Respond(nil)
				return
			}
			serfLog.Infof("Responding to query from %s with %d services", msg.ClusterID, len(wireMsg.WireServices))
			query.Respond(responseData)
			return
		}
	}

	// No services to respond with
	query.Respond(nil)
}

// SetFullSyncBuilder sets the function that builds full sync messages for query responses.
func (sc *serfCluster) setFullSyncBuilder(builder func() *SyncMessage) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.fullSyncBuilder = builder
}

// handleFullSyncQuery handles a request for full state.
func (sc *serfCluster) handleFullSyncQuery(query *serf.Query) {
	serfLog.Debugf("Received full sync query from %s", query.SourceNode())

	sc.mu.RLock()
	builder := sc.fullSyncBuilder
	sc.mu.RUnlock()

	var msg *SyncMessage
	if builder != nil {
		msg = builder()
	} else {
		// Fallback to empty message if no builder is set
		msg = &SyncMessage{
			ClusterID: sc.localClusterID,
			FullSync:  true,
		}
	}

	data, err := json.Marshal(msg)
	if err != nil {
		serfLog.Warnf("Failed to marshal full sync response: %v", err)
		return
	}

	if err := query.Respond(data); err != nil {
		serfLog.Warnf("Failed to respond to full sync query: %v", err)
	}
}

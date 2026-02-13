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
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/log"
)

var sbLog = log.RegisterScope("federation-servicebus", "Azure Service Bus federation transport")

// ServiceBusTransport implements federation sync using Azure Service Bus Topics.
//
// Architecture:
//   - Each istiod publishes SyncMessages to the "istio-service-sync" topic
//   - Each istiod has a per-cluster subscription with a SQL filter (ClusterID <> self)
//     so it only receives messages from other clusters
//   - No direct istiod-to-istiod connectivity is required
//   - Version vectors in SyncMessage handle out-of-order and duplicate delivery
//
// Service Bus provides at-least-once delivery via peek-lock, which combined with
// the idempotent version-vector logic in SyncProtocol makes duplicates harmless.
type ServiceBusTransport struct {
	client   *azservicebus.Client
	sender   *azservicebus.Sender
	receiver *azservicebus.Receiver

	// adminClient is used for subscription auto-creation/deletion in HA mode.
	// Nil when AutoCreateSubscription is false.
	adminClient *admin.Client

	localClusterID cluster.ID
	topicName      string
	subscriptionID string

	// bootstrapSubscriptionID is the cluster-level subscription used for
	// peek-based bootstrap. Pre-provisioned, never actively consumed.
	// Empty when AutoCreateSubscription is false (single-replica mode).
	bootstrapSubscriptionID string

	// autoCreatedSubscription is true if this transport created its own subscription.
	// Used to determine whether to delete it during shutdown.
	autoCreatedSubscription bool

	messageHandler func(msg *SyncMessage)
	stopCh         chan struct{}

	// stopCtx is cancelled when stopCh closes. Used as the parent context for
	// all Service Bus operations so that in-flight RPCs are cancelled on shutdown.
	stopCtx    context.Context
	stopCancel context.CancelFunc

	// bootstrapDone is closed when the bootstrap drain phase completes and
	// the transport transitions to steady-state message processing.
	bootstrapDone chan struct{}

	// bootstrapDrainTimeout is the receive timeout used during bootstrap drain.
	// If no messages arrive within this duration, the backlog is considered exhausted.
	bootstrapDrainTimeout time.Duration

	// bootstrapBatchSize is how many messages to request per batch during drain.
	bootstrapBatchSize int
}

// ServiceBusConfig contains configuration for the Service Bus transport.
type ServiceBusConfig struct {
	// ConnectionString is the Service Bus connection string.
	// If empty, Azure Workload Identity (DefaultAzureCredential) is used instead.
	ConnectionString string

	// FullyQualifiedNamespace is the Service Bus namespace (e.g., "istio-fed.servicebus.windows.net").
	// Used when authenticating via Workload Identity instead of connection string.
	FullyQualifiedNamespace string

	// TopicName is the Service Bus topic for service sync messages.
	TopicName string

	// SubscriptionName is this cluster's subscription on the topic.
	// Should be unique per cluster (e.g., the cluster ID).
	SubscriptionName string

	// LocalClusterID identifies this istiod's cluster.
	LocalClusterID cluster.ID

	// MessageHandler processes incoming SyncMessages.
	MessageHandler func(msg *SyncMessage)

	// BootstrapDrainTimeout is the receive timeout during bootstrap drain.
	// If no messages arrive within this duration, the backlog is considered exhausted.
	// Defaults to 5s if zero.
	BootstrapDrainTimeout time.Duration

	// BootstrapBatchSize is how many messages to request per batch during drain.
	// Defaults to 100 if zero.
	BootstrapBatchSize int

	// AutoCreateSubscription enables per-replica subscription auto-creation.
	// When true, the transport creates a subscription named <clusterID>-<podName>
	// with autoDeleteOnIdle=30m and a SQL filter (ClusterID <> '<clusterID>').
	// This is required for HA deployments with multiple istiod replicas per cluster.
	// Requires 'Manage' claim or 'Azure Service Bus Data Owner' role.
	AutoCreateSubscription bool

	// BootstrapSubscriptionName is the cluster-level subscription used for
	// peek-based bootstrap when AutoCreateSubscription is true.
	// Pre-provisioned, never actively consumed — messages accumulate and expire
	// via TTL. All replicas peek from this subscription to get initial state
	// from retained full-sync snapshots. Defaults to cluster ID if empty.
	BootstrapSubscriptionName string

	// StopCh signals shutdown.
	StopCh chan struct{}
}

// NewServiceBusTransport creates a ServiceBusTransport backed by Azure Service Bus.
//
// Prerequisites (created externally via Bicep/Terraform/CLI):
//   - Service Bus namespace with a topic matching cfg.TopicName
//   - A subscription on that topic matching cfg.SubscriptionName
//   - SQL filter on the subscription: "ClusterID <> '<localClusterID>'"
//   - Istiod's managed identity has "Azure Service Bus Data Sender" and
//     "Azure Service Bus Data Receiver" roles on the namespace
//
// The MessageHandler field may be nil at construction time; it will be set
// by SyncProtocol before Start() is called.
func NewServiceBusTransport(cfg ServiceBusConfig) (*ServiceBusTransport, error) {
	if cfg.TopicName == "" {
		return nil, fmt.Errorf("TopicName is required")
	}
	if cfg.SubscriptionName == "" {
		return nil, fmt.Errorf("SubscriptionName is required")
	}

	var client *azservicebus.Client
	var adminCl *admin.Client
	var err error

	if cfg.ConnectionString != "" {
		client, err = azservicebus.NewClientFromConnectionString(cfg.ConnectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Service Bus client: %w", err)
		}
		if cfg.AutoCreateSubscription {
			adminCl, err = admin.NewClientFromConnectionString(cfg.ConnectionString, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create Service Bus admin client: %w", err)
			}
		}
	} else if cfg.FullyQualifiedNamespace != "" {
		// Use Azure Workload Identity (DefaultAzureCredential).
		// In AKS with Workload Identity enabled, the pod's service account token
		// is automatically federated to an Azure Managed Identity.
		cred, credErr := azidentity.NewDefaultAzureCredential(nil)
		if credErr != nil {
			return nil, fmt.Errorf("failed to create Azure credential: %w", credErr)
		}
		client, err = azservicebus.NewClient(cfg.FullyQualifiedNamespace, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Service Bus client: %w", err)
		}
		if cfg.AutoCreateSubscription {
			adminCl, err = admin.NewClient(cfg.FullyQualifiedNamespace, cred, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create Service Bus admin client: %w", err)
			}
		}
	} else {
		return nil, fmt.Errorf("either ConnectionString or FullyQualifiedNamespace is required")
	}

	// Create sender for the topic
	sender, err := client.NewSender(cfg.TopicName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create sender for topic %s: %w", cfg.TopicName, err)
	}

	drainTimeout := cfg.BootstrapDrainTimeout
	if drainTimeout == 0 {
		drainTimeout = 5 * time.Second
	}

	batchSize := cfg.BootstrapBatchSize
	if batchSize == 0 {
		batchSize = 100
	}

	// Create a context that is cancelled when stopCh closes, so all in-flight
	// Service Bus RPCs (send, receive, peek, ack) are cancelled on shutdown.
	stopCtx, stopCancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-cfg.StopCh:
			stopCancel()
		case <-stopCtx.Done():
		}
	}()

	t := &ServiceBusTransport{
		client:                client,
		sender:                sender,
		adminClient:           adminCl,
		localClusterID:        cfg.LocalClusterID,
		topicName:             cfg.TopicName,
		subscriptionID:        cfg.SubscriptionName,
		messageHandler:        cfg.MessageHandler,
		bootstrapDone:         make(chan struct{}),
		bootstrapDrainTimeout: drainTimeout,
		bootstrapBatchSize:    batchSize,
		stopCh:                cfg.StopCh,
		stopCtx:               stopCtx,
		stopCancel:            stopCancel,
	}

	// Auto-create per-replica subscription if enabled
	if cfg.AutoCreateSubscription {
		if err := t.ensureSubscription(); err != nil {
			return nil, fmt.Errorf("failed to ensure subscription %s: %w", cfg.SubscriptionName, err)
		}
		t.autoCreatedSubscription = true

		// Store the cluster-level bootstrap subscription name for peek-based bootstrap.
		// This subscription is pre-provisioned and accumulates snapshots via TTL.
		t.bootstrapSubscriptionID = cfg.BootstrapSubscriptionName
		if t.bootstrapSubscriptionID == "" {
			t.bootstrapSubscriptionID = string(cfg.LocalClusterID)
		}
	}

	// Create receiver for this cluster's subscription
	receiver, err := client.NewReceiverForSubscription(cfg.TopicName, cfg.SubscriptionName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create receiver for subscription %s: %w", cfg.SubscriptionName, err)
	}
	t.receiver = receiver

	return t, nil
}

// Start initializes the Service Bus transport with a two-phase bootstrap:
//
// Phase 1 (Drain): Read all retained messages from the subscription backlog
// without triggering individual XDS pushes. Messages are accumulated and merged
// so that only the latest full-sync snapshot per source cluster is kept, plus
// any incrementals newer than that snapshot.
//
// Phase 2 (Steady-state): Deliver the merged bootstrap state via the message
// handler (triggering a single XDS push cycle), then switch to the live
// receive loop where each incoming message is processed individually.
func (t *ServiceBusTransport) Start() error {
	if t.messageHandler == nil {
		return fmt.Errorf("MessageHandler must be set before Start()")
	}

	// Start two-phase bootstrap, then transition to live receive loop
	go t.bootstrapThenReceive()

	sbLog.Infof("Service Bus transport started: topic=%s, subscription=%s, cluster=%s",
		t.topicName, t.subscriptionID, t.localClusterID)
	return nil
}

// BootstrapDone returns a channel that is closed when the bootstrap drain
// phase completes. Callers can use this to defer actions (e.g., broadcasting
// initial full sync) until retained state has been loaded.
func (t *ServiceBusTransport) BootstrapDone() <-chan struct{} {
	return t.bootstrapDone
}

// bootstrapThenReceive runs the two-phase startup sequence.
func (t *ServiceBusTransport) bootstrapThenReceive() {
	var retained []*SyncMessage

	if t.bootstrapSubscriptionID != "" {
		// HA mode: peek from the pre-provisioned cluster-level subscription.
		// This subscription is never actively consumed — messages accumulate
		// and expire via TTL. Peek is non-destructive, so all replicas can
		// peek simultaneously without interference.
		sbLog.Infof("Phase 1: peeking retained messages from bootstrap subscription %s", t.bootstrapSubscriptionID)
		retained = t.peekBootstrapMessages()
	} else {
		// Single-replica mode: drain from the active subscription (original behavior).
		sbLog.Info("Phase 1: draining retained messages for bootstrap")
		retained = t.drainRetainedMessages()
	}

	sbLog.Infof("Phase 1 complete: %d retained messages", len(retained))

	// Merge retained messages: keep only the latest full-sync per cluster,
	// discard superseded snapshots and stale incrementals.
	merged := mergeBootstrapMessages(retained)

	if len(merged) > 0 {
		sbLog.Infof("Phase 2: applying merged state from %d messages", len(merged))
		for _, msg := range merged {
			t.messageHandler(msg)
		}
	}

	close(t.bootstrapDone)
	sbLog.Info("Bootstrap complete, switching to live receive loop")
	t.receiveLoop()
}

// peekBootstrapMessages reads all retained messages from the cluster-level
// bootstrap subscription using non-destructive peek. This is used in HA mode
// where per-replica subscriptions are freshly created and have no retained
// messages. The cluster subscription accumulates snapshots that any replica
// can peek to bootstrap its federation state.
func (t *ServiceBusTransport) peekBootstrapMessages() []*SyncMessage {
	// Create a temporary receiver for the bootstrap subscription (peek only)
	bootstrapReceiver, err := t.client.NewReceiverForSubscription(t.topicName, t.bootstrapSubscriptionID, nil)
	if err != nil {
		sbLog.Warnf("Failed to create bootstrap receiver for subscription %s, starting with empty state: %v",
			t.bootstrapSubscriptionID, err)
		return nil
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		bootstrapReceiver.Close(ctx)
	}()

	var allMessages []*SyncMessage
	fromSeqNum := int64(0)

	for {
		select {
		case <-t.stopCh:
			return allMessages
		default:
		}

		ctx, cancel := context.WithTimeout(t.stopCtx, t.bootstrapDrainTimeout)
		messages, err := bootstrapReceiver.PeekMessages(ctx, t.bootstrapBatchSize, &azservicebus.PeekMessagesOptions{
			FromSequenceNumber: &fromSeqNum,
		})
		cancel()

		if err != nil {
			if ctx.Err() == nil {
				sbLog.Warnf("Error during bootstrap peek, proceeding with %d messages: %v", len(allMessages), err)
			}
			break
		}

		if len(messages) == 0 {
			break
		}

		for _, m := range messages {
			var syncMsg SyncMessage
			if err := json.Unmarshal(m.Body, &syncMsg); err != nil {
				sbLog.Warnf("Bootstrap peek: failed to unmarshal message, skipping: %v", err)
				continue
			}

			// Convert wire format to ServiceInfo
			syncMsg.Services = make([]model.ServiceInfo, 0, len(syncMsg.WireServices))
			for i := range syncMsg.WireServices {
				if svc := syncMsg.WireServices[i].ToServiceInfo(); svc != nil {
					syncMsg.Services = append(syncMsg.Services, *svc)
				}
			}

			allMessages = append(allMessages, &syncMsg)

			// Advance sequence number for next peek batch
			if m.SequenceNumber != nil && *m.SequenceNumber >= fromSeqNum {
				fromSeqNum = *m.SequenceNumber + 1
			}
		}
	}

	sbLog.Infof("Peeked %d messages from bootstrap subscription %s", len(allMessages), t.bootstrapSubscriptionID)
	return allMessages
}

// drainRetainedMessages reads all currently available messages from the
// subscription without waiting for new ones. It uses short receive timeouts
// to detect when the backlog is exhausted: if no messages arrive within
// bootstrapDrainTimeout, the drain is considered complete.
func (t *ServiceBusTransport) drainRetainedMessages() []*SyncMessage {
	var allMessages []*SyncMessage

	for {
		select {
		case <-t.stopCh:
			return allMessages
		default:
		}

		ctx, cancel := context.WithTimeout(t.stopCtx, t.bootstrapDrainTimeout)
		messages, err := t.receiver.ReceiveMessages(ctx, t.bootstrapBatchSize, nil)
		cancel()

		if err != nil {
			// Context deadline exceeded means no more messages — drain complete.
			// Other errors are transient; log and stop draining.
			if ctx.Err() == nil {
				sbLog.Warnf("Error during bootstrap drain, proceeding with %d messages: %v", len(allMessages), err)
			}
			break
		}

		if len(messages) == 0 {
			break
		}

		for _, m := range messages {
			var syncMsg SyncMessage
			if err := json.Unmarshal(m.Body, &syncMsg); err != nil {
				sbLog.Warnf("Bootstrap: failed to unmarshal message, dead-lettering: %v", err)
				dlCtx, dlCancel := context.WithTimeout(t.stopCtx, 5*time.Second)
				if dlErr := t.receiver.DeadLetterMessage(dlCtx, m, nil); dlErr != nil {
					sbLog.Warnf("Bootstrap: failed to dead-letter message: %v", dlErr)
				}
				dlCancel()
				continue
			}

			// Convert wire format to ServiceInfo (same as processMessage)
			syncMsg.Services = make([]model.ServiceInfo, 0, len(syncMsg.WireServices))
			for i := range syncMsg.WireServices {
				if svc := syncMsg.WireServices[i].ToServiceInfo(); svc != nil {
					syncMsg.Services = append(syncMsg.Services, *svc)
				}
			}

			allMessages = append(allMessages, &syncMsg)

			ackCtx, ackCancel := context.WithTimeout(t.stopCtx, 5*time.Second)
			if err := t.receiver.CompleteMessage(ackCtx, m, nil); err != nil {
				sbLog.Warnf("Bootstrap: failed to complete message: %v", err)
			}
			ackCancel()
		}
	}

	return allMessages
}

// mergeBootstrapMessages reduces a list of retained messages to the minimal
// set needed to reconstruct the latest state from each source cluster.
//
// For each cluster:
//   - The latest full-sync snapshot is kept (highest version wins)
//   - All older full syncs are discarded
//   - Incrementals older than the latest full sync are discarded
//   - Incrementals newer than the latest full sync are kept
//
// The result is ordered: each cluster's full sync first, then newer incrementals.
// This ensures the store's version vector merge produces the correct final state.
func mergeBootstrapMessages(messages []*SyncMessage) []*SyncMessage {
	type clusterState struct {
		latestFullSync    *SyncMessage
		fullSyncVersion   uint64
		incrementalsAfter []*SyncMessage
	}

	byCluster := make(map[cluster.ID]*clusterState)

	for _, msg := range messages {
		cid := msg.ClusterID
		state, ok := byCluster[cid]
		if !ok {
			state = &clusterState{}
			byCluster[cid] = state
		}

		version := msg.VersionVector[cid]

		if msg.FullSync {
			if version >= state.fullSyncVersion {
				state.latestFullSync = msg
				state.fullSyncVersion = version
				// Incrementals before this full sync are superseded.
				state.incrementalsAfter = nil
			}
		} else {
			if version > state.fullSyncVersion {
				state.incrementalsAfter = append(state.incrementalsAfter, msg)
			}
			// Incrementals at or below the full sync version are stale, skip them.
		}
	}

	var result []*SyncMessage
	for _, state := range byCluster {
		if state.latestFullSync != nil {
			result = append(result, state.latestFullSync)
		}
		result = append(result, state.incrementalsAfter...)
	}

	return result
}

func (t *ServiceBusTransport) Broadcast(msg *SyncMessage) error {
	return t.sendMessage(msg)
}

// SetMessageHandler sets the handler for incoming SyncMessages.
// This must be called before Start(). It exists to support late-binding
// when the transport is constructed before SyncProtocol wires its handler.
func (t *ServiceBusTransport) SetMessageHandler(handler func(msg *SyncMessage)) {
	t.messageHandler = handler
}

func (t *ServiceBusTransport) Shutdown() error {
	var firstErr error

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := t.receiver.Close(ctx); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("close receiver: %w", err)
	}
	if err := t.sender.Close(ctx); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("close sender: %w", err)
	}

	// Delete auto-created subscription on graceful shutdown.
	// If the pod is OOM-killed or the node crashes, autoDeleteOnIdle handles cleanup.
	if t.autoCreatedSubscription {
		if err := t.deleteSubscription(); err != nil {
			sbLog.Warnf("Failed to delete subscription %s (will auto-delete after idle): %v", t.subscriptionID, err)
		}
	}

	if err := t.client.Close(ctx); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("close client: %w", err)
	}

	sbLog.Info("Service Bus transport shut down")
	return firstErr
}

// ensureSubscription creates a per-replica subscription with a SQL filter and
// autoDeleteOnIdle if it does not already exist. This is the HA mechanism:
// each istiod replica gets its own subscription so it receives ALL messages.
func (t *ServiceBusTransport) ensureSubscription() error {
	if t.adminClient == nil {
		return fmt.Errorf("admin client required for subscription auto-creation")
	}

	ctx, cancel := context.WithTimeout(t.stopCtx, 30*time.Second)
	defer cancel()

	// Check if subscription already exists (pod restart with same name)
	_, err := t.adminClient.GetSubscription(ctx, t.topicName, t.subscriptionID, nil)
	if err == nil {
		sbLog.Infof("Subscription %s already exists, reusing", t.subscriptionID)
		return nil
	}

	// Create the subscription with autoDeleteOnIdle for orphan cleanup
	autoDeleteOnIdle := "PT30M" // 30 minutes
	defaultTTL := "PT10M"       // 10 minutes (matches design doc)
	maxDelivery := int32(10)
	lockDuration := "PT1M"

	_, err = t.adminClient.CreateSubscription(ctx, t.topicName, t.subscriptionID, &admin.CreateSubscriptionOptions{
		Properties: &admin.SubscriptionProperties{
			AutoDeleteOnIdle:         &autoDeleteOnIdle,
			DefaultMessageTimeToLive: &defaultTTL,
			MaxDeliveryCount:         &maxDelivery,
			LockDuration:             &lockDuration,
		},
	})
	if err != nil {
		return fmt.Errorf("create subscription %s: %w", t.subscriptionID, err)
	}

	sbLog.Infof("Created subscription %s with autoDeleteOnIdle=%s", t.subscriptionID, autoDeleteOnIdle)

	// Delete the default $Default rule (matches all messages)
	_, _ = t.adminClient.DeleteRule(ctx, t.topicName, t.subscriptionID, "$Default", nil)

	// Create SQL filter to exclude messages from this cluster
	filterExpr := fmt.Sprintf("ClusterID <> '%s'", string(t.localClusterID))
	ruleName := "filterSelfCluster"
	_, err = t.adminClient.CreateRule(ctx, t.topicName, t.subscriptionID, &admin.CreateRuleOptions{
		Name:   &ruleName,
		Filter: &admin.SQLFilter{Expression: filterExpr},
	})
	if err != nil {
		return fmt.Errorf("create filter rule on subscription %s: %w", t.subscriptionID, err)
	}

	sbLog.Infof("Created SQL filter on subscription %s: %s", t.subscriptionID, filterExpr)
	return nil
}

// deleteSubscription removes this replica's subscription during graceful shutdown.
func (t *ServiceBusTransport) deleteSubscription() error {
	if t.adminClient == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := t.adminClient.DeleteSubscription(ctx, t.topicName, t.subscriptionID, nil)
	if err != nil {
		return err
	}

	sbLog.Infof("Deleted auto-created subscription %s", t.subscriptionID)
	return nil
}

// sendMessage serializes a SyncMessage and publishes it to the topic.
// Services are converted to wire format (protobuf bytes) before JSON marshaling.
func (t *ServiceBusTransport) sendMessage(msg *SyncMessage) error {
	// Convert Services to WireServices for serialization
	wireMsg := *msg
	wireMsg.WireServices = make([]WireServiceInfo, 0, len(msg.Services))
	for i := range msg.Services {
		wireMsg.WireServices = append(wireMsg.WireServices, ToWireServiceInfo(&msg.Services[i]))
	}

	data, err := json.Marshal(wireMsg)
	if err != nil {
		return fmt.Errorf("marshal sync message: %w", err)
	}

	sbMsg := &azservicebus.Message{
		Body: data,
		ApplicationProperties: map[string]any{
			"ClusterID": string(msg.ClusterID),
			"FullSync":  msg.FullSync,
		},
	}

	ctx, cancel := context.WithTimeout(t.stopCtx, 10*time.Second)
	defer cancel()

	if err := t.sender.SendMessage(ctx, sbMsg, nil); err != nil {
		return fmt.Errorf("send to Service Bus: %w", err)
	}

	sbLog.Debugf("Published %d services (fullSync=%v, size=%d bytes) to topic %s",
		len(msg.Services), msg.FullSync, len(data), t.topicName)
	return nil
}

// receiveLoop continuously receives messages from the subscription.
// Messages are deserialized and forwarded to the messageHandler.
// Failed messages are dead-lettered for investigation.
func (t *ServiceBusTransport) receiveLoop() {
	for {
		select {
		case <-t.stopCh:
			sbLog.Info("Receive loop stopping")
			return
		default:
		}

		ctx, cancel := context.WithTimeout(t.stopCtx, 60*time.Second)
		messages, err := t.receiver.ReceiveMessages(ctx, 10, nil)
		cancel()

		if err != nil {
			select {
			case <-t.stopCh:
				return
			default:
			}
			sbLog.Warnf("Receive error (will retry): %v", err)
			time.Sleep(time.Second)
			continue
		}

		for _, m := range messages {
			t.processMessage(m)
		}
	}
}

// processMessage handles a single received Service Bus message.
func (t *ServiceBusTransport) processMessage(m *azservicebus.ReceivedMessage) {
	var syncMsg SyncMessage
	if err := json.Unmarshal(m.Body, &syncMsg); err != nil {
		sbLog.Warnf("Failed to unmarshal message (dead-lettering): %v", err)
		dlCtx, dlCancel := context.WithTimeout(t.stopCtx, 5*time.Second)
		defer dlCancel()
		if dlErr := t.receiver.DeadLetterMessage(dlCtx, m, nil); dlErr != nil {
			sbLog.Warnf("Failed to dead-letter message: %v", dlErr)
		}
		return
	}

	// Convert wire format to ServiceInfo
	syncMsg.Services = make([]model.ServiceInfo, 0, len(syncMsg.WireServices))
	for i := range syncMsg.WireServices {
		if svc := syncMsg.WireServices[i].ToServiceInfo(); svc != nil {
			syncMsg.Services = append(syncMsg.Services, *svc)
		}
	}

	sbLog.Debugf("Received sync from cluster %s: %d services, fullSync=%v",
		syncMsg.ClusterID, len(syncMsg.Services), syncMsg.FullSync)

	t.messageHandler(&syncMsg)

	// Complete (acknowledge) the message
	ackCtx, ackCancel := context.WithTimeout(t.stopCtx, 5*time.Second)
	defer ackCancel()
	if err := t.receiver.CompleteMessage(ackCtx, m, nil); err != nil {
		sbLog.Warnf("Failed to complete message: %v", err)
	}
}

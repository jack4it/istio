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
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

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

	localClusterID cluster.ID
	topicName      string
	subscriptionID string

	messageHandler func(msg *SyncMessage)
	stopCh         chan struct{}

	// fullSyncBuilder is called to build snapshot messages for periodic publishing.
	mu              sync.RWMutex
	fullSyncBuilder func() *SyncMessage

	// snapshotInterval controls how often a full-sync snapshot is published.
	// New istiods bootstrap from these retained snapshots rather than requiring
	// live peers to respond. Set to 0 to disable periodic snapshots.
	snapshotInterval time.Duration
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

	// SnapshotInterval controls periodic full-sync snapshot publishing.
	// New istiods bootstrap from retained snapshots. Defaults to 5m if zero.
	SnapshotInterval time.Duration

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
	var err error

	if cfg.ConnectionString != "" {
		client, err = azservicebus.NewClientFromConnectionString(cfg.ConnectionString, nil)
	} else if cfg.FullyQualifiedNamespace != "" {
		// Use Azure Workload Identity (DefaultAzureCredential).
		// In AKS with Workload Identity enabled, the pod's service account token
		// is automatically federated to an Azure Managed Identity.
		cred, credErr := azidentity.NewDefaultAzureCredential(nil)
		if credErr != nil {
			return nil, fmt.Errorf("failed to create Azure credential: %w", credErr)
		}
		client, err = azservicebus.NewClient(cfg.FullyQualifiedNamespace, cred, nil)
	} else {
		return nil, fmt.Errorf("either ConnectionString or FullyQualifiedNamespace is required")
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create Service Bus client: %w", err)
	}

	// Create sender for the topic
	sender, err := client.NewSender(cfg.TopicName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create sender for topic %s: %w", cfg.TopicName, err)
	}

	// Create receiver for this cluster's subscription
	receiver, err := client.NewReceiverForSubscription(cfg.TopicName, cfg.SubscriptionName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create receiver for subscription %s: %w", cfg.SubscriptionName, err)
	}

	snapshotInterval := cfg.SnapshotInterval
	if snapshotInterval == 0 {
		snapshotInterval = 5 * time.Minute
	}

	return &ServiceBusTransport{
		client:           client,
		sender:           sender,
		receiver:         receiver,
		localClusterID:   cfg.LocalClusterID,
		topicName:        cfg.TopicName,
		subscriptionID:   cfg.SubscriptionName,
		messageHandler:   cfg.MessageHandler,
		snapshotInterval: snapshotInterval,
		stopCh:           cfg.StopCh,
	}, nil
}

func (t *ServiceBusTransport) Start() error {
	if t.messageHandler == nil {
		return fmt.Errorf("MessageHandler must be set before Start()")
	}

	// Start the receive loop
	go t.receiveLoop()

	// Start periodic snapshot publishing for cold-start bootstrap
	if t.snapshotInterval > 0 {
		go t.snapshotLoop()
	}

	sbLog.Infof("Service Bus transport started: topic=%s, subscription=%s, cluster=%s",
		t.topicName, t.subscriptionID, t.localClusterID)
	return nil
}

func (t *ServiceBusTransport) Broadcast(msg *SyncMessage) error {
	return t.sendMessage(msg)
}

func (t *ServiceBusTransport) SetFullSyncBuilder(builder func() *SyncMessage) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fullSyncBuilder = builder
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
	if err := t.client.Close(ctx); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("close client: %w", err)
	}

	sbLog.Info("Service Bus transport shut down")
	return firstErr
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
		dlCtx, dlCancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ackCancel()
	if err := t.receiver.CompleteMessage(ackCtx, m, nil); err != nil {
		sbLog.Warnf("Failed to complete message: %v", err)
	}
}

// snapshotLoop periodically publishes a full-sync snapshot.
// New istiods bootstrap from these retained snapshots instead of requiring
// live peers to respond with their state.
func (t *ServiceBusTransport) snapshotLoop() {
	// Delay first snapshot to allow initial service population
	select {
	case <-time.After(30 * time.Second):
	case <-t.stopCh:
		return
	}

	ticker := time.NewTicker(t.snapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.publishSnapshot()
		case <-t.stopCh:
			sbLog.Info("Snapshot loop stopping")
			return
		}
	}
}

// publishSnapshot builds and publishes a full-sync snapshot message.
func (t *ServiceBusTransport) publishSnapshot() {
	t.mu.RLock()
	builder := t.fullSyncBuilder
	t.mu.RUnlock()

	if builder == nil {
		return
	}

	msg := builder()
	if msg == nil {
		return
	}

	if err := t.sendMessage(msg); err != nil {
		sbLog.Warnf("Failed to publish snapshot: %v", err)
		return
	}

	sbLog.Infof("Published full-sync snapshot: %d services", len(msg.Services))
}

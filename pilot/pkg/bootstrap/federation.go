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

package bootstrap

import (
	"fmt"

	"istio.io/istio/pilot/pkg/features"
	"istio.io/istio/pilot/pkg/leaderelection"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/serviceregistry/aggregate"
	"istio.io/istio/pilot/pkg/serviceregistry/federation"
	"istio.io/istio/pilot/pkg/serviceregistry/kube/controller/ambient"
	"istio.io/istio/pkg/log"
)

// initFederationSync initializes Service Bus–based federation.
// This enables pub/sub synchronization of ambient global services via Azure Service Bus.
//
// Multi-replica behavior:
//   - Each replica auto-creates its own Service Bus subscription (named after the pod)
//     with autoDeleteOnIdle=30m, ensuring all replicas receive ALL inbound messages
//     and maintain complete federation state. Bootstrap peeks from a shared subscription.
//   - Leader election gates outbound publishing: only the leader publishes snapshots
//     and service change events to Service Bus. All replicas process inbound messages.
//   - Version vectors use max(counter+1, time.Now().UnixMilli()) to ensure a new
//     leader always produces versions higher than the previous leader.
func (s *Server) initFederationSync(args *PilotArgs, serviceControllers *aggregate.Controller, fedSource *ambient.FederationSource) error {
	log.Info("Initializing Service Bus federation")

	stopCh := make(chan struct{})

	// Create the Service Bus transport
	sbTransport, err := federation.NewServiceBusTransport(federation.ServiceBusConfig{
		ConnectionString:          features.ServiceBusConnectionString,
		FullyQualifiedNamespace:   features.ServiceBusNamespace,
		TopicName:                 features.ServiceBusTopic,
		SubscriptionName:          args.PodName,
		LocalClusterID:            s.clusterID,
		BootstrapSubscriptionName: features.ServiceBusBootstrapSubscription,
		StopCh:                    stopCh,
		MessageHandler:            nil, // Set by SyncProtocol before Start()
	})
	if err != nil {
		return fmt.Errorf("failed to create Service Bus transport: %w", err)
	}

	// Create sync protocol with the Service Bus transport injected.
	// Federation collections are owned by FederationSource and shared with the ambient index.
	syncProtocol, err := federation.NewSyncProtocol(federation.SyncProtocolConfig{
		LocalClusterID: s.clusterID,
		AmbientIndexGetter: func() model.FederationAmbientIndex {
			return serviceControllers.GetAmbientIndex()
		},
		LocalNetworkGatewayGetter: func() *model.NetworkGateway {
			// Find the local cluster's network gateway by matching cluster ID.
			if s.environment.NetworkManager != nil {
				gateways := s.environment.NetworkManager.AllGateways()
				for i := range gateways {
					if gateways[i].Cluster == s.clusterID {
						return &gateways[i]
					}
				}
			}
			return nil
		},
		TrustDomainGetter: func() string {
			return s.environment.Mesh().GetTrustDomain()
		},
		FederationServices:  fedSource.StaticServices(),
		FederationWorkloads: fedSource.StaticWorkloads(),
		SnapshotInterval:    features.ServiceBusSnapshotInterval,
		ShardExpiry:         features.FederationShardExpiry,
		ShardSweepInterval:  features.FederationShardSweepInterval,
		Transport:           sbTransport,
		StopCh:              stopCh,
	})
	if err != nil {
		return fmt.Errorf("failed to create sync protocol: %w", err)
	}

	// Register health/readiness probe so Kubernetes can detect federation failures.
	s.addReadinessProbe("federation", syncProtocol.IsReady)

	// Start the sync protocol (transport + inbound processing on all replicas)
	s.addStartFunc("federation registry", func(stop <-chan struct{}) error {
		go func() {
			<-stop
			close(stopCh)
			if err := syncProtocol.Stop(); err != nil {
				log.Warnf("Error stopping sync protocol: %v", err)
			}
		}()
		return syncProtocol.Start()
	})

	// Use leader election to gate outbound publishing.
	// Only the leader replica publishes snapshots and service change events.
	// All replicas process inbound messages independently.
	go leaderelection.
		NewLeaseLeaderElection(args.Namespace, args.PodName, leaderelection.FederationController, args.Revision, s.kubeClient).
		AddRunFunction(func(leaderStop <-chan struct{}) {
			syncProtocol.BecomeLeader(leaderStop)
			<-leaderStop
			// StopLeading is called by BecomeLeader's goroutine
		}).
		Run(stopCh)

	log.Info("Service Bus federation initialized")
	return nil
}

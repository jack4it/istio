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
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/serviceregistry/aggregate"
	"istio.io/istio/pilot/pkg/serviceregistry/federation"
	kubecontroller "istio.io/istio/pilot/pkg/serviceregistry/kube/controller"
	"istio.io/istio/pilot/pkg/serviceregistry/provider"
	"istio.io/istio/pilot/pkg/serviceregistry/serviceentry"
	"istio.io/istio/pkg/log"
	"istio.io/istio/pkg/network"
	"istio.io/istio/pkg/util/sets"
)

func (s *Server) ServiceController() *aggregate.Controller {
	return s.environment.ServiceDiscovery.(*aggregate.Controller)
}

// initServiceControllers creates and initializes the service controllers
func (s *Server) initServiceControllers(args *PilotArgs) error {
	serviceControllers := s.ServiceController()

	s.serviceEntryController = serviceentry.NewController(
		s.configController, s.XDSServer,
		s.environment.Watcher,
		serviceentry.WithClusterID(s.clusterID),
	)
	serviceControllers.AddRegistry(s.serviceEntryController)

	registered := sets.New[provider.ID]()
	for _, r := range args.RegistryOptions.Registries {
		serviceRegistry := provider.ID(r)
		if registered.Contains(serviceRegistry) {
			log.Warnf("%s registry specified multiple times.", r)
			continue
		}
		registered.Insert(serviceRegistry)
		log.Infof("Adding %s registry adapter", serviceRegistry)
		switch serviceRegistry {
		case provider.Kubernetes:
			if err := s.initKubeRegistry(args); err != nil {
				return err
			}
		default:
			return fmt.Errorf("service registry %s is not supported", r)
		}
	}

	// Initialize federation if enabled.
	// Must be done before serviceControllers.Run so the sync protocol gets started.
	if features.EnableFederation {
		if err := s.initFederationSync(args, serviceControllers); err != nil {
			return err
		}
	}

	// Defer running of the service controllers.
	s.addStartFunc("service controllers", func(stop <-chan struct{}) error {
		go serviceControllers.Run(stop)
		return nil
	})

	return nil
}

// initKubeRegistry creates all the k8s service controllers under this pilot
func (s *Server) initKubeRegistry(args *PilotArgs) (err error) {
	args.RegistryOptions.KubeOptions.ClusterID = s.clusterID
	args.RegistryOptions.KubeOptions.Revision = args.Revision
	args.RegistryOptions.KubeOptions.KrtDebugger = args.KrtDebugger
	args.RegistryOptions.KubeOptions.Metrics = s.environment
	args.RegistryOptions.KubeOptions.XDSUpdater = s.XDSServer
	args.RegistryOptions.KubeOptions.MeshNetworksWatcher = s.environment.NetworksWatcher
	args.RegistryOptions.KubeOptions.MeshWatcher = s.environment.Watcher
	args.RegistryOptions.KubeOptions.SystemNamespace = args.Namespace
	args.RegistryOptions.KubeOptions.MeshServiceController = s.ServiceController()
	// pass namespace to k8s service registry
	kubecontroller.NewMulticluster(args.PodName,
		args.RegistryOptions.KubeOptions,
		s.serviceEntryController,
		s.istiodCertBundleWatcher,
		args.Revision,
		s.shouldStartNsController(),
		s.environment.ClusterLocal(),
		s.server,
		s.multiclusterController)

	return err
}

// initFederationSync initializes Service Bus–based federation.
// This enables pub/sub synchronization of ambient global services via Azure Service Bus.
func (s *Server) initFederationSync(args *PilotArgs, serviceControllers *aggregate.Controller) error {
	log.Info("Initializing Service Bus federation")

	stopCh := make(chan struct{})

	// Get local network from feature flag
	localNetwork := network.ID(features.FederationLocalNetwork)

	// Determine subscription name: explicit config or fall back to cluster ID
	subscriptionName := features.ServiceBusSubscription
	if subscriptionName == "" {
		subscriptionName = string(s.clusterID)
	}

	// Create the Service Bus transport
	sbTransport, err := federation.NewServiceBusTransport(federation.ServiceBusConfig{
		ConnectionString:        features.ServiceBusConnectionString,
		FullyQualifiedNamespace: features.ServiceBusNamespace,
		TopicName:               features.ServiceBusTopic,
		SubscriptionName:        subscriptionName,
		LocalClusterID:          s.clusterID,
		SnapshotInterval:        features.ServiceBusSnapshotInterval,
		StopCh:                  stopCh,
		MessageHandler:          nil, // Set by SyncProtocol before Start()
	})
	if err != nil {
		return fmt.Errorf("failed to create Service Bus transport: %w", err)
	}

	// Create sync protocol with the Service Bus transport injected
	syncProtocol, err := federation.NewSyncProtocol(federation.SyncProtocolConfig{
		LocalClusterID: s.clusterID,
		AmbientIndexGetter: func() model.FederationAmbientIndex {
			return serviceControllers.GetAmbientIndex()
		},
		LocalNetworkGatewayGetter: func() *model.NetworkGateway {
			if s.environment.NetworkManager != nil && localNetwork != "" {
				gateways := s.environment.NetworkManager.GatewaysForNetwork(localNetwork)
				if len(gateways) > 0 {
					return &gateways[0]
				}
			}
			return nil
		},
		TrustDomainGetter: func() string {
			return s.environment.Mesh().GetTrustDomain()
		},
		XDSUpdater: s.XDSServer,
		Transport:  sbTransport,
		StopCh:     stopCh,
	})
	if err != nil {
		return fmt.Errorf("failed to create sync protocol: %w", err)
	}

	// Start the sync protocol
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

	log.Info("Service Bus federation initialized")
	return nil
}

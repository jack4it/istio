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

package features

import (
	"time"

	"istio.io/istio/pkg/env"
	"istio.io/istio/pkg/log"
)

var (
	EnableAmbient = env.Register(
		"PILOT_ENABLE_AMBIENT",
		false,
		"If enabled, ambient mode can be used. Individual flags configure fine grained enablement; this must be enabled for any ambient functionality.").Get()

	EnableAmbientWaypoints = registerAmbient("PILOT_ENABLE_AMBIENT_WAYPOINTS",
		true, false,
		"If enabled, controllers required for ambient will run. This is required to run ambient mesh.")

	EnableHBONESend = registerAmbient(
		"PILOT_ENABLE_SENDING_HBONE",
		true, false,
		"If enabled, HBONE will be allowed when sending to destinations.")

	EnableSidecarHBONEListening = registerAmbient(
		"PILOT_ENABLE_SIDECAR_LISTENING_HBONE",
		true, false,
		"If enabled, HBONE support can be configured for proxies.")

	EnableAmbientStatus = registerAmbient(
		"AMBIENT_ENABLE_STATUS",
		true, false,
		"If enabled, status messages for ambient mode will be written to resources. "+
			"Currently, this does not do leader election, so may be unsafe to enable with multiple replicas.")

	// Not required for ambient, so disabled by default
	PreferHBONESend = registerAmbient(
		"PILOT_PREFER_SENDING_HBONE",
		false, false,
		"If enabled, HBONE will be preferred when sending to destinations. ")

	DefaultAllowFromWaypoint = registerAmbient(
		"PILOT_AUTO_ALLOW_WAYPOINT_POLICY",
		false, false,
		"If enabled, zTunnel will receive synthetic authorization policies for each workload ALLOW the Waypoint's identity. "+
			"Unless other ALLOW policies are created, this effectively denies traffic that doesn't go through the waypoint.")

	EnableIngressWaypointRouting = registerAmbient("ENABLE_INGRESS_WAYPOINT_ROUTING", true, false,
		"If true, Gateways will call service waypoints if the 'istio.io/ingress-use-waypoint' label set on the Service.")

	EnableAmbientMultiNetwork = registerAmbient("AMBIENT_ENABLE_MULTI_NETWORK", false, false,
		"If true, the multi-network functionality will be enabled.")

	// Using just EnableAmbientMultiNetwork is not enough for users that already experiment with ambient multi-network and use istio from head.
	// While we don't provide much guarantees for alpha features like ambient multi-network, if it's easy to avoid breaking users unnecessarily
	// we should do that.
	//
	// NOTE: This flag does nothing when AMBIENT_ENABLE_MULTI_NETWORK is false.
	EnableAmbientWaypointMultiNetwork = registerAmbient("AMBIENT_ENABLE_MULTI_NETWORK_WAYPOINT", true, false,
		"If true and AMBIENT_ENABLE_MULTI_NETWORK is also true, it will enable waypoints to route requests to clusters on remote networks, "+
			"while by default waypoints will keep traffic local.")

	WaypointLayeredAuthorizationPolicies = env.Register(
		"ENABLE_LAYERED_WAYPOINT_AUTHORIZATION_POLICIES",
		false,
		"If enabled, selector based authorization policies will be enforced as L4 policies in front of the waypoint.").Get()

	// --- Federation: Azure Service Bus multi-cluster sync ---

	EnableFederation = registerAmbient("PILOT_ENABLE_FEDERATION", false, false,
		"If enabled, istiod will use Azure Service Bus pub/sub to synchronize ambient global services with peer istiod instances.")

	FederationLocalNetwork = env.Register("PILOT_FEDERATION_LOCAL_NETWORK", "",
		"The network ID of the local cluster for federation. Used to identify which network gateway to sync.").Get()

	// Service Bus connection settings (provide connection string OR namespace for Workload Identity).

	ServiceBusConnectionString = env.Register("PILOT_SERVICEBUS_CONNECTION_STRING", "",
		"Azure Service Bus connection string. If empty, Azure Workload Identity (DefaultAzureCredential) is used.").Get()

	ServiceBusNamespace = env.Register("PILOT_SERVICEBUS_NAMESPACE", "",
		"Fully qualified Azure Service Bus namespace (e.g., 'istio-fed.servicebus.windows.net'). "+
			"Used when authenticating via Workload Identity instead of connection string.").Get()

	// Service Bus topic and subscription settings.

	ServiceBusTopic = env.Register("PILOT_SERVICEBUS_TOPIC", "istio-service-sync",
		"The Service Bus topic name for federation sync messages.").Get()

	ServiceBusSubscription = env.Register("PILOT_SERVICEBUS_SUBSCRIPTION", "",
		"This cluster's subscription name on the Service Bus topic. Should be unique per cluster (defaults to cluster ID).").Get()

	ServiceBusAutoCreateSubscription = env.Register("PILOT_SERVICEBUS_AUTO_CREATE_SUBSCRIPTION", false,
		"If true, each istiod replica auto-creates its own Service Bus subscription (<clusterID>-<podName>) "+
			"with autoDeleteOnIdle=30m for HA multi-replica deployments. Requires 'Manage' claim or "+
			"'Azure Service Bus Data Owner' role. When false, all replicas share a single pre-provisioned "+
			"subscription — only correct for single-replica deployments.").Get()

	// Operational tuning.

	ServiceBusSnapshotInterval = env.Register("PILOT_SERVICEBUS_SNAPSHOT_INTERVAL", 5*time.Minute,
		"How often to publish a full-sync snapshot to Service Bus for cold-start bootstrap of new istiod instances.").Get()
)

// registerAmbient registers a variable that is allowed only if EnableAmbient is set
func registerAmbient[T env.Parseable](name string, defaultWithAmbient, defaultWithoutAmbient T, description string) T {
	if EnableAmbient {
		return env.Register(name, defaultWithAmbient, description).Get()
	}

	_, f := env.Register(name, defaultWithoutAmbient, description).Lookup()
	if f {
		log.Warnf("ignoring %v; requires PILOT_ENABLE_AMBIENT=true", name)
	}
	return defaultWithoutAmbient
}

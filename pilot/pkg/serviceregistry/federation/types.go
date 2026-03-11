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
	"time"

	"google.golang.org/protobuf/proto"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/util/protoconv"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/workloadapi"
)

// SyncMessage represents a federation synchronization message between istiod peers.
// Note: We use a wire-friendly format (WireServiceInfo) for serialization because
// model.ServiceInfo contains protobuf types that don't serialize cleanly with JSON.
type SyncMessage struct {
	// VersionVector tracks versions per cluster for conflict resolution.
	// Key is cluster ID, value is the monotonically increasing version.
	VersionVector map[cluster.ID]uint64

	// FullSync indicates whether this is a full synchronization (on connect)
	// or an incremental delta update.
	FullSync bool

	// ClusterID identifies the originating cluster for this message.
	ClusterID cluster.ID

	// NetworkGateway contains the originating cluster's east-west gateway info.
	// This is used by remote clusters to create split-horizon workloads.
	NetworkGateway *WireNetworkGateway `json:"networkGateway,omitempty"`

	// Services contains the service information being synchronized.
	// Note: This field is used internally but not for wire serialization.
	Services []model.ServiceInfo `json:"-"`

	// WireServices contains the wire-friendly service representations.
	// This is populated during serialization from Services.
	WireServices []WireServiceInfo `json:"services,omitempty"`

	// DeletedHostnames contains hostnames of services that were deleted.
	// Used in incremental (non-full-sync) messages to remove services from the store.
	DeletedHostnames []string `json:"deletedHostnames,omitempty"`
}

// WireServiceInfo is a wire-friendly representation of ServiceInfo.
// It uses base64-encoded protobuf for the Service field to avoid JSON marshaling issues.
type WireServiceInfo struct {
	// ServiceProto is the base64-encoded protobuf of workloadapi.Service
	ServiceProto []byte `json:"serviceProto,omitempty"`

	// Hostname is the service hostname for quick access without decoding
	Hostname string `json:"hostname,omitempty"`

	// Namespace is the service namespace
	Namespace string `json:"namespace,omitempty"`

	// Scope indicates if the service is local or global
	Scope model.ServiceScope `json:"scope,omitempty"`

	// LabelSelector preserves the service selector metadata needed by ambient internals.
	LabelSelector model.LabelSelector `json:"labelSelector,omitempty"`

	// PortNames preserves the service-port to target-port name mapping.
	PortNames map[int32]model.ServicePortName `json:"portNames,omitempty"`

	// Source identifies the originating resource for status and conflict handling.
	Source model.TypedObject `json:"source,omitempty"`

	// Waypoint preserves service waypoint binding status used by ambient and status code.
	Waypoint model.WaypointBindingStatus `json:"waypoint,omitempty"`

	// CreationTime preserves deterministic merge ordering metadata.
	CreationTime time.Time `json:"creationTime,omitempty"`
}

// WireNetworkGateway represents network gateway information synced via federation.
type WireNetworkGateway struct {
	// Network is the network ID this gateway serves.
	Network string `json:"network"`
	// Cluster is the cluster ID where this gateway resides.
	Cluster string `json:"cluster"`
	// Addr is the gateway address (IP or hostname).
	Addr string `json:"addr"`
	// HBONEPort is the HBONE mTLS port for the gateway.
	HBONEPort uint32 `json:"hbonePort"`
	// ServiceAccount is the service account name used by the gateway.
	// This is used for mTLS identity verification.
	ServiceAccount string `json:"serviceAccount,omitempty"`
	// Namespace is the namespace where the gateway pod resides.
	Namespace string `json:"namespace,omitempty"`
}

// clusterShard holds the synchronized state from a remote cluster.
type clusterShard struct {
	// ClusterID identifies the remote cluster.
	ClusterID cluster.ID

	// Version is the current version for this cluster's data.
	Version uint64

	// Services maps service hostname to service info.
	Services map[string]*model.ServiceInfo

	// NetworkGateway is the network gateway info for this cluster.
	NetworkGateway *WireNetworkGateway
}

// ToWireServiceInfo converts a model.ServiceInfo to wire format.
func ToWireServiceInfo(svc *model.ServiceInfo) WireServiceInfo {
	if svc == nil || svc.Service == nil {
		return WireServiceInfo{}
	}

	wire := WireServiceInfo{
		Hostname:      svc.Service.Hostname,
		Namespace:     svc.Service.Namespace,
		Scope:         svc.Scope,
		LabelSelector: svc.LabelSelector,
		PortNames:     svc.PortNames,
		Source:        svc.Source,
		Waypoint:      svc.Waypoint,
		CreationTime:  svc.CreationTime,
	}

	// Serialize the protobuf Service
	if data, err := proto.Marshal(svc.Service); err == nil {
		wire.ServiceProto = data
	}

	return wire
}

// ToServiceInfo converts wire format back to model.ServiceInfo.
// It also precomputes the AsAddress field for use by AddressInformation.
func (w *WireServiceInfo) ToServiceInfo() *model.ServiceInfo {
	if w == nil || len(w.ServiceProto) == 0 {
		return nil
	}

	svc := &workloadapi.Service{}
	if err := proto.Unmarshal(w.ServiceProto, svc); err != nil {
		return nil
	}

	// Precompute the AddressInfo for use by AddressInformation()
	addr := &workloadapi.Address{
		Type: &workloadapi.Address_Service{
			Service: svc,
		},
	}
	marshaled := protoconv.MessageToAny(addr)

	return &model.ServiceInfo{
		Service:          svc,
		LabelSelector:    w.LabelSelector,
		PortNames:        w.PortNames,
		Source:           w.Source,
		Scope:            w.Scope,
		Waypoint:         w.Waypoint,
		CreationTime:     w.CreationTime,
		MarshaledAddress: marshaled,
		AsAddress: model.AddressInfo{
			Address:   addr,
			Marshaled: marshaled,
		},
	}
}

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
	"encoding/json"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/config/schema/kind"
	"istio.io/istio/pkg/test/util/assert"
	"istio.io/istio/pkg/workloadapi"
)

func TestWireServiceInfoJSONRoundTripPreservesAmbientMetadata(t *testing.T) {
	t.Parallel()

	original := model.ServiceInfo{
		Service: &workloadapi.Service{
			Name:      "svc1",
			Namespace: "ns1",
			Hostname:  "svc1.ns1.svc.company.com",
			Addresses: []*workloadapi.NetworkAddress{{
				Network: "network-remote",
				Address: []byte{10, 0, 0, 1},
			}},
			Ports: []*workloadapi.Port{{
				ServicePort: 80,
				TargetPort:  8080,
			}},
			Waypoint: &workloadapi.GatewayAddress{
				Destination: &workloadapi.GatewayAddress_Hostname{
					Hostname: &workloadapi.NamespacedHostname{
						Namespace: "ns1",
						Hostname:  "wp.ns1.svc.company.com",
					},
				},
				HboneMtlsPort: 15008,
			},
		},
		LabelSelector: model.NewSelector(map[string]string{"app": "a"}),
		PortNames: map[int32]model.ServicePortName{
			80: {
				PortName:       "http",
				TargetPortName: "http-app",
			},
		},
		Source: model.TypedObject{
			NamespacedName: types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			Kind:           kind.Service,
		},
		Scope: model.Global,
		Waypoint: model.WaypointBindingStatus{
			ResourceName:        "gateway.networking.k8s.io/Gateway/ns1/wp",
			IngressUseWaypoint:  true,
			IngressLabelPresent: true,
			Error: &model.StatusMessage{
				Reason:  "Accepted",
				Message: "bound",
			},
		},
		CreationTime: time.Unix(1_700_000_000, 0).UTC(),
	}

	msg := SyncMessage{
		WireServices: []WireServiceInfo{ToWireServiceInfo(&original)},
	}

	data, err := json.Marshal(msg)
	assert.NoError(t, err)

	var decoded SyncMessage
	err = json.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, len(decoded.WireServices), 1)

	roundTripped := decoded.WireServices[0].ToServiceInfo()
	assert.Equal(t, roundTripped.Service, original.Service)
	assert.Equal(t, roundTripped.LabelSelector, original.LabelSelector)
	assert.Equal(t, roundTripped.PortNames, original.PortNames)
	assert.Equal(t, roundTripped.Source, original.Source)
	assert.Equal(t, roundTripped.Scope, original.Scope)
	assert.Equal(t, roundTripped.Waypoint, original.Waypoint)
	assert.Equal(t, roundTripped.CreationTime, original.CreationTime)
	assert.Equal(t, roundTripped.ResourceName(), original.ResourceName())
	assert.Equal(t, roundTripped.MarshaledAddress != nil, true)
	assert.Equal(t, roundTripped.AsAddress.Address.GetService(), original.Service)
}

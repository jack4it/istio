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

package ambient

import (
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/cluster"
	"istio.io/istio/pkg/kube/krt"
)

// FederationSource provides service and workload collections for a single
// remote federated cluster. Data enters as model.ServiceInfo and
// model.WorkloadInfo directly (no Kubernetes object translation).
//
// The underlying StaticCollections participate in krt's reactive pipeline:
// mutations trigger downstream recomputation of indexes, RegisterBatch
// handlers, and XDS pushes automatically.
type FederationSource struct {
	clusterID cluster.ID

	services  krt.StaticCollection[model.ServiceInfo]
	workloads krt.StaticCollection[model.WorkloadInfo]
}

// NewFederationSource creates collections for a remote federated cluster.
// The stopCh controls the lifecycle of the underlying krt collections.
func NewFederationSource(clusterID cluster.ID, stopCh <-chan struct{}) *FederationSource {
	services := krt.NewStaticCollection[model.ServiceInfo](
		nil, nil,
		krt.WithName("federation/"+string(clusterID)+"/Services"),
		krt.WithStop(stopCh),
	)
	workloads := krt.NewStaticCollection[model.WorkloadInfo](
		nil, nil,
		krt.WithName("federation/"+string(clusterID)+"/Workloads"),
		krt.WithStop(stopCh),
	)
	return &FederationSource{
		clusterID: clusterID,
		services:  services,
		workloads: workloads,
	}
}

// ClusterID returns the remote cluster identity for this source.
func (f *FederationSource) ClusterID() cluster.ID {
	return f.clusterID
}

// Services returns the service collection for merging into the ambient index.
func (f *FederationSource) Services() krt.Collection[model.ServiceInfo] {
	return f.services
}

// Workloads returns the workload collection for merging into the ambient index.
func (f *FederationSource) Workloads() krt.Collection[model.WorkloadInfo] {
	return f.workloads
}

// StaticServices returns the underlying StaticCollection for direct manipulation
// by the sync protocol (Reset, UpdateObject, DeleteObject).
func (f *FederationSource) StaticServices() krt.StaticCollection[model.ServiceInfo] {
	return f.services
}

// StaticWorkloads returns the underlying StaticCollection for direct manipulation
// by the sync protocol.
func (f *FederationSource) StaticWorkloads() krt.StaticCollection[model.WorkloadInfo] {
	return f.workloads
}

// ResetServices replaces the full set of services. The StaticCollection
// diffs old vs new and emits granular Add/Update/Delete events.
func (f *FederationSource) ResetServices(svcs []model.ServiceInfo) {
	f.services.Reset(svcs)
}

// ResetWorkloads replaces the full set of workloads.
func (f *FederationSource) ResetWorkloads(wls []model.WorkloadInfo) {
	f.workloads.Reset(wls)
}

// UpsertService adds or updates a single service.
func (f *FederationSource) UpsertService(svc model.ServiceInfo) {
	f.services.UpdateObject(svc)
}

// DeleteService removes a service by its resource name (namespace/hostname).
func (f *FederationSource) DeleteService(resourceName string) {
	f.services.DeleteObject(resourceName)
}

// UpsertWorkload adds or updates a single workload.
func (f *FederationSource) UpsertWorkload(wl model.WorkloadInfo) {
	f.workloads.UpdateObject(wl)
}

// DeleteWorkload removes a workload by its resource name (UID).
func (f *FederationSource) DeleteWorkload(resourceName string) {
	f.workloads.DeleteObject(resourceName)
}

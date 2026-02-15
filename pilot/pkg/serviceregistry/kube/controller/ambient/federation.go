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
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/log"
	"istio.io/istio/pkg/spiffe"
	"istio.io/istio/pkg/util/protomarshal"
	"istio.io/istio/pkg/util/sets"
)

// AllLocalNetworkGlobalServicesWithSANs returns all known globally scoped services with
// SubjectAltNames populated based on the local workloads backing them. This is used for
// federation sync so remote clusters know what identities to expect when connecting.
// It uses localServices (pre-merge) to avoid including federated services.
func (a *index) AllLocalNetworkGlobalServicesWithSANs() []model.ServiceInfo {
	// Use empty WaypointKey for federation context - network is only used for debug logging
	svcCollection := a.localServices
	if svcCollection == nil {
		svcCollection = a.services.Collection
	}
	services := a.allLocalNetworkGlobalServicesFromCollection(svcCollection)

	// Get mesh config for trust domain
	meshCfg := a.meshConfig.Get()
	if meshCfg == nil {
		log.Warnf("Mesh config not available, returning services without SANs")
		return services
	}

	result := make([]model.ServiceInfo, 0, len(services))
	for _, svc := range services {
		// Look up local workloads backing this service
		svcKey := svc.Service.Namespace + "/" + svc.Service.Hostname
		var wls []model.WorkloadInfo
		if a.localWorkloadsByServiceKey != nil {
			wls = a.localWorkloadsByServiceKey.Lookup(svcKey)
		} else {
			wls = a.workloads.ByServiceKey.Lookup(svcKey)
		}

		if len(wls) == 0 {
			// No workloads, return service as-is
			result = append(result, svc)
			continue
		}

		// Collect SANs from all workloads backing this service
		sans := sets.String{}
		for _, wl := range wls {
			san := spiffe.MustGenSpiffeURI(meshCfg.MeshConfig, wl.Workload.Namespace, wl.Workload.ServiceAccount)
			sans.Insert(san)
		}

		if sans.IsEmpty() {
			result = append(result, svc)
			continue
		}

		// Merge with any existing SANs
		sans = sans.Union(sets.New(svc.Service.SubjectAltNames...))

		// Clone and update the service
		newSvcInfo := model.ServiceInfo{
			Service:      protomarshal.Clone(svc.Service),
			Scope:        svc.Scope,
			CreationTime: svc.CreationTime,
		}
		newSvcInfo.Service.SubjectAltNames = sans.UnsortedList()
		result = append(result, newSvcInfo)

		log.Debugf("Added SANs for service %s: %v", svc.Service.Hostname, sans.UnsortedList())
	}

	return result
}

// allLocalNetworkGlobalServicesFromCollection returns global-scoped services from the given
// collection. This is factored out from AllLocalNetworkGlobalServices so that the outbound
// federation path can operate on localServices (pre-merge) without including federated data.
func (a *index) allLocalNetworkGlobalServicesFromCollection(col krt.Collection[model.ServiceInfo]) []model.ServiceInfo {
	var res []model.ServiceInfo
	for _, svc := range col.List() {
		if svc.Scope != model.Global {
			// Check if the service is a waypoint containing global services
			wpSvcs := a.services.ByOwningWaypointHostname.Lookup(NamespaceHostname{
				Namespace: svc.Service.Namespace,
				Hostname:  svc.Service.Hostname,
			})
			if len(wpSvcs) == 0 {
				continue
			}
			for _, resp := range wpSvcs {
				if resp.Scope == model.Global {
					res = append(res, svc)
					break
				}
			}
		} else {
			res = append(res, svc)
		}
	}
	return res
}

// ServiceWithSANs returns a copy of the service with SubjectAltNames populated
// based on local workloads backing it. Uses localWorkloadsByServiceKey to avoid
// including federation workloads in the SAN computation.
func (a *index) ServiceWithSANs(svc *model.ServiceInfo) *model.ServiceInfo {
	if svc == nil || svc.Service == nil {
		return svc
	}

	// Get mesh config for trust domain
	meshCfg := a.meshConfig.Get()
	if meshCfg == nil {
		return svc
	}

	// Look up local workloads backing this service
	svcKey := svc.Service.Namespace + "/" + svc.Service.Hostname
	var wls []model.WorkloadInfo
	if a.localWorkloadsByServiceKey != nil {
		wls = a.localWorkloadsByServiceKey.Lookup(svcKey)
	} else {
		wls = a.workloads.ByServiceKey.Lookup(svcKey)
	}

	if len(wls) == 0 {
		return svc
	}

	// Collect SANs from all workloads backing this service
	sans := sets.String{}
	for _, wl := range wls {
		san := spiffe.MustGenSpiffeURI(meshCfg.MeshConfig, wl.Workload.Namespace, wl.Workload.ServiceAccount)
		sans.Insert(san)
	}

	if sans.IsEmpty() {
		return svc
	}

	// Merge with any existing SANs
	sans = sans.Union(sets.New(svc.Service.SubjectAltNames...))

	// Clone and update the service
	newSvcInfo := &model.ServiceInfo{
		Service:      protomarshal.Clone(svc.Service),
		Scope:        svc.Scope,
		CreationTime: svc.CreationTime,
	}
	newSvcInfo.Service.SubjectAltNames = sans.UnsortedList()

	return newSvcInfo
}

// RegisterGlobalServiceHandler registers a callback that is invoked when
// local global-scoped services change. Uses localServices (pre-merge) so that
// federation data changes don't trigger outbound sync loops.
func (a *index) RegisterGlobalServiceHandler(f model.GlobalServiceHandler) {
	svcCollection := a.localServices
	if svcCollection == nil {
		svcCollection = a.services.Collection
	}
	svcCollection.Register(func(e krt.Event[model.ServiceInfo]) {
		svc := e.Latest()
		// Only notify for global scope services
		if svc.Scope != model.Global {
			return
		}

		var prev, curr *model.ServiceInfo
		var event model.Event

		switch e.Event {
		case controllers.EventAdd:
			curr = e.New
			event = model.EventAdd
		case controllers.EventUpdate:
			prev = e.Old
			curr = e.New
			event = model.EventUpdate
		case controllers.EventDelete:
			prev = e.Old
			event = model.EventDelete
		}

		f(prev, curr, event)
	})
}

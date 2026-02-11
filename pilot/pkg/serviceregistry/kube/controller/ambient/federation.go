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
	"istio.io/istio/pkg/workloadapi"
)

// federationIndex encapsulates federation-synced services and workloads state.
// These are populated by the federation sync protocol when enabled.
type federationIndex struct {
	services   krt.StaticCollection[model.ServiceInfo]
	workloads  krt.StaticCollection[model.WorkloadInfo]
	registered bool
}

// lookupFederation checks federation collections for services and workloads.
func (a *index) lookupFederation(key string) []model.AddressInfo {
	if !a.federation.registered {
		return nil
	}

	// Try service lookup by key (namespace/hostname)
	if svc := a.federation.services.GetKey(key); svc != nil {
		res := []model.AddressInfo{svc.AsAddress}
		// Also get workloads for this service
		res = append(res, a.lookupFederationWorkloadsForService(svc.ResourceName())...)
		return res
	}

	return nil
}

// lookupFederationWorkloadsForService returns federation workloads that serve the given service.
func (a *index) lookupFederationWorkloadsForService(serviceKey string) []model.AddressInfo {
	if !a.federation.registered {
		return nil
	}

	var res []model.AddressInfo
	for _, w := range a.federation.workloads.List() {
		// Check if this workload serves the requested service
		if _, ok := w.Workload.Services[serviceKey]; ok {
			res = append(res, w.AsAddress)
		}
	}
	return res
}

// lookupFederationWorkloadByKey returns a federation workload by its key (UID).
func (a *index) lookupFederationWorkloadByKey(key string) *model.WorkloadInfo {
	if !a.federation.registered {
		return nil
	}
	return a.federation.workloads.GetKey(key)
}

// lookupFederationServiceByKey returns a federation service by its key (namespace/hostname).
func (a *index) lookupFederationServiceByKey(key string) *model.ServiceInfo {
	if !a.federation.registered {
		return nil
	}
	return a.federation.services.GetKey(key)
}

// lookupFederationServiceByAddress returns a federation service by network/ip address.
func (a *index) lookupFederationServiceByAddress(network, ip string) *model.ServiceInfo {
	if !a.federation.registered {
		return nil
	}
	for _, gs := range a.federation.services.List() {
		for _, addr := range gs.Service.Addresses {
			if addr.Network == network && string(addr.Address) == ip {
				return &gs
			}
		}
	}
	return nil
}

// allFederationAddresses returns all federation-synced services and workloads as AddressInfo.
func (a *index) allFederationAddresses() []model.AddressInfo {
	if !a.federation.registered {
		return nil
	}

	var res []model.AddressInfo
	for _, s := range a.federation.services.List() {
		res = append(res, s.AsAddress)
	}
	for _, wl := range a.federation.workloads.List() {
		res = append(res, wl.AsAddress)
	}
	return res
}

// AllLocalNetworkGlobalServicesWithSANs returns all known globally scoped services with
// SubjectAltNames populated based on the local workloads backing them. This is used for
// federation sync so remote clusters know what identities to expect when connecting.
func (a *index) AllLocalNetworkGlobalServicesWithSANs() []model.ServiceInfo {
	// Use empty WaypointKey for federation context - network is only used for debug logging
	services := a.AllLocalNetworkGlobalServices(model.WaypointKey{})

	// Get mesh config for trust domain
	meshCfg := a.meshConfig.Get()
	if meshCfg == nil {
		log.Warnf("Mesh config not available, returning services without SANs")
		return services
	}

	result := make([]model.ServiceInfo, 0, len(services))
	for _, svc := range services {
		// Look up workloads backing this service
		svcKey := svc.Service.Namespace + "/" + svc.Service.Hostname
		wls := a.workloads.ByServiceKey.Lookup(svcKey)

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

// ServiceWithSANs returns a copy of the service with SubjectAltNames populated
// based on local workloads backing it.
func (a *index) ServiceWithSANs(svc *model.ServiceInfo) *model.ServiceInfo {
	if svc == nil || svc.Service == nil {
		return svc
	}

	// Get mesh config for trust domain
	meshCfg := a.meshConfig.Get()
	if meshCfg == nil {
		return svc
	}

	// Look up workloads backing this service
	svcKey := svc.Service.Namespace + "/" + svc.Service.Hostname
	wls := a.workloads.ByServiceKey.Lookup(svcKey)

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
// global-scoped services change. This uses KRT's push-based event system.
func (a *index) RegisterGlobalServiceHandler(f model.GlobalServiceHandler) {
	a.services.Register(func(e krt.Event[model.ServiceInfo]) {
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

// RegisterFederationCollections registers external collections for federation-synced services and workloads.
// This allows federation-synced remote services to be included in the ambient index's Lookup results.
// The parameters are typed as any to satisfy model.FederationAmbientIndex interface (avoiding import cycles).
func (a *index) RegisterFederationCollections(services, workloads any) {
	svcCol := services.(krt.StaticCollection[model.ServiceInfo])
	wlCol := workloads.(krt.StaticCollection[model.WorkloadInfo])

	a.federation.services = svcCol
	a.federation.workloads = wlCol
	a.federation.registered = true

	// Register event handlers to trigger XDS pushes when federation data changes
	if a.XDSUpdater != nil {
		svcCol.RegisterBatch(krt.BatchedEventFilter(
			func(s model.ServiceInfo) *workloadapi.Service {
				return s.Service
			},
			PushXdsAddress(a.XDSUpdater, model.ServiceInfo.ResourceName),
		), false)

		wlCol.RegisterBatch(krt.BatchedEventFilter(
			func(w model.WorkloadInfo) *workloadapi.Workload {
				return w.Workload
			},
			PushXdsAddress(a.XDSUpdater, model.WorkloadInfo.ResourceName),
		), false)
	}

	log.Infof("Registered federation collections with ambient index")
}

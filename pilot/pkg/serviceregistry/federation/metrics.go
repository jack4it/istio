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
	"istio.io/istio/pkg/monitoring"
)

var (
	// Labels

	clusterTag   = monitoring.CreateLabel("cluster")
	typeTag      = monitoring.CreateLabel("type")
	reasonTag    = monitoring.CreateLabel("reason")
	directionTag = monitoring.CreateLabel("direction")
	goroutineTag = monitoring.CreateLabel("goroutine")

	// --- Counters ---

	messagesReceived = monitoring.NewSum(
		"pilot_federation_messages_received_total",
		"Total federation sync messages received from remote clusters.",
	)
	messagesReceivedFullSync    = messagesReceived.With(typeTag.Value("full_sync"))
	messagesReceivedIncremental = messagesReceived.With(typeTag.Value("incremental"))

	messagesSent = monitoring.NewSum(
		"pilot_federation_messages_sent_total",
		"Total federation sync messages sent to remote clusters.",
	)
	messagesSentFullSync    = messagesSent.With(typeTag.Value("full_sync"))
	messagesSentIncremental = messagesSent.With(typeTag.Value("incremental"))

	messagesDropped = monitoring.NewSum(
		"pilot_federation_messages_dropped_total",
		"Total federation messages dropped.",
	)
	messagesDroppedStale          = messagesDropped.With(reasonTag.Value("stale"))
	messagesDroppedUnmarshalError = messagesDropped.With(reasonTag.Value("unmarshal_error"))

	shardsExpired = monitoring.NewSum(
		"pilot_federation_shards_expired_total",
		"Total shards tombstoned due to expiry (no messages received within the expiry window).",
	)

	shardsResurrected = monitoring.NewSum(
		"pilot_federation_shards_resurrected_total",
		"Total shards resurrected from tombstoned state by a strictly newer version.",
	)

	broadcastErrors = monitoring.NewSum(
		"pilot_federation_broadcast_errors_total",
		"Total errors when broadcasting sync messages to Service Bus.",
	)

	panicsRecovered = monitoring.NewSum(
		"pilot_federation_panics_total",
		"Total panics recovered in federation goroutines.",
	)

	// --- Gauges ---

	shardsLive = monitoring.NewGauge(
		"pilot_federation_shards_live",
		"Current number of live (non-tombstoned) federation shards.",
	)

	shardsTombstoned = monitoring.NewGauge(
		"pilot_federation_shards_tombstoned",
		"Current number of tombstoned federation shards.",
	)

	federatedServices = monitoring.NewGauge(
		"pilot_federation_services",
		"Current number of services in the federation store.",
	)

	federatedWorkloads = monitoring.NewGauge(
		"pilot_federation_workloads",
		"Current number of workloads (split-horizon + gateway) in the federation store.",
	)

	isLeader = monitoring.NewGauge(
		"pilot_federation_is_leader",
		"Whether this replica is the federation leader (1) or not (0).",
	)

	// --- Distributions ---

	messageSizeBytes = monitoring.NewDistribution(
		"pilot_federation_message_size_bytes",
		"Size of federation sync messages in bytes.",
		[]float64{256, 1024, 4096, 16384, 65536, 262144},
		monitoring.WithUnit(monitoring.Bytes),
	)
	messageSizeBytesInbound  = messageSizeBytes.With(directionTag.Value("inbound"))
	messageSizeBytesOutbound = messageSizeBytes.With(directionTag.Value("outbound"))

	bootstrapDuration = monitoring.NewDistribution(
		"pilot_federation_bootstrap_duration_seconds",
		"Time taken for the federation bootstrap phase.",
		[]float64{0.1, 0.5, 1, 5, 10, 30, 60},
		monitoring.WithUnit(monitoring.Seconds),
	)

	flushDuration = monitoring.NewDistribution(
		"pilot_federation_flush_duration_seconds",
		"Time taken to flush debounced federation updates to KRT collections.",
		[]float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		monitoring.WithUnit(monitoring.Seconds),
	)
	flushDurationInbound = flushDuration.With(directionTag.Value("inbound"))
)

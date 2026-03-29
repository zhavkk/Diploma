package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HeartbeatsReceived counts heartbeats received by the orchestrator monitor.
	HeartbeatsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ha",
		Subsystem: "monitor",
		Name:      "heartbeats_received_total",
		Help:      "Total number of heartbeats received from node agents.",
	}, []string{"node_id"})

	// FailoverTotal counts failover attempts by reason and result.
	FailoverTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ha",
		Subsystem: "failover",
		Name:      "total",
		Help:      "Total number of failover operations by reason and result.",
	}, []string{"reason", "result"})

	// FailoverDurationSeconds tracks failover duration.
	FailoverDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ha",
		Subsystem: "failover",
		Name:      "duration_seconds",
		Help:      "Duration of failover operations in seconds.",
		Buckets:   prometheus.DefBuckets,
	})

	// NodesHealthy reports the current count of healthy nodes.
	NodesHealthy = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ha",
		Subsystem: "cluster",
		Name:      "nodes_healthy",
		Help:      "Current number of healthy nodes in the cluster.",
	})

	// ReplicationLagBytes reports replication lag per node.
	ReplicationLagBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ha",
		Subsystem: "replication",
		Name:      "lag_bytes",
		Help:      "Replication lag in bytes per replica node.",
	}, []string{"node_id"})

	// ProbeCollectDurationSeconds tracks probe collection latency.
	ProbeCollectDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ha",
		Subsystem: "probe",
		Name:      "collect_duration_seconds",
		Help:      "Duration of probe collect calls in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"node_id"})
)

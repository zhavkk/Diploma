package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HeartbeatsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ha",
		Subsystem: "monitor",
		Name:      "heartbeats_received_total",
		Help:      "Total number of heartbeats received from node agents.",
	}, []string{"node_id"})

	FailoverTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ha",
		Subsystem: "failover",
		Name:      "total",
		Help:      "Total number of failover operations by reason and result.",
	}, []string{"reason", "result"})

	FailoverDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ha",
		Subsystem: "failover",
		Name:      "duration_seconds",
		Help:      "Duration of failover operations in seconds.",
		Buckets:   prometheus.DefBuckets,
	})

	NodesHealthy = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "ha",
		Subsystem: "cluster",
		Name:      "nodes_healthy",
		Help:      "Current number of healthy nodes in the cluster.",
	})

	ReplicationLagBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ha",
		Subsystem: "replication",
		Name:      "lag_bytes",
		Help:      "Replication lag in bytes per replica node.",
	}, []string{"node_id"})

	ProbeCollectDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ha",
		Subsystem: "probe",
		Name:      "collect_duration_seconds",
		Help:      "Duration of probe collect calls in seconds.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"node_id"})

	ReplicationReconfigureTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ha",
		Subsystem: "replication",
		Name:      "reconfigure_total",
		Help:      "Total number of replication reconfiguration attempts by result.",
	}, []string{"result"})

	VersionCompatibilityCheckTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ha",
		Subsystem: "failover",
		Name:      "version_compatibility_check_total",
		Help:      "Total version compatibility checks by result. Possible results: success, incompatible, unparseable.",
	}, []string{"result"})
)

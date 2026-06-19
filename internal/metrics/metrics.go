package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// SubroutineTotal counts subroutine Process calls by subroutine and result (success/error).
	SubroutineTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "search_operator_subroutine_total",
		Help: "Total number of subroutine Process calls by subroutine and result.",
	}, []string{"subroutine", "result"})

	// SubroutineDuration observes subroutine Process duration in seconds by subroutine.
	SubroutineDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "search_operator_subroutine_duration_seconds",
		Help:    "Duration of subroutine Process calls in seconds by subroutine.",
		Buckets: prometheus.DefBuckets,
	}, []string{"subroutine"})

	// OpenSearchOperationsTotal counts OpenSearch operations by operation type and result.
	// operation: create_index, delete_index, index_document, delete_document, update_replicas.
	OpenSearchOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "search_operator_opensearch_operations_total",
		Help: "Total number of OpenSearch operations by type and result.",
	}, []string{"operation", "result"})

	// OpenSearchOperationDuration observes OpenSearch operation duration in seconds by operation type.
	OpenSearchOperationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "search_operator_opensearch_operation_duration_seconds",
		Help:    "Duration of OpenSearch operations in seconds by type.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		SubroutineTotal,
		SubroutineDuration,
		OpenSearchOperationsTotal,
		OpenSearchOperationDuration,
	)
}

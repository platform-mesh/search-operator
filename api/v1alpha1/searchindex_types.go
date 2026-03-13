package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TrackedResource defines a resource type to index
type TrackedResource struct {
	// Group is the API group (empty string for core)
	// +optional
	Group string `json:"group,omitempty"`

	// Version is the API version
	// +required
	Version string `json:"version"`

	// Kind is the resource kind
	// +required
	Kind string `json:"kind"`
}

// SearchIndexSpec defines the desired state of SearchIndex
type SearchIndexSpec struct {
	// IndexPrefix is prepended to all index names for this workspace
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9-]*$`
	// +required
	IndexPrefix string `json:"indexPrefix"`

	// OrganizationClusterID immutable KCP cluster ID (LogicalCluster's kcp.io/cluster annotation); used as OpenSearch index name
	// +kubebuilder:validation:Pattern=`^[a-z0-9][a-z0-9-]*$`
	// +required
	OrganizationClusterID string `json:"organizationClusterID"`

	// +kubebuilder:default=1
	NumberOfShards int32 `json:"numberOfShards"`

	// +kubebuilder:default=1
	NumberOfReplicas int32 `json:"numberOfReplicas"`

	// TrackedResources lists which resource types to index
	// If empty, uses a default set of resources
	// +optional
	TrackedResources []TrackedResource `json:"trackedResources,omitempty"`

	// Paused stops all indexing when true
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// SearchIndexStatus defines the observed state of SearchIndex.
type SearchIndexStatus struct {
	// IndexName is the OpenSearch index name
	// +optional
	IndexName string `json:"indexName,omitempty"`

	// DocumentCount is the number of documents indexed
	// +optional
	DocumentCount int64 `json:"documentCount,omitempty"`

	// LastSyncTime is the last successful sync time
	// +optional
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`

	// Conditions represent the current state of the SearchIndex resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// GetConditions returns the conditions for lifecycle manager compatibility
func (s *SearchIndex) GetConditions() []metav1.Condition {
	return s.Status.Conditions
}

// SetConditions sets the conditions for lifecycle manager compatibility
func (s *SearchIndex) SetConditions(conditions []metav1.Condition) {
	s.Status.Conditions = conditions
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Index",type=string,JSONPath=`.status.indexName`
// +kubebuilder:printcolumn:name="Documents",type=integer,JSONPath=`.status.documentCount`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`

// SearchIndex configures search indexing for a workspace
type SearchIndex struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of SearchIndex
	// +required
	Spec SearchIndexSpec `json:"spec"`

	// Status defines the observed state of SearchIndex
	// +optional
	Status SearchIndexStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SearchIndexList contains a list of SearchIndex
type SearchIndexList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SearchIndex `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SearchIndex{}, &SearchIndexList{})
}

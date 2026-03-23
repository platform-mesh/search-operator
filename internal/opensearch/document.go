package opensearch

import (
	"time"
)

// DefaultIndexMapping returns the default OpenSearch index mapping for workspace and resource documents.
// - payload_raw is stored but not indexed (enabled=false).
// - payload_text stores the full serialized object for full-text search.
func DefaultIndexMapping() string {
	return `{
	  "dynamic": false,
	  "properties": {
	    "id": {"type": "keyword"},
	    "name": {
	      "type": "text",
	      "fields": {
	        "keyword": {"type": "keyword", "ignore_above": 256}
	      }
	    },
	    "type": {"type": "keyword"},
	    "kind": {"type": "keyword"},
	    "namespace": {"type": "keyword"},
	    "api_group": {"type": "keyword"},
	    "api_version": {"type": "keyword"},
	    "cluster_name": {"type": "keyword"},
	    "path": {"type": "keyword"},
	    "workspace_path": {"type": "keyword"},
	    "organization_id": {"type": "keyword"},
	    "organization_name": {"type": "keyword"},
	    "account_id": {"type": "keyword"},
	    "account_name": {"type": "keyword"},
	    "fga_object": {"type": "keyword"},
	    "labels": {"type": "flat_object"},
	    "annotations": {"type": "flat_object"},
	    "permissions": {
	      "type": "nested",
	      "properties": {
	        "user": {"type": "keyword"},
	        "relation": {"type": "keyword"},
	        "object": {"type": "keyword"}
	      }
	    },
	    "created_at": {"type": "date"},
	    "updated_at": {"type": "date"},
	    "payload_raw_json": {"type": "keyword", "index": false, "doc_values": false},
	    "payload_text": {"type": "text"}
	  }
	}`
}

// WorkspaceDocument represents an indexed workspace/account in OpenSearch
type WorkspaceDocument struct {
	// Core workspace/account fields
	ID   string `json:"id"`   // Document ID (typically the cluster name)
	Name string `json:"name"` // Human-readable name
	Type string `json:"type"` // "workspace", "account", or "organization"

	// KCP-specific fields
	ClusterName string `json:"cluster_name"` // Logical cluster name
	Path        string `json:"path"`         // Full path in the KCP hierarchy

	// Organization context (for permission scoping)
	OrganizationID   string `json:"organization_id,omitempty"`
	OrganizationName string `json:"organization_name,omitempty"`

	// Account context (if applicable)
	AccountID   string `json:"account_id,omitempty"`
	AccountName string `json:"account_name,omitempty"`

	// FGAObject is the unique FGA object name for this document (e.g. "core_platform-mesh_io_account:ID/name")
	FGAObject string `json:"fga_object,omitempty"`

	// OpenFGA Permission Tuples for this resource
	Permissions []PermissionTuple `json:"permissions,omitempty"`

	// Timestamps
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`

	// Additional metadata
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// PermissionTuple represents an OpenFGA tuple embedded in the document
// This allows for permission-based filtering at search time
type PermissionTuple struct {
	// User is the subject of the permission (e.g., "user:alice" or "role:admin#assignee")
	User string `json:"user"`
	// Relation is the permission type (e.g., "member", "owner", "viewer")
	Relation string `json:"relation"`
	// Object is the target object (typically matches the document ID)
	Object string `json:"object"`
}

// ResourceDocument represents a generic Kubernetes resource indexed in OpenSearch
type ResourceDocument struct {
	// Core resource identification
	ID        string `json:"id"`        // Unique document ID
	Kind      string `json:"kind"`      // Resource kind
	Name      string `json:"name"`      // Resource name
	Namespace string `json:"namespace"` // Resource namespace

	// API version info
	APIGroup   string `json:"api_group"`
	APIVersion string `json:"api_version"`

	// KCP context
	ClusterName      string `json:"cluster_name"`
	WorkspacePath    string `json:"workspace_path"`
	OrganizationID   string `json:"organization_id,omitempty"`
	OrganizationName string `json:"organization_name,omitempty"`
	AccountID        string `json:"account_id,omitempty"`
	AccountName      string `json:"account_name,omitempty"`

	// FGAObject is the unique FGA object name for this document
	FGAObject string `json:"fga_object,omitempty"`

	// OpenFGA Permission Tuples for this resource
	Permissions []PermissionTuple `json:"permissions,omitempty"`

	// Resource metadata
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`

	// Resource spec and status (arbitrary nested maps from the unstructured object)
	Spec   map[string]interface{} `json:"spec,omitempty"`
	Status map[string]interface{} `json:"status,omitempty"`

	// Timestamps
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`

	// Full raw object payload serialized as JSON, stored but not indexed.
	PayloadRawJSON string `json:"payload_raw_json,omitempty"`

	// Full serialized object payload for full-text search.
	PayloadText string `json:"payload_text,omitempty"`
}

// NewWorkspaceDocument creates a new workspace document with default values
func NewWorkspaceDocument(id, name, workspaceType, clusterName, path string) *WorkspaceDocument {
	return &WorkspaceDocument{
		ID:          id,
		Name:        name,
		Type:        workspaceType,
		ClusterName: clusterName,
		Path:        path,
		UpdatedAt:   time.Now(),
	}
}

// AddPermission adds a permission tuple to the document
func (d *WorkspaceDocument) AddPermission(user, relation, object string) {
	d.Permissions = append(d.Permissions, PermissionTuple{
		User:     user,
		Relation: relation,
		Object:   object,
	})
}

// NewResourceDocument creates a new resource document with default values
func NewResourceDocument(id, kind, name, namespace, clusterName, workspacePath string) *ResourceDocument {
	return &ResourceDocument{
		ID:            id,
		Kind:          kind,
		Name:          name,
		Namespace:     namespace,
		ClusterName:   clusterName,
		WorkspacePath: workspacePath,
		UpdatedAt:     time.Now(),
	}
}

// AddPermission adds a permission tuple to the resource document
func (d *ResourceDocument) AddPermission(user, relation, object string) {
	d.Permissions = append(d.Permissions, PermissionTuple{
		User:     user,
		Relation: relation,
		Object:   object,
	})
}

package opensearch

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// DefaultIndexMapping returns the default OpenSearch index mapping for workspace and resource documents.
// - payload_raw is stored but not indexed (enabled=false).
// - payload_text stores the full serialized object for full-text search.
func DefaultIndexMapping() string {
	mapping, err := BuildSearchIndexMapping(nil, nil, nil)
	if err != nil {
		panic(fmt.Sprintf("build default index mapping: %v", err))
	}

	return mapping
}

// BuildSearchIndexMapping returns the OpenSearch mapping used for SearchIndex-backed
// resource indices.
//
// All configured searchable fields are mapped as text fields with a keyword
// subfield so the same source value supports both full-text queries and exact
// match filtering/sorting. Semantic fields additionally get an explicitly
// materialized sibling <field>_semantic text field.
func BuildSearchIndexMapping(filterableFields, defaultFields, semanticFields []string) (string, error) {
	properties := baseIndexProperties()

	for _, fieldPath := range uniqueFieldPaths(defaultFields, filterableFields, semanticFields) {
		if err := addFieldMapping(properties, fieldPath, searchableTextFieldMapping()); err != nil {
			return "", err
		}
	}
	for _, fieldPath := range uniqueFieldPaths(semanticFields) {
		if err := addFieldMapping(properties, semanticShadowFieldPath(fieldPath), semanticFieldMapping()); err != nil {
			return "", err
		}
	}

	raw, err := json.Marshal(map[string]any{
		"dynamic":    false,
		"properties": properties,
	})
	if err != nil {
		return "", fmt.Errorf("marshal index mapping: %w", err)
	}

	return string(raw), nil
}

func uniqueFieldPaths(groups ...[]string) []string {
	seen := map[string]struct{}{}
	paths := make([]string, 0)
	for _, group := range groups {
		for _, fieldPath := range group {
			normalized := strings.TrimSpace(fieldPath)
			if normalized == "" {
				continue
			}
			if _, ok := seen[normalized]; ok {
				continue
			}
			seen[normalized] = struct{}{}
			paths = append(paths, normalized)
		}
	}

	return paths
}

func baseIndexProperties() map[string]any {
	return map[string]any{
		"id": map[string]any{
			"type": "keyword",
		},
		"name": map[string]any{
			"type": "text",
			"fields": map[string]any{
				"keyword": map[string]any{
					"type":         "keyword",
					"ignore_above": 256,
				},
			},
		},
		"type": map[string]any{
			"type": "keyword",
		},
		"kind": map[string]any{
			"type": "keyword",
		},
		"namespace": map[string]any{
			"type": "keyword",
		},
		"api_group": map[string]any{
			"type": "keyword",
		},
		"api_version": map[string]any{
			"type": "keyword",
		},
		"cluster_name": map[string]any{
			"type": "keyword",
		},
		"path": map[string]any{
			"type": "keyword",
		},
		"workspace_path": map[string]any{
			"type": "keyword",
		},
		"organization_id": map[string]any{
			"type": "keyword",
		},
		"organization_name": map[string]any{
			"type": "keyword",
		},
		"account_id": map[string]any{
			"type": "keyword",
		},
		"account_name": map[string]any{
			"type": "keyword",
		},
		"fga_object": map[string]any{
			"type": "keyword",
		},
		"labels": map[string]any{
			"type": "flat_object",
		},
		"annotations": map[string]any{
			"type": "flat_object",
		},
		"permissions": map[string]any{
			"type": "nested",
			"properties": map[string]any{
				"user": map[string]any{
					"type": "keyword",
				},
				"relation": map[string]any{
					"type": "keyword",
				},
				"object": map[string]any{
					"type": "keyword",
				},
			},
		},
		"created_at": map[string]any{
			"type": "date",
		},
		"updated_at": map[string]any{
			"type": "date",
		},
		"payload_raw_json": map[string]any{
			"type":       "keyword",
			"index":      false,
			"doc_values": false,
		},
		"payload_text": map[string]any{
			"type": "text",
		},
		"spec": map[string]any{
			"type":    "object",
			"dynamic": true,
		},
		"status": map[string]any{
			"type":    "object",
			"dynamic": true,
		},
	}
}

func addFieldMapping(properties map[string]any, fieldPath string, mapping map[string]any) error {
	parts := splitFieldPath(fieldPath)
	if len(parts) == 0 {
		return nil
	}

	current := properties
	for i := 0; i < len(parts)-1; i++ {
		part := parts[i]
		existing, found := current[part]
		if !found {
			next := objectFieldMapping()
			current[part] = next
			current = next["properties"].(map[string]any)
			continue
		}

		existingMap, ok := existing.(map[string]any)
		if !ok {
			return fmt.Errorf("field path %q collides with non-object segment %q", fieldPath, part)
		}

		existingType, _ := existingMap["type"].(string)
		if existingType != "" && existingType != "object" {
			return fmt.Errorf("field path %q collides with non-object mapping at %q", fieldPath, part)
		}

		next, ok := existingMap["properties"].(map[string]any)
		if !ok {
			next = map[string]any{}
			existingMap["type"] = "object"
			existingMap["properties"] = next
		}
		current = next
	}

	last := parts[len(parts)-1]
	if existing, found := current[last]; found {
		existingMap, ok := existing.(map[string]any)
		if !ok {
			return fmt.Errorf("field path %q collides with invalid mapping at %q", fieldPath, last)
		}

		existingType, _ := existingMap["type"].(string)
		if existingType == "object" {
			return fmt.Errorf("field path %q collides with existing object mapping at %q", fieldPath, last)
		}
	}

	current[last] = cloneMapping(mapping)
	return nil
}

func splitFieldPath(fieldPath string) []string {
	rawParts := strings.Split(strings.TrimSpace(fieldPath), ".")
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		parts = append(parts, part)
	}

	return parts
}

func objectFieldMapping() map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": map[string]any{},
	}
}

func searchableTextFieldMapping() map[string]any {
	return map[string]any{
		"type": "text",
		"fields": map[string]any{
			"keyword": map[string]any{
				"type":         "keyword",
				"ignore_above": 256,
			},
		},
	}
}

func semanticFieldMapping() map[string]any {
	return map[string]any{
		"type": "text",
	}
}

func cloneMapping(source map[string]any) map[string]any {
	cloned := make(map[string]any, len(source))
	for key, value := range source {
		if nested, ok := value.(map[string]any); ok {
			cloned[key] = cloneMapping(nested)
			continue
		}
		cloned[key] = value
	}

	return cloned
}

func semanticShadowFieldPath(fieldPath string) string {
	parts := splitFieldPath(fieldPath)
	if len(parts) == 0 {
		return ""
	}

	parts[len(parts)-1] = parts[len(parts)-1] + "_semantic"
	return strings.Join(parts, ".")
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

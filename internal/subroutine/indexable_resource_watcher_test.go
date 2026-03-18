package subroutine

import (
	"encoding/json"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestBuildPayloadSeparatesRawJSONFromText(t *testing.T) {
	resource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "core.platform-mesh.io/v1alpha1",
			"kind":       "Component",
			"metadata": map[string]interface{}{
				"name":          "my-component",
				"namespace":     "default",
				"uid":           "abc-123-def",
				"managedFields": []interface{}{map[string]interface{}{"manager": "kubectl"}},
				"labels": map[string]interface{}{
					"app": "frontend",
				},
			},
			"spec": map[string]interface{}{
				"replicas": float64(3),
				"image":    "nginx:latest",
				"enabled":  true,
			},
		},
	}

	rawJSON, text, err := buildPayload(resource)
	if err != nil {
		t.Fatalf("buildPayload returned error: %v", err)
	}

	// rawJSON must be valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(rawJSON), &parsed); err != nil {
		t.Fatalf("rawJSON is not valid JSON: %v", err)
	}

	// rawJSON should NOT contain managedFields
	if strings.Contains(rawJSON, "managedFields") {
		t.Fatal("rawJSON should not contain managedFields")
	}

	// text should be YAML (contains colons and indentation, no braces for the whole object)
	if !strings.Contains(text, "kind: Component") {
		t.Error("text should contain 'kind: Component'")
	}
	if !strings.Contains(text, "replicas: 3") {
		t.Error("text should contain 'replicas: 3'")
	}
	if !strings.Contains(text, "image: nginx:latest") {
		t.Error("text should contain 'image: nginx:latest'")
	}

	// text should NOT contain managedFields
	if strings.Contains(text, "managedFields") {
		t.Fatal("text should not contain managedFields")
	}
}

func TestExtractAccountFromKCPPath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    string
		wantErr bool
	}{
		{
			name:    "standard account path",
			path:    "root:orgs:sap:workspaces:my-acc",
			want:    "workspaces",
			wantErr: false,
		},
		{
			name:    "direct org path",
			path:    "root:orgs:sap",
			want:    "sap",
			wantErr: false,
		},
		{
			name:    "direct account path",
			path:    "root:orgs:sap:my-acc",
			want:    "my-acc",
			wantErr: false,
		},
		{
			name:    "too short",
			path:    "root",
			want:    "",
			wantErr: true,
		},
		{
			name:    "structural segment resolves account scope",
			path:    "root:orgs:sap:workspaces",
			want:    "workspaces",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractAccountFromKCPPath(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractAccountFromKCPPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("extractAccountFromKCPPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildFGAObjectName(t *testing.T) {
	tests := []struct {
		name      string
		group     string
		kind      string
		clusterID string
		resource  string
		namespace string
		want      string
	}{
		{
			name:      "namespaced resource",
			group:     "core.platform-mesh.io",
			kind:      "Component",
			clusterID: "cluster1",
			resource:  "comp1",
			namespace: "ns1",
			want:      "core_platform-mesh_io_component:cluster1/ns1/comp1",
		},
		{
			name:      "cluster scoped resource",
			group:     "core.platform-mesh.io",
			kind:      "Account",
			clusterID: "cluster1",
			resource:  "acc1",
			namespace: "",
			want:      "core_platform-mesh_io_account:cluster1/acc1",
		},
		{
			name:      "core resource",
			group:     "",
			kind:      "Namespace",
			clusterID: "cluster1",
			resource:  "ns1",
			namespace: "",
			want:      "core_namespace:cluster1/ns1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildFGAObjectName(tt.group, tt.kind, tt.clusterID, tt.resource, tt.namespace); got != tt.want {
				t.Errorf("buildFGAObjectName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMapResourceToFGAObject(t *testing.T) {
	tests := []struct {
		name         string
		group        string
		kind         string
		clusterID    string
		orgID        string
		wantGroup    string
		wantKind     string
		wantCluster  string
	}{
		{
			name:        "account maps to core account",
			group:       "core.platform-mesh.io",
			kind:        "Account",
			clusterID:   "acc-cluster",
			orgID:       "org-cluster",
			wantGroup:   "core.platform-mesh.io",
			wantKind:    "Account",
			wantCluster: "acc-cluster",
		},
		{
			name:        "workspace maps to core account",
			group:       "tenancy.kcp.io",
			kind:        "Workspace",
			clusterID:   "ws-cluster",
			orgID:       "org-cluster",
			wantGroup:   "core.platform-mesh.io",
			wantKind:    "Account",
			wantCluster: "ws-cluster",
		},
		{
			name:        "organization maps to core account rooted at org",
			group:       "core.platform-mesh.io",
			kind:        "Organization",
			clusterID:   "org-resource-cluster",
			orgID:       "org-cluster",
			wantGroup:   "core.platform-mesh.io",
			wantKind:    "Account",
			wantCluster: "org-cluster",
		},
		{
			name:        "unmapped resource keeps own type",
			group:       "core.platform-mesh.io",
			kind:        "Component",
			clusterID:   "component-cluster",
			orgID:       "org-cluster",
			wantGroup:   "core.platform-mesh.io",
			wantKind:    "Component",
			wantCluster: "component-cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotGroup, gotKind, gotCluster := mapResourceToFGAObject(tt.group, tt.kind, tt.clusterID, tt.orgID)
			if gotGroup != tt.wantGroup || gotKind != tt.wantKind || gotCluster != tt.wantCluster {
				t.Fatalf(
					"mapResourceToFGAObject() = (%s, %s, %s), want (%s, %s, %s)",
					gotGroup, gotKind, gotCluster,
					tt.wantGroup, tt.wantKind, tt.wantCluster,
				)
			}
		})
	}
}

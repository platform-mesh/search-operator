package subroutine

import (
	"encoding/json"
	"strings"
	"testing"

	accountv1alpha1 "github.com/platform-mesh/account-operator/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestBuildPayloadSeparatesRawJSONFromText(t *testing.T) {
	resource := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "core.platform-mesh.io/v1alpha1",
			"kind":       "Component",
			"metadata": map[string]any{
				"name":          "my-component",
				"namespace":     "default",
				"uid":           "abc-123-def",
				"managedFields": []any{map[string]any{"manager": "kubectl"}},
				"labels": map[string]any{
					"app": "frontend",
				},
			},
			"spec": map[string]any{
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
	var parsed map[string]any
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
	accountInfo := &accountv1alpha1.AccountInfo{
		Spec: accountv1alpha1.AccountInfoSpec{
			Account: accountv1alpha1.AccountLocation{
				Name:            "teams",
				OriginClusterId: "account-origin",
			},
			Organization: accountv1alpha1.AccountLocation{
				Name:            "sap",
				OriginClusterId: "org-origin",
			},
		},
	}

	tests := []struct {
		name        string
		group       string
		kind        string
		clusterID   string
		accountInfo *accountv1alpha1.AccountInfo
		wantGroup   string
		wantKind    string
		wantCluster string
	}{
		{
			name:        "account maps to core account using OriginClusterId",
			group:       "core.platform-mesh.io",
			kind:        "Account",
			clusterID:   "acc-cluster",
			accountInfo: accountInfo,
			wantGroup:   "core.platform-mesh.io",
			wantKind:    "Account",
			wantCluster: "account-origin",
		},
		{
			name:        "workspace maps to core account using OriginClusterId",
			group:       "tenancy.kcp.io",
			kind:        "Workspace",
			clusterID:   "ws-cluster",
			accountInfo: accountInfo,
			wantGroup:   "core.platform-mesh.io",
			wantKind:    "Account",
			wantCluster: "account-origin",
		},
		{
			name:        "organization maps to core account preserving origin cluster id",
			group:       "core.platform-mesh.io",
			kind:        "Organization",
			clusterID:   "org-resource-cluster",
			accountInfo: accountInfo,
			wantGroup:   "core.platform-mesh.io",
			wantKind:    "Account",
			wantCluster: "org-origin",
		},
		{
			name:        "unmapped resource keeps own type",
			group:       "core.platform-mesh.io",
			kind:        "Component",
			clusterID:   "component-cluster",
			accountInfo: accountInfo,
			wantGroup:   "core.platform-mesh.io",
			wantKind:    "Component",
			wantCluster: "component-cluster",
		},
		{
			name:        "account-like resource without accountInfo falls back to clusterID",
			group:       "core.platform-mesh.io",
			kind:        "Account",
			clusterID:   "acc-cluster",
			accountInfo: nil,
			wantGroup:   "core.platform-mesh.io",
			wantKind:    "Account",
			wantCluster: "acc-cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotGroup, gotKind, gotCluster := mapResourceToFGAObject(tt.group, tt.kind, tt.clusterID, tt.accountInfo)
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

func TestResolveResourceClusterID(t *testing.T) {
	resourceWithAnnotation := &unstructured.Unstructured{}
	resourceWithAnnotation.SetAnnotations(map[string]string{
		kcpClusterAnnotation: "ann-cluster",
	})

	resourceWithoutAnnotation := &unstructured.Unstructured{}

	if got := resolveResourceClusterID(resourceWithAnnotation, "fallback"); got != "ann-cluster" {
		t.Fatalf("resolveResourceClusterID() with annotation = %q, want %q", got, "ann-cluster")
	}

	if got := resolveResourceClusterID(resourceWithoutAnnotation, "fallback"); got != "fallback" {
		t.Fatalf("resolveResourceClusterID() without annotation = %q, want %q", got, "fallback")
	}
}

func TestResolveSpecClusterID(t *testing.T) {
	resource := &unstructured.Unstructured{
		Object: map[string]any{
			"spec": map[string]any{
				"cluster": " spec-cluster ",
			},
		},
	}

	if got := resolveSpecClusterID(resource); got != "spec-cluster" {
		t.Fatalf("resolveSpecClusterID() = %q, want %q", got, "spec-cluster")
	}

	resourceNoSpec := &unstructured.Unstructured{
		Object: map[string]any{},
	}
	if got := resolveSpecClusterID(resourceNoSpec); got != "" {
		t.Fatalf("resolveSpecClusterID() without spec = %q, want empty", got)
	}
}

func TestResolveAccountInfoLookupClusters(t *testing.T) {
	resource := &unstructured.Unstructured{
		Object: map[string]any{
			"spec": map[string]any{
				"cluster": "spec-cluster",
			},
		},
	}

	got := resolveAccountInfoLookupClusters(resource, "ctx-cluster", "resource-cluster")
	want := []string{"resource-cluster", "ctx-cluster", "spec-cluster"}
	if len(got) != len(want) {
		t.Fatalf("resolveAccountInfoLookupClusters() len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("resolveAccountInfoLookupClusters()[%d] = %q, want %q (full=%v)", i, got[i], want[i], got)
		}
	}

	resourceDup := &unstructured.Unstructured{
		Object: map[string]any{
			"spec": map[string]any{
				"cluster": "ctx-cluster",
			},
		},
	}

	gotDup := resolveAccountInfoLookupClusters(resourceDup, "ctx-cluster", "ctx-cluster")
	if len(gotDup) != 1 || gotDup[0] != "ctx-cluster" {
		t.Fatalf("resolveAccountInfoLookupClusters() dedupe = %v, want [ctx-cluster]", gotDup)
	}
}

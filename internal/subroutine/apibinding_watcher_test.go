package subroutine

import (
	"context"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/platform-mesh/search-operator/api/v1alpha1"
)

func TestSearchIndexFieldCollectorDerivesDefaultSemanticAndFilterableFields(t *testing.T) {
	collector := newSearchIndexFieldCollector()
	collector.addFields([]schemaSearchIndexField{
		{Path: "apiVersion", Type: "string"},
		{Path: "certificate", Type: "string"},
		{Path: "clientCrt", Type: "string"},
		{Path: "description", Type: "string"},
		{Path: "kind", Type: "string"},
		{Path: "password", Type: "string"},
		{Path: "spec.description", Type: "string"},
		{Path: "spec.replicas", Type: "integer"},
		{Path: "spec.settings.class", Type: "string"},
		{Path: "status.phase", Type: "string"},
	})

	got := collector.fields()

	wantDefault := []string{"apiVersion", "description", "kind", "spec.description", "spec.replicas", "spec.settings.class", "status.phase"}
	if !reflect.DeepEqual(got.DefaultFields, wantDefault) {
		t.Fatalf("DefaultFields = %v, want %v", got.DefaultFields, wantDefault)
	}

	wantSemantic := []string{"apiVersion", "description", "kind", "spec.description", "spec.settings.class", "status.phase"}
	if !reflect.DeepEqual(got.SemanticFields, wantSemantic) {
		t.Fatalf("SemanticFields = %v, want %v", got.SemanticFields, wantSemantic)
	}

	wantFilterable := []string{"apiVersion", "description", "kind", "spec.description", "spec.replicas", "spec.settings.class", "status.phase"}
	if !reflect.DeepEqual(got.FilterableFields, wantFilterable) {
		t.Fatalf("FilterableFields = %v, want %v", got.FilterableFields, wantFilterable)
	}
}

func TestSearchIndexFieldCollectorAppliesSearchConfig(t *testing.T) {
	collector := newSearchIndexFieldCollector()
	collector.addSearchConfig([]schemaSearchIndexField{
		{Path: "apiVersion", Type: "string"},
		{Path: "kind", Type: "string"},
		{Path: "spec.automation.enabled", Type: "boolean"},
		{Path: "spec.componentType", Type: "string"},
		{Path: "spec.description", Type: "string"},
		{Path: "spec.displayName", Type: "string"},
		{Path: "spec.id", Type: "string"},
		{Path: "status.phase", Type: "string"},
	}, &v1alpha1.SearchConfig{
		Spec: v1alpha1.SearchConfigSpec{
			ExcludedFields: []string{"spec.automation", "status"},
			SemanticFields: []string{"spec.displayName", "spec.description", "spec.longDescription"},
			ExactFields:    []string{"spec.id"},
		},
	})

	got := collector.fields()

	wantDefault := []string{"apiVersion", "kind", "spec.componentType"}
	if !reflect.DeepEqual(got.DefaultFields, wantDefault) {
		t.Fatalf("DefaultFields = %v, want %v", got.DefaultFields, wantDefault)
	}

	wantSemantic := []string{"spec.description", "spec.displayName", "spec.longDescription"}
	if !reflect.DeepEqual(got.SemanticFields, wantSemantic) {
		t.Fatalf("SemanticFields = %v, want %v", got.SemanticFields, wantSemantic)
	}

	wantFilterable := []string{"spec.id"}
	if !reflect.DeepEqual(got.FilterableFields, wantFilterable) {
		t.Fatalf("FilterableFields = %v, want %v", got.FilterableFields, wantFilterable)
	}
}

func TestApplySearchIndexOrgMetadata(t *testing.T) {
	searchIndex := &v1alpha1.SearchIndex{}

	if changed := applySearchIndexOrgMetadata(searchIndex, "abc123"); !changed {
		t.Fatal("applySearchIndexOrgMetadata changed = false, want true")
	}

	if got := searchIndex.Labels[searchIndexOrgClusterIDLabel]; got != "abc123" {
		t.Fatalf("label = %q, want abc123", got)
	}
	if got := searchIndex.Annotations[searchIndexOrgClusterIDAnnotation]; got != "abc123" {
		t.Fatalf("annotation = %q, want abc123", got)
	}

	if changed := applySearchIndexOrgMetadata(searchIndex, "abc123"); changed {
		t.Fatal("applySearchIndexOrgMetadata changed = true, want false")
	}
}

func TestGetSearchIndexPrefersProviderWorkspace(t *testing.T) {
	scheme := newTestScheme(t)
	ctx := context.Background()
	name := buildCanonicalIndexName("pm-orgs", "org-123", "accounts")

	provider := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&v1alpha1.SearchIndex{
			ObjectMeta: testObjectMeta(name),
			Spec: v1alpha1.SearchIndexSpec{
				IndexPrefix:           "pm-orgs",
				OrganizationClusterID: "org-123",
				DefaultFields:         []string{"provider"},
			},
		}).
		Build()
	legacy := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&v1alpha1.SearchIndex{
			ObjectMeta: testObjectMeta(name),
			Spec: v1alpha1.SearchIndexSpec{
				IndexPrefix:           "pm-orgs",
				OrganizationClusterID: "org-123",
				DefaultFields:         []string{"legacy"},
			},
		}).
		Build()

	got, err := getSearchIndex(ctx, provider, legacy, "org-123", "accounts", "pm-orgs")
	if err != nil {
		t.Fatalf("getSearchIndex returned error: %v", err)
	}
	if !reflect.DeepEqual(got.Spec.DefaultFields, []string{"provider"}) {
		t.Fatalf("DefaultFields = %v, want provider result", got.Spec.DefaultFields)
	}
}

func TestGetSearchIndexFallsBackToLegacyOrgWorkspace(t *testing.T) {
	scheme := newTestScheme(t)
	ctx := context.Background()
	name := buildCanonicalIndexName("pm-orgs", "org-123", "accounts")

	provider := fake.NewClientBuilder().WithScheme(scheme).Build()
	legacy := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&v1alpha1.SearchIndex{
			ObjectMeta: testObjectMeta(name),
			Spec: v1alpha1.SearchIndexSpec{
				IndexPrefix:           "pm-orgs",
				OrganizationClusterID: "org-123",
				DefaultFields:         []string{"legacy"},
			},
		}).
		Build()

	got, err := getSearchIndex(ctx, provider, legacy, "org-123", "accounts", "pm-orgs")
	if err != nil {
		t.Fatalf("getSearchIndex returned error: %v", err)
	}
	if !reflect.DeepEqual(got.Spec.DefaultFields, []string{"legacy"}) {
		t.Fatalf("DefaultFields = %v, want legacy fallback result", got.Spec.DefaultFields)
	}
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme returned error: %v", err)
	}
	return scheme
}

func testObjectMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name}
}

package opensearch

import (
	"encoding/json"
	"testing"
)

func TestDefaultIndexMappingIsValidJSON(t *testing.T) {
	mapping, err := DefaultIndexMapping(nil, "")
	if err != nil {
		t.Fatalf("DefaultIndexMapping() returned error: %v", err)
	}
	var js map[string]interface{}
	if err := json.Unmarshal([]byte(mapping), &js); err != nil {
		t.Fatalf("DefaultIndexMapping() returned invalid JSON: %v\nMapping content:\n%s", err, mapping)
	}
}

func TestDefaultIndexMappingIncludesSemanticFields(t *testing.T) {
	mapping, err := DefaultIndexMapping([]string{"description", "spec.summary"}, "model-123")
	if err != nil {
		t.Fatalf("DefaultIndexMapping() returned error: %v", err)
	}

	var js map[string]any
	if err := json.Unmarshal([]byte(mapping), &js); err != nil {
		t.Fatalf("DefaultIndexMapping() returned invalid JSON: %v\nMapping content:\n%s", err, mapping)
	}

	properties := js["properties"].(map[string]any)

	description := properties["description"].(map[string]any)

	//nolint:goconst
	if got := description["type"]; got != "semantic" {
		t.Fatalf("description type = %v, want semantic", got)
	}
	if got := description["model_id"]; got != "model-123" {
		t.Fatalf("description model_id = %v, want model-123", got)
	}

	spec := properties["spec"].(map[string]any)
	specProperties := spec["properties"].(map[string]any)
	summary := specProperties["summary"].(map[string]any)

	//nolint:goconst
	if got := summary["type"]; got != "semantic" {
		t.Fatalf("spec.summary type = %v, want semantic", got)
	}
	if got := summary["model_id"]; got != "model-123" {
		t.Fatalf("spec.summary model_id = %v, want model-123", got)
	}
}

func TestDefaultIndexMappingRequiresSemanticModelID(t *testing.T) {
	if _, err := DefaultIndexMapping([]string{"description"}, ""); err == nil {
		t.Fatal("DefaultIndexMapping() error = nil, want semantic model id validation error")
	}
}

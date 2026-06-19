package opensearch

import (
	"encoding/json"
	"testing"
)

func TestDefaultIndexMappingIsValidJSON(t *testing.T) {
	mapping, err := DefaultIndexMapping(nil, nil, nil, "")
	if err != nil {
		t.Fatalf("DefaultIndexMapping() returned error: %v", err)
	}
	var js map[string]interface{}
	if err := json.Unmarshal([]byte(mapping), &js); err != nil {
		t.Fatalf("DefaultIndexMapping() returned invalid JSON: %v\nMapping content:\n%s", err, mapping)
	}
}

func TestDefaultIndexMappingIncludesSemanticFields(t *testing.T) {
	mapping, err := DefaultIndexMapping(nil, []string{"description", "spec.summary"}, nil, "model-123")
	if err != nil {
		t.Fatalf("DefaultIndexMapping() returned error: %v", err)
	}

	var js map[string]any
	if err := json.Unmarshal([]byte(mapping), &js); err != nil {
		t.Fatalf("DefaultIndexMapping() returned invalid JSON: %v\nMapping content:\n%s", err, mapping)
	}

	properties := js["properties"].(map[string]any)
	semanticFields := properties["semantic_fields"].(map[string]any)
	semanticProperties := semanticFields["properties"].(map[string]any)

	description := semanticProperties["description"].(map[string]any)
	//nolint:goconst
	if got := description["type"]; got != "semantic" {
		t.Fatalf("description type = %v, want semantic", got)
	}
	if got := description["model_id"]; got != "model-123" {
		t.Fatalf("description model_id = %v, want model-123", got)
	}

	spec := semanticProperties["spec"].(map[string]any)
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
	if _, err := DefaultIndexMapping(nil, []string{"description"}, nil, ""); err == nil {
		t.Fatal("DefaultIndexMapping() error = nil, want semantic model id validation error")
	}
}

func TestDefaultIndexMappingMapsFilterableFieldsAsKeywordsWithDynamicTemplate(t *testing.T) {
	mapping, err := DefaultIndexMapping(nil, nil, []string{"spec.region"}, "")
	if err != nil {
		t.Fatalf("DefaultIndexMapping() returned error: %v", err)
	}

	var js map[string]any
	if err := json.Unmarshal([]byte(mapping), &js); err != nil {
		t.Fatalf("DefaultIndexMapping() returned invalid JSON: %v\nMapping content:\n%s", err, mapping)
	}

	properties := js["properties"].(map[string]any)
	filterableFields := properties["filterable_fields"].(map[string]any)
	if got := filterableFields["dynamic"]; got != true {
		t.Fatalf("filterable_fields dynamic = %v, want true", got)
	}

	templates := js["dynamic_templates"].([]any)
	if len(templates) != 1 {
		t.Fatalf("dynamic_templates len = %d, want 1", len(templates))
	}
	template := templates[0].(map[string]any)["filterable_fields_keywords"].(map[string]any)
	if got := template["path_match"]; got != "filterable_fields.*" {
		t.Fatalf("path_match = %v, want filterable_fields.*", got)
	}
	mappingTemplate := template["mapping"].(map[string]any)
	if got := mappingTemplate["type"]; got != "keyword" {
		t.Fatalf("filterable dynamic template type = %v, want keyword", got)
	}
}

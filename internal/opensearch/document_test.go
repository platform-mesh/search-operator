package opensearch

import (
	"encoding/json"
	"testing"
)

func TestDefaultIndexMappingIsValidJSON(t *testing.T) {
	mapping := DefaultIndexMapping()
	var js map[string]interface{}
	if err := json.Unmarshal([]byte(mapping), &js); err != nil {
		t.Fatalf("DefaultIndexMapping() returned invalid JSON: %v\nMapping content:\n%s", err, mapping)
	}
}

func TestBuildSearchIndexMappingAddsConfiguredFields(t *testing.T) {
	mapping, err := BuildSearchIndexMapping(
		[]string{"spec.owner", "spec.labels.team"},
		[]string{"spec.displayName"},
		[]string{"spec.description"},
	)
	if err != nil {
		t.Fatalf("BuildSearchIndexMapping() returned error: %v", err)
	}

	var js map[string]any
	if err := json.Unmarshal([]byte(mapping), &js); err != nil {
		t.Fatalf("BuildSearchIndexMapping() returned invalid JSON: %v\nMapping content:\n%s", err, mapping)
	}

	properties := js["properties"].(map[string]any)
	spec := properties["spec"].(map[string]any)
	if got := spec["type"]; got != "object" {
		t.Fatalf("spec.type = %v, want object", got)
	}
	if got := spec["dynamic"]; got != true {
		t.Fatalf("spec.dynamic = %v, want true", got)
	}

	specProps := spec["properties"].(map[string]any)
	displayName := specProps["displayName"].(map[string]any)
	if got := displayName["type"]; got != "text" {
		t.Fatalf("spec.displayName.type = %v, want text", got)
	}
	displayNameKeyword := displayName["fields"].(map[string]any)["keyword"].(map[string]any)
	if got := displayNameKeyword["type"]; got != "keyword" {
		t.Fatalf("spec.displayName.fields.keyword.type = %v, want keyword", got)
	}

	description := specProps["description"].(map[string]any)
	if got := description["type"]; got != "text" {
		t.Fatalf("spec.description.type = %v, want text", got)
	}
	descriptionKeyword := description["fields"].(map[string]any)["keyword"].(map[string]any)
	if got := descriptionKeyword["type"]; got != "keyword" {
		t.Fatalf("spec.description.fields.keyword.type = %v, want keyword", got)
	}

	descriptionSemantic := specProps["description_semantic"].(map[string]any)
	if got := descriptionSemantic["type"]; got != "text" {
		t.Fatalf("spec.description_semantic.type = %v, want text", got)
	}

	owner := specProps["owner"].(map[string]any)
	if got := owner["type"]; got != "text" {
		t.Fatalf("spec.owner.type = %v, want text", got)
	}
	ownerKeyword := owner["fields"].(map[string]any)["keyword"].(map[string]any)
	if got := ownerKeyword["type"]; got != "keyword" {
		t.Fatalf("spec.owner.fields.keyword.type = %v, want keyword", got)
	}

	labels := specProps["labels"].(map[string]any)
	team := labels["properties"].(map[string]any)["team"].(map[string]any)
	if got := team["type"]; got != "text" {
		t.Fatalf("spec.labels.team.type = %v, want text", got)
	}
	teamKeyword := team["fields"].(map[string]any)["keyword"].(map[string]any)
	if got := teamKeyword["type"]; got != "keyword" {
		t.Fatalf("spec.labels.team.fields.keyword.type = %v, want keyword", got)
	}
}

func TestBuildSearchIndexMappingRejectsConflictingFieldPaths(t *testing.T) {
	_, err := BuildSearchIndexMapping(nil, []string{"spec"}, []string{"spec.description"})
	if err == nil {
		t.Fatal("BuildSearchIndexMapping() error = nil, want conflict")
	}
}

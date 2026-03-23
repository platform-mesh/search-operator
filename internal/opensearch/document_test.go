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

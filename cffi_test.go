//go:build cgo

package chronicle

import (
	"testing"
)

func TestGenerateCHeader(t *testing.T) {
	header := GenerateCHeader()

	// Check header contains expected declarations
	if header == "" {
		t.Error("header should not be empty")
	}

	if !strContains(header, "chronicle_db_t") {
		t.Error("header should define chronicle_db_t")
	}

	if !strContains(header, "chronicle_open") {
		t.Error("header should declare chronicle_open")
	}

	if !strContains(header, "chronicle_close") {
		t.Error("header should declare chronicle_close")
	}

	if !strContains(header, "chronicle_write") {
		t.Error("header should declare chronicle_write")
	}

	if !strContains(header, "chronicle_query") {
		t.Error("header should declare chronicle_query")
	}

	if !strContains(header, "CHRONICLE_OK") {
		t.Error("header should define error codes")
	}
}

func TestDefaultFFIConfig(t *testing.T) {
	config := DefaultFFIConfig()

	if !config.EnableThreadSafety {
		t.Error("thread safety should be enabled by default")
	}

	if config.MaxHandles <= 0 {
		t.Error("max handles should be positive")
	}
}

func TestStringToAggFunc(t *testing.T) {
	tests := []struct {
		input    string
		expected AggFunc
	}{
		{"avg", AggMean},
		{"mean", AggMean},
		{"sum", AggSum},
		{"min", AggMin},
		{"max", AggMax},
		{"count", AggCount},
		{"first", AggFirst},
		{"last", AggLast},
		{"stddev", AggStddev},
		{"rate", AggRate},
		{"unknown", AggNone},
	}

	for _, tt := range tests {
		result := stringToAggFunc(tt.input)
		if result != tt.expected {
			t.Errorf("stringToAggFunc(%q) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestHandleRegistry(t *testing.T) {
	db := setupTestDB(t)

	// Register handle
	handle := registerHandle(db)
	if handle == 0 {
		t.Error("handle should not be 0")
	}

	// Get handle
	retrieved, ok := getHandle(handle)
	if !ok {
		t.Error("should find registered handle")
	}
	if retrieved != db {
		t.Error("retrieved db should match registered db")
	}

	// Remove handle
	removeHandle(handle)

	// Should not find anymore
	_, ok = getHandle(handle)
	if ok {
		t.Error("should not find removed handle")
	}
}

func TestHandleRegistryUnknown(t *testing.T) {
	_, ok := getHandle(99999)
	if ok {
		t.Error("should not find unknown handle")
	}
}

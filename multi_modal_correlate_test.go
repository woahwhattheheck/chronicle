package chronicle

import (
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
)

func TestMultiModalCorrelate_Smoke(t *testing.T) {
	// Smoke test: verify SpanQuery types and functions from multi_modal_correlate.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}

func TestMultiModalGeneratedIDUniqueness(t *testing.T) {
	const workers, perWorker = 8, 256
	ids := make(chan string, workers*perWorker*2)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				ids <- generateLogID()
				ids <- generateSpanID()
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[string]bool)
	for id := range ids {
		if len(id) != 16 {
			t.Fatalf("ID %q is not 16 hexadecimal characters", id)
		}
		if _, err := hex.DecodeString(id); err != nil {
			t.Fatalf("ID %q is not hexadecimal: %v", id, err)
		}
		if seen[id] {
			t.Fatalf("generated duplicate ID %q", id)
		}
		seen[id] = true
	}
	if len(seen) != workers*perWorker*2 {
		t.Fatalf("generated %d unique IDs, want %d", len(seen), workers*perWorker*2)
	}
}

func TestMultiModalGeneratedIDsRetainRecords(t *testing.T) {
	mms, err := NewMultiModalStorage(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mms.Close()
	const count = 32
	for i := 0; i < count; i++ {
		message, service := "Database connection", "database"
		if i%2 != 0 {
			message, service = "User authentication", "auth"
		}
		if err := mms.WriteLog(&MMLogEntry{Message: message}); err != nil {
			t.Fatal(err)
		}
		if err := mms.WriteSpan(&Span{TraceID: fmt.Sprintf("trace-%d", i), Service: service}); err != nil {
			t.Fatal(err)
		}
	}
	logs, err := mms.QueryLogs(&MMLogQuery{Search: "database"})
	if err != nil || len(logs) != count/2 {
		t.Fatalf("database logs: count=%d, error=%v; want %d", len(logs), err, count/2)
	}
	for _, log := range logs {
		if log.Message != "Database connection" {
			t.Fatalf("search included unrelated log: %+v", log)
		}
	}
	spans, err := mms.QuerySpans(&SpanQuery{})
	if err != nil || len(spans) != count {
		t.Fatalf("spans: count=%d, error=%v; want %d", len(spans), err, count)
	}
	filtered, err := mms.QuerySpans(&SpanQuery{Service: "database"})
	if err != nil || len(filtered) != count/2 {
		t.Fatalf("database spans: count=%d, error=%v; want %d", len(filtered), err, count/2)
	}

	// Explicit producer identities remain authoritative and unmodified.
	log := &MMLogEntry{ID: "producer-log", Message: "provided"}
	span := &Span{TraceID: "producer-trace", SpanID: "producer-span"}
	if err := mms.WriteLog(log); err != nil {
		t.Fatal(err)
	}
	if err := mms.WriteSpan(span); err != nil {
		t.Fatal(err)
	}
	if log.ID != "producer-log" || span.SpanID != "producer-span" {
		t.Fatal("explicit producer identity changed")
	}
}

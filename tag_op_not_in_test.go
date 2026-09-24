package chronicle

import (
	"testing"
	"time"
)

func TestTagOpNotInFilter(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	base := time.Now().Add(-time.Hour)
	points := []Point{
		{Metric: "cpu", Tags: map[string]string{"host": "web-1", "env": "prod"}, Value: 10, Timestamp: base.UnixNano()},
		{Metric: "cpu", Tags: map[string]string{"host": "web-2", "env": "prod"}, Value: 20, Timestamp: base.Add(time.Second).UnixNano()},
		{Metric: "cpu", Tags: map[string]string{"host": "db-1", "env": "prod"}, Value: 30, Timestamp: base.Add(2 * time.Second).UnixNano()},
		{Metric: "cpu", Tags: map[string]string{"host": "web-1", "env": "dev"}, Value: 40, Timestamp: base.Add(3 * time.Second).UnixNano()},
	}
	for i, p := range points {
		if err := db.Write(p); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	result, err := db.Execute(&Query{
		Metric: "cpu",
		TagFilters: []TagFilter{
			{Key: "host", Op: TagOpNotIn, Values: []string{"web-1", "web-2"}},
		},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Points) != 1 {
		t.Fatalf("expected 1 point (db-1 only), got %d: %+v", len(result.Points), result.Points)
	}
	if got := result.Points[0].Tags["host"]; got != "db-1" {
		t.Fatalf("expected host=db-1, got %q", got)
	}
}

func TestParseNotInOperator(t *testing.T) {
	var p QueryParser
	q, err := p.Parse("SELECT sum(value) FROM cpu WHERE host NOT IN ('web-1', 'web-2')")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	found := false
	for _, tf := range q.TagFilters {
		if tf.Key == "host" && tf.Op == TagOpNotIn {
			found = true
			if len(tf.Values) != 2 || tf.Values[0] != "web-1" || tf.Values[1] != "web-2" {
				t.Fatalf("unexpected values: %#v", tf.Values)
			}
		}
	}
	if !found {
		t.Fatalf("TagOpNotIn filter not found in %#v", q.TagFilters)
	}
	if TagOpNotIn == TagOpRegex {
		t.Fatal("TagOpNotIn must not share iota value with TagOpRegex")
	}
}

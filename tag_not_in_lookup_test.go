package chronicle

import (
	"fmt"
	"testing"
	"time"
)

func testNotInValues(n int) []string {
	values := make([]string, n)
	for i := range values {
		values[i] = fmt.Sprintf("excluded-%04d", i)
	}
	if n > 1 {
		values[0] = ""
		values[1] = "東京,west"
	}
	return values
}

func TestNotInLookupPreparationIsExecutionLocal(t *testing.T) {
	values := testNotInValues(64)
	q := &Query{Metric: "cpu", TagFilters: []TagFilter{{Key: "host", Op: TagOpNotIn, Values: values}}}

	prepared, err := prepareQueryTagFilters(q)
	if err != nil {
		t.Fatal(err)
	}
	if prepared == q {
		t.Fatal("large NOT IN query must use an execution-local copy")
	}
	if q.TagFilters[0].excludedValues != nil || q.TagFilters[0].compiledRe != nil {
		t.Fatal("caller-owned filter was mutated")
	}
	if prepared.TagFilters[0].excludedValues == nil {
		t.Fatal("prepared filter is missing the lookup")
	}
	if !tagFilterExcludes(prepared.TagFilters[0], values[len(values)-1]) {
		t.Fatal("prepared lookup lost an excluded value")
	}

	small := &Query{Metric: "cpu", TagFilters: []TagFilter{{Key: "host", Op: TagOpNotIn, Values: []string{"a"}}}}
	preparedSmall, err := prepareQueryTagFilters(small)
	if err != nil {
		t.Fatal(err)
	}
	if preparedSmall != small {
		t.Fatal("small NOT IN list should retain the scan path without an execution copy")
	}
}

func TestNotInLookupBoundarySemantics(t *testing.T) {
	matchers := []func(map[string]string, []TagFilter) bool{matchSeriesTagFilters, matchesTagFilters}
	for _, n := range []int{15, 16, 64} {
		values := testNotInValues(n)
		q := &Query{Metric: "cpu", TagFilters: []TagFilter{{Key: "host", Op: TagOpNotIn, Values: values}}}
		prepared, err := prepareQueryTagFilters(q)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range matchers {
			if !match(nil, prepared.TagFilters) {
				t.Fatalf("n=%d: missing host must match NOT IN", n)
			}
			if match(map[string]string{"host": values[n-1]}, prepared.TagFilters) {
				t.Fatalf("n=%d: excluded host matched", n)
			}
			if !match(map[string]string{"host": "allowed"}, prepared.TagFilters) {
				t.Fatalf("n=%d: allowed host was rejected", n)
			}
		}
	}
}

func TestNotInLookupExecuteKeepsCallerImmutable(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	ts := time.Now().Add(-time.Hour).Truncate(time.Hour).UnixNano()
	values := testNotInValues(64)
	for i, host := range values {
		if err := db.Write(Point{Metric: "lookup", Tags: map[string]string{"host": host}, Value: 1, Timestamp: ts + int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Write(Point{Metric: "lookup", Tags: map[string]string{"host": "allowed"}, Value: 1, Timestamp: ts + 100}); err != nil {
		t.Fatal(err)
	}
	if err := db.Write(Point{Metric: "lookup", Tags: nil, Value: 1, Timestamp: ts + 101}); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	q := &Query{Metric: "lookup", TagFilters: []TagFilter{{Key: "host", Op: TagOpNotIn, Values: values}}}
	result, err := db.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	if q.TagFilters[0].excludedValues != nil || q.TagFilters[0].compiledRe != nil {
		t.Fatal("Execute mutated caller-owned filter caches")
	}
	if len(result.Points) != 2 {
		t.Fatalf("raw query returned %d points, want allowed + missing", len(result.Points))
	}

	q.Aggregation = &Aggregation{Function: AggSum, Window: time.Hour}
	result, err = db.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	if q.TagFilters[0].excludedValues != nil || q.TagFilters[0].compiledRe != nil {
		t.Fatal("aggregate Execute mutated caller-owned filter caches")
	}
	if len(result.Points) != 1 || result.Points[0].Value != 2 {
		t.Fatalf("aggregate query returned %+v, want one bucket with sum 2", result.Points)
	}
}

var benchmarkNotInMatches int

func BenchmarkNotInLookup4096(b *testing.B) {
	values := testNotInValues(4096)
	q := &Query{Metric: "cpu", TagFilters: []TagFilter{{Key: "host", Op: TagOpNotIn, Values: values}}}
	prepared, err := prepareQueryTagFilters(q)
	if err != nil {
		b.Fatal(err)
	}
	rows := []map[string]string{{"host": values[0]}, {"host": values[len(values)-1]}, {"host": "allowed"}, nil}
	b.ReportAllocs()
	b.ResetTimer()
	matched := 0
	for i := 0; i < b.N; i++ {
		if matchesTagFilters(rows[i%len(rows)], prepared.TagFilters) {
			matched++
		}
	}
	benchmarkNotInMatches = matched
}

func BenchmarkNotInPrepareAndMatch4096(b *testing.B) {
	values := testNotInValues(4096)
	rows := []map[string]string{{"host": values[0]}, {"host": values[len(values)-1]}, {"host": "allowed"}, nil}
	b.ReportAllocs()
	b.ResetTimer()
	matched := 0
	for i := 0; i < b.N; i++ {
		q := &Query{Metric: "cpu", TagFilters: []TagFilter{{Key: "host", Op: TagOpNotIn, Values: values}}}
		prepared, err := prepareQueryTagFilters(q)
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 1000; j++ {
			if matchesTagFilters(rows[j%len(rows)], prepared.TagFilters) {
				matched++
			}
		}
	}
	benchmarkNotInMatches = matched
}

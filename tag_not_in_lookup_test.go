package chronicle

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func notInLookupValues(n int) []string {
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

// This file deliberately runs unchanged against the pre-lookup implementation.
// The benchmark comparison therefore uses identical input and assertions.
func TestTagNotInLookupSemantics(t *testing.T) {
	matchers := []struct {
		name  string
		match func(map[string]string, []TagFilter) bool
	}{
		{"series", matchSeriesTagFilters},
		{"partition", matchesTagFilters},
	}
	for _, n := range []int{0, 1, 15, 16, 64, 4096} {
		values := notInLookupValues(n)
		if n > 16 {
			values[n/2] = values[1] // A duplicate does not change membership.
		}
		tags := []map[string]string{nil, {}, {"env": "prod"}, {"host": ""}, {"host": "東京,west"}, {"host": "allowed"}}
		if n > 0 {
			tags = append(tags, map[string]string{"host": values[n-1]})
		}
		for _, matcher := range matchers {
			t.Run(fmt.Sprintf("%s/%d", matcher.name, n), func(t *testing.T) {
				filters := []TagFilter{{Key: "host", Op: TagOpNotIn, Values: values}}
				for _, prepared := range []bool{false, true} {
					if prepared {
						if err := prepareTagFilters(filters); err != nil {
							t.Fatal(err)
						}
					}
					for _, row := range tags {
						value, present := row["host"]
						want := true
						for _, excluded := range values {
							if present && value == excluded {
								want = false
							}
						}
						if got := matcher.match(row, filters); got != want {
							t.Fatalf("prepared=%v tags=%v got=%v want=%v", prepared, row, got, want)
						}
					}
				}
			})
		}
	}
}

func TestTagNotInLookupReprepare(t *testing.T) {
	filters := []TagFilter{{Key: "host", Op: TagOpNotIn, Values: notInLookupValues(64)}}
	check := func(value string, want bool) {
		t.Helper()
		for _, match := range []func(map[string]string, []TagFilter) bool{matchSeriesTagFilters, matchesTagFilters} {
			if got := match(map[string]string{"host": value}, filters); got != want {
				t.Fatalf("value=%q got=%v want=%v", value, got, want)
			}
		}
	}
	prepare := func() {
		t.Helper()
		if err := prepareTagFilters(filters); err != nil {
			t.Fatal(err)
		}
	}
	prepare()
	check("excluded-0063", false)
	filters[0].Values[63] = "replacement"
	prepare()
	check("excluded-0063", true)
	check("replacement", false)
	filters[0].Values = []string{"small"}
	prepare()
	check("replacement", true)
	check("small", false)
	filters[0].Values = nil
	prepare()
	check("small", true)
	filters[0].Op = TagOpIn
	filters[0].Values = []string{"included"}
	prepare()
	check("included", true)
	check("small", false)
}

func TestTagNotInLookupExecute(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()
	ts := time.Now().Add(-time.Hour).Truncate(time.Hour).UnixNano()
	for i := 0; i < 130; i++ {
		tags := map[string]string{"host": fmt.Sprintf("host-%03d", i)}
		if i == 128 {
			tags = nil
		} else if i == 129 {
			tags["host"] = ""
		}
		if err := db.Write(Point{Metric: "lookup", Tags: tags, Value: 1, Timestamp: ts}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	values := []string{""}
	for i := 0; i < 64; i++ {
		values = append(values, fmt.Sprintf("host-%03d", i))
	}
	q := &Query{Metric: "lookup", TagFilters: []TagFilter{{Key: "host", Op: TagOpNotIn, Values: values}}}
	originalFilter := q.TagFilters[0]
	result, err := db.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(q.TagFilters[0], originalFilter) {
		t.Fatal("Execute changed the caller-owned tag filter")
	}
	if len(result.Points) != 65 {
		t.Fatalf("raw query: got %d points, want 64 included hosts plus one missing host", len(result.Points))
	}
	for _, point := range result.Points {
		if host, present := point.Tags["host"]; present && (host == "" || host < "host-064") {
			t.Fatalf("excluded host returned: %q", host)
		}
	}
	q.Aggregation = &Aggregation{Function: AggSum, Window: time.Hour}
	result, err = db.Execute(q)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Points) != 1 || result.Points[0].Value != 65 {
		t.Fatalf("aggregate query: got %+v, want one bucket with sum 65", result.Points)
	}
}

var notInLookupBenchmarkMatches int

func BenchmarkTagNotInLookup(b *testing.B) {
	for _, matcher := range []struct {
		name  string
		match func(map[string]string, []TagFilter) bool
	}{
		{"series", matchSeriesTagFilters},
		{"partition", matchesTagFilters},
	} {
		for _, n := range []int{1, 16, 256, 4096} {
			b.Run(fmt.Sprintf("%s/%d", matcher.name, n), func(b *testing.B) {
				values := notInLookupValues(n)
				filters := []TagFilter{{Key: "host", Op: TagOpNotIn, Values: values}}
				if err := prepareTagFilters(filters); err != nil {
					b.Fatal(err)
				}
				rows := []map[string]string{{"host": values[0]}, {"host": values[n-1]}, {"host": "allowed"}, nil}
				b.ReportAllocs()
				b.ResetTimer()
				matched := 0
				for i := 0; i < b.N; i++ {
					if matcher.match(rows[i%len(rows)], filters) {
						matched++
					}
				}
				notInLookupBenchmarkMatches = matched
			})
		}
	}
}

// Include building the lookup, rather than reporting only steady-state probes.
func BenchmarkTagNotInLookupBatch(b *testing.B) {
	for _, n := range []int{1, 16, 256, 4096} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			values := notInLookupValues(n)
			filters := []TagFilter{{Key: "host", Op: TagOpNotIn, Values: values}}
			rows := []map[string]string{{"host": values[0]}, {"host": values[n-1]}, {"host": "allowed"}, nil}
			b.ReportAllocs()
			b.ResetTimer()
			matched := 0
			for i := 0; i < b.N; i++ {
				if err := prepareTagFilters(filters); err != nil {
					b.Fatal(err)
				}
				for j := 0; j < 1000; j++ {
					if matchesTagFilters(rows[j%len(rows)], filters) {
						matched++
					}
				}
			}
			notInLookupBenchmarkMatches = matched
		})
	}
}

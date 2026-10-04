package chronicle

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestPredicatePushDown_ExactTagFilterSelection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		filters []TagFilter
		selects []int
		wants   []int
	}{
		{
			name: "selected exclusion with identical inclusion values",
			filters: []TagFilter{
				{Key: "host", Op: TagOpIn, Values: []string{"web-1"}},
				{Key: "host", Op: TagOpNotIn, Values: []string{"web-1"}},
			},
			selects: []int{1}, wants: []int{1},
		},
		{
			name: "reordered opposite operators",
			filters: []TagFilter{
				{Key: "host", Op: TagOpIn, Values: []string{"web-1"}},
				{Key: "host", Op: TagOpNotIn, Values: []string{"web-1"}},
			},
			selects: []int{1, 0}, wants: []int{1, 0},
		},
		{
			name: "selected comma-separated values are not a comma-bearing value",
			filters: []TagFilter{
				{Key: "host", Op: TagOpNotIn, Values: []string{"web-1,web-2"}},
				{Key: "host", Op: TagOpNotIn, Values: []string{"web-1", "web-2"}},
			},
			selects: []int{1}, wants: []int{1},
		},
		{
			name: "reordered value-boundary collision",
			filters: []TagFilter{
				{Key: "host", Op: TagOpIn, Values: []string{"web-1,web-2"}},
				{Key: "host", Op: TagOpIn, Values: []string{"web-1", "web-2"}},
			},
			selects: []int{1, 0}, wants: []int{1, 0},
		},
		{
			name: "empty exclusion set is not an empty-string exclusion",
			filters: []TagFilter{
				{Key: "host", Op: TagOpNotIn, Values: []string{""}},
				{Key: "host", Op: TagOpNotIn},
			},
			selects: []int{1}, wants: []int{1},
		},
		{
			name: "repeat does not consume a different operator",
			filters: []TagFilter{
				{Key: "host", Op: TagOpIn, Values: []string{"web-1"}},
				{Key: "host", Op: TagOpNotIn, Values: []string{"web-1"}},
			},
			selects: []int{1, 1}, wants: []int{1},
		},
		{
			name: "complete original order",
			filters: []TagFilter{
				{Key: "host", Op: TagOpIn, Values: []string{"web-1"}},
				{Key: "host", Op: TagOpNotIn, Values: []string{"web-1"}},
			},
			selects: []int{0, 1}, wants: []int{0, 1},
		},
		{
			name: "distinct values keep prior selection behavior",
			filters: []TagFilter{
				{Key: "host", Op: TagOpIn, Values: []string{"web-1", "db-1"}},
				{Key: "host", Op: TagOpNotIn, Values: []string{"web-1"}},
			},
			selects: []int{1}, wants: []int{1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, transport := range []string{"direct", "json"} {
				t.Run(transport, func(t *testing.T) {
					pp := NewPredicatePushDown(true)
					q := &Query{Metric: "cpu", TagFilters: tc.filters}
					analysis := pp.AnalyzePredicates(q)
					var selected []PushablePredicate
					for _, i := range tc.selects {
						selected = append(selected, analysis.Pushable[i])
					}
					if transport == "json" {
						data, err := json.Marshal(selected)
						if err != nil {
							t.Fatal(err)
						}
						selected = nil
						if err := json.Unmarshal(data, &selected); err != nil {
							t.Fatal(err)
						}
					}
					var want []TagFilter
					for _, i := range tc.wants {
						want = append(want, q.TagFilters[i])
					}
					got := pp.CreateRemoteQuery(q, selected).TagFilters
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("selected filters changed: got %#v, want %#v", got, want)
					}
				})
			}
		})
	}
}

func TestPredicatePushDown_TagFilterDescriptorGuards(t *testing.T) {
	pp := NewPredicatePushDown(true)
	q := &Query{Metric: "cpu", TagFilters: []TagFilter{
		{Key: "host", Op: TagOpIn, Values: []string{"web-1"}},
		{Key: "host", Op: TagOpRegex, Values: []string{"web-1"}},
	}}
	for _, tc := range []struct {
		name string
		json string
		want int
	}{
		{"legacy predicate", `[{"kind":2,"field":"host","value":"web-1","safe":true}]`, 1},
		{"unsafe predicate", `[{"kind":2,"field":"host","value":"web-1","safe":false}]`, 0},
		{"wrong operator", `[{"kind":2,"field":"host","value":"web-1","safe":true,"tag_filter":{"Key":"host","Op":5,"Values":["web-1"]}}]`, 0},
		{"wrong key", `[{"kind":2,"field":"host","value":"web-1","safe":true,"tag_filter":{"Key":"region","Op":2,"Values":["web-1"]}}]`, 0},
		{"wrong values", `[{"kind":2,"field":"host","value":"web-1","safe":true,"tag_filter":{"Key":"host","Op":2,"Values":["web-2"]}}]`, 0},
		{"local regex", `[{"kind":2,"field":"host","value":"web-1","safe":true,"tag_filter":{"Key":"host","Op":3,"Values":["web-1"]}}]`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var selected []PushablePredicate
			if err := json.Unmarshal([]byte(tc.json), &selected); err != nil {
				t.Fatal(err)
			}
			got := pp.CreateRemoteQuery(q, selected).TagFilters
			if len(got) != tc.want {
				t.Fatalf("got %d filters, want %d: %#v", len(got), tc.want, got)
			}
			if tc.want == 1 && !reflect.DeepEqual(got[0], q.TagFilters[0]) {
				t.Fatalf("legacy predicate changed: %#v", got)
			}
		})
	}
}

func TestPredicatePushDown_TagFilterDescriptorSnapshot(t *testing.T) {
	pp := NewPredicatePushDown(true)
	q := &Query{Metric: "cpu", TagFilters: []TagFilter{
		{Key: "host", Op: TagOpNotIn, Values: []string{"a,b", "c"}},
	}}
	analysis := pp.AnalyzePredicates(q)
	// Preserve the old comma-joined display value while changing boundaries.
	// An analysis descriptor must not alias the source query's Values slice.
	q.TagFilters[0].Values[0] = "a"
	q.TagFilters[0].Values[1] = "b,c"
	if got := pp.CreateRemoteQuery(q, analysis.Pushable).TagFilters; len(got) != 0 {
		t.Fatalf("stale analysis selected a changed filter: %#v", got)
	}
}

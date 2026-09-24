package chronicle

import (
	"testing"
)

func TestSeriesKeyFromMetricTags(t *testing.T) {
	tests := []struct {
		name   string
		metric string
		tags   map[string]string
		want   string
	}{
		{
			name:   "no tags",
			metric: "cpu",
			tags:   nil,
			want:   "cpu",
		},
		{
			name:   "empty tags",
			metric: "mem",
			tags:   map[string]string{},
			want:   "mem",
		},
		{
			name:   "single tag",
			metric: "disk",
			tags:   map[string]string{"host": "server1"},
			want:   "disk|host=server1",
		},
		{
			name:   "multiple tags sorted",
			metric: "network",
			tags:   map[string]string{"host": "server1", "device": "eth0"},
			want:   "network|device=eth0,host=server1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := seriesKeyFromMetricTags(tt.metric, tt.tags)
			if got != tt.want {
				t.Errorf("seriesKeyFromMetricTags() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMatchSeriesTags(t *testing.T) {
	tests := []struct {
		name       string
		seriesTags map[string]string
		filter     map[string]string
		want       bool
	}{
		{
			name:       "empty filter matches all",
			seriesTags: map[string]string{"host": "a"},
			filter:     nil,
			want:       true,
		},
		{
			name:       "exact match",
			seriesTags: map[string]string{"host": "a", "env": "prod"},
			filter:     map[string]string{"host": "a"},
			want:       true,
		},
		{
			name:       "mismatch",
			seriesTags: map[string]string{"host": "a"},
			filter:     map[string]string{"host": "b"},
			want:       false,
		},
		{
			name:       "missing tag",
			seriesTags: map[string]string{"host": "a"},
			filter:     map[string]string{"env": "prod"},
			want:       false,
		},
		{
			name:       "multiple filters all match",
			seriesTags: map[string]string{"host": "a", "env": "prod"},
			filter:     map[string]string{"host": "a", "env": "prod"},
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchSeriesTags(tt.seriesTags, tt.filter)
			if got != tt.want {
				t.Errorf("matchSeriesTags() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchSeriesTagFilters(t *testing.T) {
	tests := []struct {
		name       string
		seriesTags map[string]string
		filters    []TagFilter
		want       bool
	}{
		{
			name:       "empty filters",
			seriesTags: map[string]string{"host": "a"},
			filters:    nil,
			want:       true,
		},
		{
			name:       "equal match",
			seriesTags: map[string]string{"host": "a"},
			filters:    []TagFilter{{Key: "host", Op: TagOpEq, Values: []string{"a"}}},
			want:       true,
		},
		{
			name:       "equal no match",
			seriesTags: map[string]string{"host": "a"},
			filters:    []TagFilter{{Key: "host", Op: TagOpEq, Values: []string{"b"}}},
			want:       false,
		},
		{
			name:       "equal missing tag",
			seriesTags: map[string]string{"host": "a"},
			filters:    []TagFilter{{Key: "env", Op: TagOpEq, Values: []string{"prod"}}},
			want:       false,
		},
		{
			name:       "equal empty values",
			seriesTags: map[string]string{"host": "a"},
			filters:    []TagFilter{{Key: "host", Op: TagOpEq, Values: []string{}}},
			want:       false,
		},
		{
			name:       "not equal match",
			seriesTags: map[string]string{"host": "a"},
			filters:    []TagFilter{{Key: "host", Op: TagOpNotEq, Values: []string{"b"}}},
			want:       true,
		},
		{
			name:       "not equal no match",
			seriesTags: map[string]string{"host": "a"},
			filters:    []TagFilter{{Key: "host", Op: TagOpNotEq, Values: []string{"a"}}},
			want:       false,
		},
		{
			name:       "not equal missing tag",
			seriesTags: map[string]string{"host": "a"},
			filters:    []TagFilter{{Key: "env", Op: TagOpNotEq, Values: []string{"prod"}}},
			want:       true,
		},
		{
			name:       "in operator match",
			seriesTags: map[string]string{"host": "a"},
			filters:    []TagFilter{{Key: "host", Op: TagOpIn, Values: []string{"a", "b", "c"}}},
			want:       true,
		},
		{
			name:       "in operator no match",
			seriesTags: map[string]string{"host": "d"},
			filters:    []TagFilter{{Key: "host", Op: TagOpIn, Values: []string{"a", "b", "c"}}},
			want:       false,
		},
		{
			name:       "not in operator match (value absent from list)",
			seriesTags: map[string]string{"host": "d"},
			filters:    []TagFilter{{Key: "host", Op: TagOpNotIn, Values: []string{"a", "b", "c"}}},
			want:       true,
		},
		{
			name:       "not in operator match (missing tag)",
			seriesTags: map[string]string{"env": "prod"},
			filters:    []TagFilter{{Key: "host", Op: TagOpNotIn, Values: []string{"a", "b", "c"}}},
			want:       true,
		},
		{
			name:       "not in operator no match (value present)",
			seriesTags: map[string]string{"host": "a"},
			filters:    []TagFilter{{Key: "host", Op: TagOpNotIn, Values: []string{"a", "b", "c"}}},
			want:       false,
		},
		{
			name:       "multiple filters all pass",
			seriesTags: map[string]string{"host": "a", "env": "prod"},
			filters: []TagFilter{
				{Key: "host", Op: TagOpEq, Values: []string{"a"}},
				{Key: "env", Op: TagOpIn, Values: []string{"prod", "staging"}},
			},
			want: true,
		},
		{
			name:       "multiple filters one fails",
			seriesTags: map[string]string{"host": "a", "env": "dev"},
			filters: []TagFilter{
				{Key: "host", Op: TagOpEq, Values: []string{"a"}},
				{Key: "env", Op: TagOpIn, Values: []string{"prod", "staging"}},
			},
			want: false,
		},
		{
			name:       "unknown operator",
			seriesTags: map[string]string{"host": "a"},
			filters:    []TagFilter{{Key: "host", Op: TagOp(99), Values: []string{"a"}}},
			want:       false,
		},
		{
			name:       "regex match",
			seriesTags: map[string]string{"method": "GET"},
			filters:    []TagFilter{{Key: "method", Op: TagOpRegex, Values: []string{"GET|POST"}}},
			want:       true,
		},
		{
			name:       "regex no match",
			seriesTags: map[string]string{"method": "DELETE"},
			filters:    []TagFilter{{Key: "method", Op: TagOpRegex, Values: []string{"GET|POST"}}},
			want:       false,
		},
		{
			name:       "not regex match (excluded)",
			seriesTags: map[string]string{"status": "500"},
			filters:    []TagFilter{{Key: "status", Op: TagOpNotRegex, Values: []string{"5.."}}},
			want:       false,
		},
		{
			name:       "not regex no match (included)",
			seriesTags: map[string]string{"status": "200"},
			filters:    []TagFilter{{Key: "status", Op: TagOpNotRegex, Values: []string{"5.."}}},
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchSeriesTagFilters(tt.seriesTags, tt.filters)
			if got != tt.want {
				t.Errorf("matchSeriesTagFilters() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCopyTags(t *testing.T) {
	// Test nil
	if got := copyTags(nil); got != nil {
		t.Errorf("copyTags(nil) = %v, want nil", got)
	}

	// Test empty
	if got := copyTags(map[string]string{}); got != nil {
		t.Errorf("copyTags({}) = %v, want nil", got)
	}

	// Test copy
	original := map[string]string{"a": "1", "b": "2"}
	copied := copyTags(original)
	if len(copied) != 2 || copied["a"] != "1" || copied["b"] != "2" {
		t.Errorf("copyTags() = %v, want %v", copied, original)
	}

	// Verify it's a true copy
	copied["a"] = "modified"
	if original["a"] == "modified" {
		t.Error("copyTags() did not create independent copy")
	}
}

func TestFormatTagsString(t *testing.T) {
	tests := []struct {
		name string
		tags map[string]string
		want string
	}{
		{"empty", nil, ""},
		{"empty map", map[string]string{}, ""},
		{"single", map[string]string{"a": "1"}, "a=1"},
		{"multiple sorted", map[string]string{"b": "2", "a": "1"}, "a=1,b=2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatTagsString(tt.tags)
			if got != tt.want {
				t.Errorf("formatTagsString() = %q, want %q", got, tt.want)
			}
		})
	}
}

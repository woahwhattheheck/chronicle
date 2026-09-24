package chronicle

import (
	"testing"
	"time"
)

func TestAggBuckets_New(t *testing.T) {
	buckets := newAggBuckets(1024 * 1024)
	if buckets == nil {
		t.Fatal("newAggBuckets returned nil")
	}
}

func TestAggBuckets_AddAndFinalize(t *testing.T) {
	tests := []struct {
		name     string
		values   []float64
		aggFunc  AggFunc
		expected float64
	}{
		{
			name:     "sum",
			values:   []float64{1, 2, 3, 4, 5},
			aggFunc:  AggSum,
			expected: 15,
		},
		{
			name:     "count",
			values:   []float64{1, 2, 3, 4, 5},
			aggFunc:  AggCount,
			expected: 5,
		},
		{
			name:     "mean",
			values:   []float64{1, 2, 3, 4, 5},
			aggFunc:  AggMean,
			expected: 3,
		},
		{
			name:     "min",
			values:   []float64{5, 3, 1, 4, 2},
			aggFunc:  AggMin,
			expected: 1,
		},
		{
			name:     "max",
			values:   []float64{5, 3, 1, 4, 2},
			aggFunc:  AggMax,
			expected: 5,
		},
		{
			name:     "first",
			values:   []float64{10, 20, 30},
			aggFunc:  AggFirst,
			expected: 10,
		},
		{
			name:     "last",
			values:   []float64{10, 20, 30},
			aggFunc:  AggLast,
			expected: 30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buckets := newAggBuckets(1024 * 1024)
			window := time.Minute
			baseTime := time.Now().Truncate(window).UnixNano()

			tags := map[string]string{"host": "server1"}
			for i, v := range tt.values {
				ts := baseTime + int64(i*1000) // 1µs apart
				err := buckets.add(tags, ts, v, window, nil, tt.aggFunc)
				if err != nil {
					t.Fatalf("add failed: %v", err)
				}
			}

			points := buckets.finalize(tt.aggFunc, window, 0)
			if len(points) != 1 {
				t.Fatalf("expected 1 point, got %d", len(points))
			}

			if points[0].Value != tt.expected {
				t.Errorf("value = %v, want %v", points[0].Value, tt.expected)
			}
		})
	}
}

func TestAggBuckets_Stddev(t *testing.T) {
	buckets := newAggBuckets(1024 * 1024)
	window := time.Minute
	baseTime := time.Now().Truncate(window).UnixNano()

	// stddev of [2, 4, 4, 4, 5, 5, 7, 9]
	// sample stddev ≈ 2.138
	values := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	tags := map[string]string{"host": "server1"}

	for i, v := range values {
		ts := baseTime + int64(i*1000)
		err := buckets.add(tags, ts, v, window, nil, AggStddev)
		if err != nil {
			t.Fatalf("add failed: %v", err)
		}
	}

	points := buckets.finalize(AggStddev, window, 0)
	if len(points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(points))
	}

	// Sample stddev (implementation uses Welford's algorithm)
	expected := 2.138
	tolerance := 0.1
	if points[0].Value < expected-tolerance || points[0].Value > expected+tolerance {
		t.Errorf("stddev = %v, want ~%v", points[0].Value, expected)
	}
}

func TestAggBuckets_MultipleWindows(t *testing.T) {
	buckets := newAggBuckets(1024 * 1024)
	window := time.Minute

	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano()
	tags := map[string]string{"host": "server1"}

	// Add points spanning 3 minutes
	for i := 0; i < 30; i++ {
		ts := baseTime + int64(i)*10*int64(time.Second)
		err := buckets.add(tags, ts, float64(i), window, nil, AggCount)
		if err != nil {
			t.Fatalf("add failed: %v", err)
		}
	}

	points := buckets.finalize(AggCount, window, 0)

	// Should have multiple buckets
	if len(points) < 2 {
		t.Errorf("expected multiple windows, got %d points", len(points))
	}
}

func TestAggBuckets_GroupBy(t *testing.T) {
	buckets := newAggBuckets(1024 * 1024)
	window := time.Minute
	baseTime := time.Now().Truncate(window).UnixNano()

	// Add points for two different hosts
	for i := 0; i < 5; i++ {
		ts := baseTime + int64(i*1000)
		buckets.add(map[string]string{"host": "server1"}, ts, 1.0, window, []string{"host"}, AggSum)
		buckets.add(map[string]string{"host": "server2"}, ts, 2.0, window, []string{"host"}, AggSum)
	}

	points := buckets.finalize(AggSum, window, 0)

	if len(points) != 2 {
		t.Fatalf("expected 2 points (one per host), got %d", len(points))
	}

	// Verify each host has correct sum
	for _, p := range points {
		host := p.Tags["host"]
		switch host {
		case "server1":
			if p.Value != 5.0 {
				t.Errorf("server1 sum = %v, want 5.0", p.Value)
			}
		case "server2":
			if p.Value != 10.0 {
				t.Errorf("server2 sum = %v, want 10.0", p.Value)
			}
		}
	}
}

func TestMakeGroupKey(t *testing.T) {
	tests := []struct {
		name    string
		tags    map[string]string
		groupBy []string
		want    string
	}{
		{
			name:    "empty groupBy",
			tags:    map[string]string{"host": "server1", "region": "us-east"},
			groupBy: nil,
			want:    "",
		},
		{
			name:    "single groupBy",
			tags:    map[string]string{"host": "server1", "region": "us-east"},
			groupBy: []string{"host"},
			want:    "host=server1",
		},
		{
			name:    "multiple groupBy",
			tags:    map[string]string{"host": "server1", "region": "us-east"},
			groupBy: []string{"host", "region"},
			want:    "host=server1|region=us-east",
		},
		{
			name:    "missing tag skipped",
			tags:    map[string]string{"host": "server1"},
			groupBy: []string{"host", "region"},
			want:    "host=server1", // missing tags are skipped
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := makeGroupKey(tt.tags, tt.groupBy)
			if got != tt.want {
				t.Errorf("makeGroupKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseGroupKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want map[string]string
	}{
		{
			name: "empty key",
			key:  "",
			want: nil,
		},
		{
			name: "single pair",
			key:  "host=server1",
			want: map[string]string{"host": "server1"},
		},
		{
			name: "multiple pairs",
			key:  "host=server1|region=us-east",
			want: map[string]string{"host": "server1", "region": "us-east"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseGroupKey(tt.key)
			if tt.want == nil {
				if got != nil {
					t.Errorf("parseGroupKey() = %v, want nil", got)
				}
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("parseGroupKey() returned %d pairs, want %d", len(got), len(tt.want))
				return
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("parseGroupKey()[%q] = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func TestAggBuckets_MemoryBudget(t *testing.T) {
	// Very small budget
	buckets := newAggBuckets(100)
	window := time.Minute
	baseTime := time.Now().Truncate(window).UnixNano()

	tags := map[string]string{"host": "server1"}

	// Percentile stores all values, so it will exceed budget
	var err error
	for i := 0; i < 1000; i++ {
		ts := baseTime + int64(i*1000)
		err = buckets.add(tags, ts, float64(i), window, nil, AggPercentile)
		if err != nil {
			break
		}
	}

	if err == nil {
		t.Error("expected memory budget error, got nil")
	}
}

func TestAggBuckets_EmptyInput(t *testing.T) {
	buckets := newAggBuckets(1024 * 1024)
	points := buckets.finalize(AggSum, time.Minute, 0)

	if len(points) != 0 {
		t.Errorf("expected 0 points for empty input, got %d", len(points))
	}
}

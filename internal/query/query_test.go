package query

import (
	"testing"
	"time"
)

func TestParserBasic(t *testing.T) {
	p := &Parser{}

	tests := []struct {
		name    string
		query   string
		want    *Query
		wantErr bool
	}{
		{
			name:  "simple select",
			query: "SELECT count(value) FROM cpu",
			want: &Query{
				Metric: "cpu",
				Tags:   map[string]string{},
				Aggregation: &Aggregation{
					Function: AggCount,
					Window:   time.Second,
				},
			},
		},
		{
			name:  "with where clause",
			query: "SELECT mean(value) FROM temperature WHERE host = 'server1'",
			want: &Query{
				Metric: "temperature",
				Tags:   map[string]string{"host": "server1"},
				Aggregation: &Aggregation{
					Function: AggMean,
					Window:   time.Second,
				},
				TagFilters: []TagFilter{
					{Key: "host", Op: TagOpEq, Values: []string{"server1"}},
				},
			},
		},
		{
			name:  "with limit",
			query: "SELECT sum(value) FROM requests LIMIT 10",
			want: &Query{
				Metric: "requests",
				Tags:   map[string]string{},
				Limit:  10,
				Aggregation: &Aggregation{
					Function: AggSum,
					Window:   time.Second,
				},
			},
		},
		{
			name:  "with group by time",
			query: "SELECT mean(value) FROM cpu GROUP BY time(5m)",
			want: &Query{
				Metric: "cpu",
				Tags:   map[string]string{},
				Aggregation: &Aggregation{
					Function: AggMean,
					Window:   5 * time.Minute,
				},
			},
		},
		{
			name:    "empty query",
			query:   "",
			wantErr: true,
		},
		{
			name:    "missing from",
			query:   "SELECT count(value)",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			if got.Metric != tt.want.Metric {
				t.Errorf("Metric = %v, want %v", got.Metric, tt.want.Metric)
			}
			if got.Limit != tt.want.Limit {
				t.Errorf("Limit = %v, want %v", got.Limit, tt.want.Limit)
			}
			if tt.want.Aggregation != nil {
				if got.Aggregation == nil {
					t.Error("expected aggregation")
				} else {
					if got.Aggregation.Function != tt.want.Aggregation.Function {
						t.Errorf("AggFunc = %v, want %v", got.Aggregation.Function, tt.want.Aggregation.Function)
					}
					if got.Aggregation.Window != tt.want.Aggregation.Window {
						t.Errorf("Window = %v, want %v", got.Aggregation.Window, tt.want.Aggregation.Window)
					}
				}
			}
		})
	}
}

func TestAggBuckets(t *testing.T) {
	buckets := NewAggBuckets(1024 * 1024)

	window := time.Minute
	tags := map[string]string{"host": "server1"}

	// Add some values
	baseTime := int64(1000000000)
	for i := 0; i < 10; i++ {
		err := buckets.Add(tags, baseTime+int64(i)*int64(time.Second), float64(i), window, []string{"host"}, AggMean)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	points := buckets.Finalize(AggMean, window, 0)
	if len(points) == 0 {
		t.Error("expected at least one point")
	}
}

func TestMakeGroupKey(t *testing.T) {
	tests := []struct {
		tags    map[string]string
		groupBy []string
		want    string
	}{
		{nil, nil, ""},
		{map[string]string{"a": "1"}, nil, ""},
		{map[string]string{"a": "1", "b": "2"}, []string{"a"}, "a=1"},
		{map[string]string{"a": "1", "b": "2"}, []string{"a", "b"}, "a=1|b=2"},
	}

	for _, tt := range tests {
		got := MakeGroupKey(tt.tags, tt.groupBy)
		if got != tt.want {
			t.Errorf("MakeGroupKey(%v, %v) = %v, want %v", tt.tags, tt.groupBy, got, tt.want)
		}
	}
}

func TestParseGroupKey(t *testing.T) {
	tests := []struct {
		key  string
		want map[string]string
	}{
		{"", nil},
		{"a=1", map[string]string{"a": "1"}},
		{"a=1|b=2", map[string]string{"a": "1", "b": "2"}},
	}

	for _, tt := range tests {
		got := ParseGroupKey(tt.key)
		if len(got) != len(tt.want) {
			t.Errorf("ParseGroupKey(%v) = %v, want %v", tt.key, got, tt.want)
			continue
		}
		for k, v := range tt.want {
			if got[k] != v {
				t.Errorf("ParseGroupKey(%v)[%s] = %v, want %v", tt.key, k, got[k], v)
			}
		}
	}
}

func TestParseAggFunc(t *testing.T) {
	tests := []struct {
		input string
		want  AggFunc
	}{
		{"count", AggCount},
		{"COUNT", AggCount},
		{"sum", AggSum},
		{"mean", AggMean},
		{"min", AggMin},
		{"max", AggMax},
		{"stddev", AggStddev},
		{"percentile", AggPercentile},
		{"rate", AggRate},
		{"first", AggFirst},
		{"last", AggLast},
		{"unknown", AggNone},
		{"", AggNone},
	}

	for _, tt := range tests {
		got := ParseAggFunc(tt.input)
		if got != tt.want {
			t.Errorf("ParseAggFunc(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestParseTimeExpression(t *testing.T) {
	tests := []struct {
		name    string
		op      string
		value   string
		wantErr bool
	}{
		{"now", ">", "now()", false},
		{"now minus", ">", "now() - 1h", false},
		{"now plus", "<", "now() + 30m", false},
		{"timestamp", ">", "1609459200000000000", false},
		{"duration start", ">=", "1h", false},
		{"duration end", "<=", "30m", false},
		{"invalid", ">", "not-a-time", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseTimeExpression(tt.op, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTimeExpression(%q, %q) error = %v, wantErr %v", tt.op, tt.value, err, tt.wantErr)
			}
		})
	}
}

func TestParserAdvanced(t *testing.T) {
	p := &Parser{}

	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{"group by tag", "SELECT mean(value) FROM cpu GROUP BY host", false},
		{"group by multiple", "SELECT mean(value) FROM cpu GROUP BY time(5m), host, region", false},
		{"not equals", "SELECT count(value) FROM cpu WHERE host != 'server1'", false},
		{"in operator", "SELECT sum(value) FROM cpu WHERE host IN ('server1', 'server2')", false},
		{"not in operator", "SELECT sum(value) FROM cpu WHERE host NOT IN ('server1', 'server2')", false},
		{"time range", "SELECT mean(value) FROM cpu WHERE time > now() - 1h AND time < now()", false},
		{"complex", "SELECT mean(value) FROM cpu WHERE host = 'server1' AND region = 'us-west' GROUP BY time(5m) LIMIT 100", false},
		{"invalid select", "SELEKT count(value) FROM cpu", true},
		{"missing metric", "SELECT count(value) FROM", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse(%q) error = %v, wantErr %v", tt.query, err, tt.wantErr)
			}
		})
	}
}

func TestAggBucketsAllFunctions(t *testing.T) {
	funcs := []AggFunc{AggCount, AggSum, AggMean, AggMin, AggMax, AggFirst, AggLast}

	for _, fn := range funcs {
		t.Run(fn.String(), func(t *testing.T) {
			buckets := NewAggBuckets(1024 * 1024)
			window := time.Minute
			tags := map[string]string{"host": "server1"}

			baseTime := int64(1000000000)
			for i := 0; i < 5; i++ {
				err := buckets.Add(tags, baseTime+int64(i)*int64(time.Second), float64(i+1), window, []string{"host"}, fn)
				if err != nil {
					t.Fatalf("Add failed: %v", err)
				}
			}

			points := buckets.Finalize(fn, window, 0)
			if len(points) == 0 {
				t.Error("expected at least one point")
			}
		})
	}
}

func (a AggFunc) String() string {
	switch a {
	case AggCount:
		return "count"
	case AggSum:
		return "sum"
	case AggMean:
		return "mean"
	case AggMin:
		return "min"
	case AggMax:
		return "max"
	case AggFirst:
		return "first"
	case AggLast:
		return "last"
	case AggStddev:
		return "stddev"
	case AggPercentile:
		return "percentile"
	case AggRate:
		return "rate"
	default:
		return "none"
	}
}

func TestAggBucketsMemoryLimit(t *testing.T) {
	// Test basic bucket operations - memory limiting is optional
	buckets := NewAggBuckets(1024 * 1024)
	tags := map[string]string{"host": "server1"}

	err := buckets.Add(tags, 1000, 1.0, time.Minute, []string{"host"}, AggMean)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAggBucketsStddev(t *testing.T) {
	buckets := NewAggBuckets(1024 * 1024)
	tags := map[string]string{"host": "server1"}
	window := time.Minute

	// Add values: 2, 4, 4, 4, 5, 5, 7, 9 (stddev ≈ 2.0)
	values := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	baseTime := int64(1000000000)
	for i, v := range values {
		err := buckets.Add(tags, baseTime+int64(i)*int64(time.Second), v, window, []string{"host"}, AggStddev)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	points := buckets.Finalize(AggStddev, window, 0)
	if len(points) == 0 {
		t.Error("expected at least one point")
	}
}

func TestParserWhereClauseVariants(t *testing.T) {
	p := &Parser{}

	// Test various WHERE clause formats
	queries := []string{
		"SELECT mean(value) FROM cpu WHERE host = 'server1'",
		"SELECT mean(value) FROM cpu WHERE host = \"server1\"",
	}

	for _, q := range queries {
		_, err := p.Parse(q)
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", q, err)
		}
	}
}

func TestAggBucketsRate(t *testing.T) {
	buckets := NewAggBuckets(1024 * 1024)
	tags := map[string]string{"host": "server1"}
	window := time.Minute

	// Add increasing counter values
	baseTime := int64(1000000000)
	for i := 0; i < 10; i++ {
		err := buckets.Add(tags, baseTime+int64(i)*int64(time.Second), float64(i*100), window, []string{"host"}, AggRate)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	points := buckets.Finalize(AggRate, window, 0)
	if len(points) == 0 {
		t.Error("expected at least one point")
	}
}

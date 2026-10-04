package chronicle

import (
	"math"
	"testing"
)

func TestNotInSelectivityCountsDistinctValues(t *testing.T) {
	filter := func(values ...string) []TagFilter {
		return []TagFilter{{Key: "host", Op: TagOpNotIn, Values: values}}
	}

	t.Run("cardinality estimator", func(t *testing.T) {
		ce := NewCardinalityEstimator(nil, DefaultCardinalityEstimatorConfig())
		ce.stats["cpu"] = &MetricCardinalityStats{
			Metric:           "cpu",
			RowCount:         100,
			TagCardinalities: map[string]uint64{"host": 10},
		}

		single := ce.EstimateCardinality(&Query{Metric: "cpu", TagFilters: filter("a")})
		repeated := ce.EstimateCardinality(&Query{Metric: "cpu", TagFilters: filter("a", "a")})
		if repeated != single {
			t.Fatalf("repeated NOT IN value changed estimate: single=%d repeated=%d", single, repeated)
		}
		if repeated != 90 {
			t.Fatalf("expected one distinct exclusion from 10 values to estimate 90 rows, got %d", repeated)
		}
	})

	t.Run("cost optimizer", func(t *testing.T) {
		se := &SelectivityEstimator{config: DefaultCostBasedOptimizerConfig()}
		stats := &TableStatistics{
			Metric:           "cpu",
			RowCount:         100,
			DistinctTagCount: map[string]int64{"host": 10},
		}

		single := se.CombinedSelectivity(stats, &Query{Metric: "cpu", TagFilters: filter("a")})
		repeated := se.CombinedSelectivity(stats, &Query{Metric: "cpu", TagFilters: filter("a", "a")})
		if math.Abs(repeated-single) > 1e-12 {
			t.Fatalf("repeated NOT IN value changed selectivity: single=%f repeated=%f", single, repeated)
		}
		if math.Abs(repeated-0.9) > 1e-12 {
			t.Fatalf("expected one distinct exclusion from 10 values to select 0.9, got %f", repeated)
		}
	})
}

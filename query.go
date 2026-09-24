package chronicle

// query.go implements the query data path.
//
// Query Pipeline:
//   Query → QueryMiddleware chain → Materialized View check
//   → Partition lookup (B-tree) → Series filtering (Index)
//   → Partition scan or Aggregation → Limit → Result
//
// All queries flow through Execute() or ExecuteContext(). When
// QueryMiddleware is configured, it wraps the execution chain.

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"time"
)

// Query describes a time-series data retrieval request. At minimum, set Metric
// to select a series. Use Start/End (Unix nanoseconds) to bound the time range,
// TagFilters to narrow by labels, and Aggregation to reduce results.
type Query struct {
	Metric      string
	Tags        map[string]string
	TagFilters  []TagFilter
	Start       int64 // inclusive lower bound, Unix nanoseconds; 0 means unbounded
	End         int64 // exclusive upper bound, Unix nanoseconds; 0 means unbounded
	Aggregation *Aggregation
	GroupBy     []string
	Limit       int
}

// Aggregation specifies a downsampling operation applied during query execution.
// Function selects the reducer (sum, mean, etc.) and Window sets the bucket width.
type Aggregation struct {
	Function   AggFunc
	Window     time.Duration
	Percentile float64 // percentile value (0-100) for AggPercentile; defaults to 95 if zero
}

// AggFunc selects the aggregation function applied to each time window.
type AggFunc int

const (
	AggNone       AggFunc = iota // No aggregation (raw points)
	AggCount                     // Number of points per window
	AggSum                       // Sum of values per window
	AggMean                      // Arithmetic mean per window
	AggMin                       // Minimum value per window
	AggMax                       // Maximum value per window
	AggStddev                    // Standard deviation per window
	AggPercentile                // Percentile per window
	AggRate                      // Rate of change per window
	AggFirst                     // First value per window
	AggLast                      // Last value per window
)

// TagOp selects the comparison operator used in a TagFilter.
type TagOp int

const (
	TagOpEq       TagOp = iota // Exact match (tag == value)
	TagOpNotEq                 // Exclusion (tag != value)
	TagOpIn                    // Set membership (tag in [values...])
	TagOpRegex                 // Regex match (tag =~ pattern)
	TagOpNotRegex              // Negated regex match (tag !~ pattern)
	TagOpNotIn                 // Negated set membership (tag not in [values...])
)

// TagFilter restricts query results to series whose tag Key satisfies
// the comparison Op against the given Values.
type TagFilter struct {
	Key    string
	Op     TagOp
	Values []string

	// compiledRe caches the compiled regex for TagOpRegex/TagOpNotRegex.
	// Set by prepareTagFilters before query execution.
	compiledRe *regexp.Regexp
}

const maxTagFilterRegexLen = 1024

// prepareTagFilters pre-compiles regex patterns in tag filters.
// Must be called before query execution to avoid per-series compilation.
func prepareTagFilters(filters []TagFilter) error {
	for i := range filters {
		if (filters[i].Op == TagOpRegex || filters[i].Op == TagOpNotRegex) && len(filters[i].Values) > 0 {
			pattern := filters[i].Values[0]
			if len(pattern) > maxTagFilterRegexLen {
				return fmt.Errorf("tag filter regex pattern too long (%d > %d)", len(pattern), maxTagFilterRegexLen)
			}
			re, err := regexp.Compile(pattern)
			if err != nil {
				return fmt.Errorf("invalid tag filter regex %q: %w", pattern, err)
			}
			filters[i].compiledRe = re
		}
	}
	return nil
}

// Execute runs a query and returns results.
// This is a convenience wrapper around ExecuteContext with a background context.
func (db *DB) Execute(q *Query) (*Result, error) {
	if db.isClosed() {
		return nil, ErrClosed
	}
	ctx := context.Background()
	if db.config.Query.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, db.config.Query.QueryTimeout)
		defer cancel()
	}

	// Route through query middleware pipeline if available
	if db.features != nil {
		if qm := db.features.QueryMiddleware(); qm != nil && qm.MiddlewareCount() > 0 {
			return qm.ExecuteWithContext(ctx, q)
		}
	}

	return db.ExecuteContext(ctx, q)
}

// ExecuteContext runs a query with context support for cancellation and timeout.
func (db *DB) ExecuteContext(ctx context.Context, q *Query) (*Result, error) {
	queryStart := time.Now()
	result, err := db.executeContextInternal(ctx, q)

	// Record query metrics for self-instrumentation
	if db.features != nil {
		if si := db.features.SelfInstrumentation(); si != nil {
			si.RecordQuery(time.Since(queryStart).Nanoseconds(), err != nil)
		}
	}

	return result, err
}

func (db *DB) executeContextInternal(ctx context.Context, q *Query) (*Result, error) {
	if q == nil {
		return &Result{Points: []Point{}}, nil
	}

	if q.Metric == "" {
		return nil, newQueryError(QueryErrorTypeInvalid, "metric name is required", q, nil)
	}

	// Pre-compile regex tag filters once before scanning partitions
	if len(q.TagFilters) > 0 {
		if err := prepareTagFilters(q.TagFilters); err != nil {
			return nil, newQueryError(QueryErrorTypeInvalid, err.Error(), q, err)
		}
	}

	// Check query cost budget (only if estimator already initialized)
	if db.features != nil {
		if ce := db.features.queryCost; ce != nil {
			if allowed, est, err := ce.ShouldExecute(q); err == nil && !allowed {
				return nil, newQueryError(QueryErrorTypeMemory,
					fmt.Sprintf("query rejected by cost estimator: estimated cost %.2f exceeds threshold (verdict: %s)",
						est.EstimatedCost, est.Verdict), q, nil)
			}
		}
	}

	q = db.tryMaterialized(q)

	db.mu.RLock()
	partitions := db.findPartitionsLocked(q.Start, q.End)
	allowed := db.index.FilterSeries(q.Metric, q.Tags)
	db.mu.RUnlock()

	slog.Debug("query plan",
		"metric", q.Metric,
		"partitions", len(partitions),
		"series", len(allowed),
		"has_aggregation", q.Aggregation != nil,
	)

	if q.Aggregation != nil {
		buckets := newAggBuckets(db.config.Storage.MaxMemory)
		window := q.Aggregation.Window
		if window <= 0 {
			return nil, newQueryError(QueryErrorTypeInvalid,
				"aggregation window must be positive", q, nil)
		}
		for _, p := range partitions {
			// Check for context cancellation
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					return nil, newQueryError(QueryErrorTypeTimeout, "query timeout", q, ctx.Err())
				}
				return nil, newQueryError(QueryErrorTypeCanceled, "query canceled", q, ctx.Err())
			default:
			}

			if err := p.aggregateIntoContext(ctx, db, q, allowed, buckets); err != nil {
				return nil, err
			}
		}
		points := buckets.finalize(q.Aggregation.Function, window, q.Aggregation.Percentile)
		if q.Limit > 0 && len(points) > q.Limit {
			points = points[:q.Limit]
		}
		return &Result{Points: points}, nil
	}

	var points []Point
	var usedBytes int64
	for _, p := range partitions {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return nil, newQueryError(QueryErrorTypeTimeout, "query timeout", q, ctx.Err())
			}
			return nil, newQueryError(QueryErrorTypeCanceled, "query canceled", q, ctx.Err())
		default:
		}

		pts, err := p.queryContext(ctx, db, q, allowed)
		if err != nil {
			return nil, err
		}
		points = append(points, pts...)
		if db.config.Storage.MaxMemory > 0 {
			usedBytes += int64(len(pts)) * 48
			if usedBytes > db.config.Storage.MaxMemory {
				return nil, newQueryError(QueryErrorTypeMemory, "query memory budget exceeded", q, nil)
			}
		}
	}

	if q.Limit > 0 && len(points) > q.Limit {
		points = points[:q.Limit]
	}

	return &Result{Points: points}, nil
}

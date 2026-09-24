package chronicle

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"time"
)

// query executes a query on this partition.
// Deprecated: Use queryContext instead for cancellation support.
func (p *Partition) query(db *DB, q *Query, allowed map[uint64]struct{}) ([]Point, error) {
	return p.queryContext(context.Background(), db, q, allowed)
}

// aggregateInto aggregates data into the provided buckets.
// Deprecated: Use aggregateIntoContext instead for cancellation support.
func (p *Partition) aggregateInto(db *DB, q *Query, allowed map[uint64]struct{}, buckets *aggBuckets) error {
	return p.aggregateIntoContext(context.Background(), db, q, allowed, buckets)
}

// ensureLoaded loads partition data from disk if not already loaded.
// Deprecated: Use ensureLoadedContext instead for cancellation support.
func (p *Partition) ensureLoaded(db *DB) error {
	return p.ensureLoadedContext(context.Background(), db)
}

func (p *Partition) ensureLoadedContext(ctx context.Context, db *DB) error {
	p.mu.Lock()
	if p.loaded {
		p.mu.Unlock()
		return nil
	}
	defer p.mu.Unlock()

	// Check context before potentially expensive I/O
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if p.length == 0 && p.offset == 0 {
		p.loaded = true
		return nil
	}

	// Load partition data using the DataStore abstraction.
	var data []byte
	var err error
	data, err = db.dataStore.ReadPartitionAt(ctx, p.offset, p.length)
	if errors.Is(err, ErrUnsupportedOperation) {
		data, err = db.dataStore.ReadPartition(ctx, p.id)
	}

	if err != nil {
		return err
	}
	decoded, err := decodePartition(data)
	if err != nil {
		return err
	}

	p.series = decoded.series
	p.minTime = decoded.minTime
	p.maxTime = decoded.maxTime
	p.pointCount = decoded.pointCount
	p.loaded = true
	return nil
}

// queryContext is like query but supports context cancellation.
func (p *Partition) queryContext(ctx context.Context, db *DB, q *Query, allowed map[uint64]struct{}) ([]Point, error) {
	if err := p.ensureLoadedContext(ctx, db); err != nil {
		return nil, err
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	// Pre-allocate with estimated capacity to reduce slice growth.
	estimatedCap := 0
	for _, series := range p.series {
		estimatedCap += len(series.Timestamps)
	}
	if q.Limit > 0 && estimatedCap > q.Limit {
		estimatedCap = q.Limit
	}
	points := make([]Point, 0, estimatedCap)

	sorted := true
	var lastTS int64

	for _, series := range p.series {
		// Check context periodically during iteration
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if allowed != nil {
			if _, ok := allowed[series.Series.ID]; !ok {
				continue
			}
		}
		if q.Start != 0 && series.MaxTime != 0 && series.MaxTime < q.Start {
			continue
		}
		if q.End != 0 && series.MinTime != 0 && series.MinTime >= q.End {
			continue
		}
		if q.Metric != "" && series.Series.Metric != q.Metric {
			continue
		}
		if !matchesTags(series.Series.Tags, q.Tags) {
			continue
		}
		if !matchesTagFilters(series.Series.Tags, q.TagFilters) {
			continue
		}

		// Share tag reference — tags are immutable within a partition.
		tags := series.Series.Tags
		for i, ts := range series.Timestamps {
			if q.Start != 0 && ts < q.Start {
				continue
			}
			if q.End != 0 && ts >= q.End {
				continue
			}
			if ts < lastTS {
				sorted = false
			}
			lastTS = ts
			points = append(points, Point{
				Metric:    series.Series.Metric,
				Tags:      tags,
				Value:     series.Values[i],
				Timestamp: ts,
			})
		}
	}

	// Only sort if data isn't already in order.
	if !sorted {
		sort.Slice(points, func(i, j int) bool {
			return points[i].Timestamp < points[j].Timestamp
		})
	}

	if q.Limit > 0 && len(points) > q.Limit {
		points = points[:q.Limit]
	}

	return points, nil
}

// aggregateIntoContext is like aggregateInto but supports context cancellation.
func (p *Partition) aggregateIntoContext(ctx context.Context, db *DB, q *Query, allowed map[uint64]struct{}, buckets *aggBuckets) error {
	if err := p.ensureLoadedContext(ctx, db); err != nil {
		return err
	}
	if q.Aggregation == nil {
		return nil
	}
	window := q.Aggregation.Window
	if window <= 0 {
		window = time.Second
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, series := range p.series {
		// Check context periodically during iteration
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if allowed != nil {
			if _, ok := allowed[series.Series.ID]; !ok {
				continue
			}
		}
		if q.Start != 0 && series.MaxTime != 0 && series.MaxTime < q.Start {
			continue
		}
		if q.End != 0 && series.MinTime != 0 && series.MinTime >= q.End {
			continue
		}
		if q.Metric != "" && series.Series.Metric != q.Metric {
			continue
		}
		if !matchesTags(series.Series.Tags, q.Tags) {
			continue
		}
		if !matchesTagFilters(series.Series.Tags, q.TagFilters) {
			continue
		}

		for i, ts := range series.Timestamps {
			if q.Start != 0 && ts < q.Start {
				continue
			}
			if q.End != 0 && ts >= q.End {
				continue
			}
			if err := buckets.add(series.Series.Tags, ts, series.Values[i], window, q.GroupBy, q.Aggregation.Function); err != nil {
				return err
			}
		}
	}
	return nil
}

func matchesTags(seriesTags, filter map[string]string) bool {
	if len(filter) == 0 {
		return true
	}
	for k, v := range filter {
		if seriesTags[k] != v {
			return false
		}
	}
	return true
}

func matchesTagFilters(seriesTags map[string]string, filters []TagFilter) bool {
	if len(filters) == 0 {
		return true
	}
	for _, filter := range filters {
		value, ok := seriesTags[filter.Key]
		switch filter.Op {
		case TagOpEq:
			if !ok || len(filter.Values) == 0 || value != filter.Values[0] {
				return false
			}
		case TagOpNotEq:
			if ok && len(filter.Values) > 0 && value == filter.Values[0] {
				return false
			}
		case TagOpIn:
			matched := false
			for _, v := range filter.Values {
				if value == v {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		case TagOpNotIn:
			if ok {
				for _, v := range filter.Values {
					if value == v {
						return false
					}
				}
			}
		case TagOpRegex:
			if len(filter.Values) == 0 {
				return false
			}
			re := filter.compiledRe
			if re == nil {
				compiled, err := regexp.Compile(filter.Values[0])
				if err != nil {
					return false
				}
				re = compiled
			}
			if !re.MatchString(value) {
				return false
			}
		case TagOpNotRegex:
			if len(filter.Values) > 0 {
				re := filter.compiledRe
				if re == nil {
					compiled, err := regexp.Compile(filter.Values[0])
					if err != nil {
						return false
					}
					re = compiled
				}
				if re.MatchString(value) {
					return false
				}
			}
		default:
			return false
		}
	}
	return true
}

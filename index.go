package chronicle

import (
	"sort"
	"sync"
)

// Index tracks partitions and metrics.
type Index struct {
	mu           sync.RWMutex
	partitions   []*Partition
	byID         map[uint64]*Partition
	metrics      map[string]struct{}
	seriesByKey  map[string]Series
	seriesByID   map[uint64]Series
	metricSeries map[string]map[uint64]struct{}
	tagSeries    map[string]map[string]map[uint64]struct{}
	nextSeriesID uint64
	timeIndex    *BTree
}

func newIndex() *Index {
	return &Index{
		byID:         make(map[uint64]*Partition),
		metrics:      make(map[string]struct{}),
		seriesByKey:  make(map[string]Series),
		seriesByID:   make(map[uint64]Series),
		metricSeries: make(map[string]map[uint64]struct{}),
		tagSeries:    make(map[string]map[string]map[uint64]struct{}),
		timeIndex:    newBTree(8),
	}
}

func (idx *Index) GetOrCreatePartition(id uint64, start, end int64) *Partition {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if part, ok := idx.byID[id]; ok {
		return part
	}

	part := &Partition{
		id:        id,
		startTime: start,
		endTime:   end,
		series:    make(map[string]*SeriesData),
		loaded:    true,
	}
	idx.byID[id] = part
	idx.partitions = append(idx.partitions, part)
	sort.Slice(idx.partitions, func(i, j int) bool {
		return idx.partitions[i].startTime < idx.partitions[j].startTime
	})
	idx.timeIndex.Insert(start, part)

	return part
}

func (idx *Index) FindPartitions(start, end int64) []*Partition {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.partitions) == 0 || (start != 0 && end != 0 && start >= end) {
		return nil
	}

	// Select interval overlaps, not just partitions whose start falls inside
	// the query. The partition slice is maintained in start-time order.
	var result []*Partition
	for _, part := range idx.partitions {
		if end != 0 && part.startTime >= end {
			break
		}
		if start != 0 && part.endTime <= start {
			continue
		}
		result = append(result, part)
	}
	return result
}

func (idx *Index) RemovePartitionsBefore(cutoff int64) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(idx.partitions) == 0 {
		return false
	}

	var kept []*Partition
	removed := false
	for _, part := range idx.partitions {
		if part.endTime <= cutoff {
			delete(idx.byID, part.id)
			removed = true
			continue
		}
		kept = append(kept, part)
	}
	idx.partitions = kept
	if removed {
		idx.rebuildTimeIndexLocked()
	}
	return removed
}

func (idx *Index) RemoveOldestPartition() bool {
	if len(idx.partitions) == 0 {
		return false
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(idx.partitions) == 0 {
		return false
	}
	oldest := idx.partitions[0]
	idx.partitions = idx.partitions[1:]
	delete(idx.byID, oldest.id)
	idx.rebuildTimeIndexLocked()
	return true
}

func (idx *Index) RemovePartitionByID(id uint64) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if _, ok := idx.byID[id]; !ok {
		return false
	}

	delete(idx.byID, id)
	filtered := idx.partitions[:0]
	for _, part := range idx.partitions {
		if part.id == id {
			continue
		}
		filtered = append(filtered, part)
	}
	idx.partitions = filtered
	idx.rebuildTimeIndexLocked()
	return true
}

func (idx *Index) Metrics() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	out := make([]string, 0, len(idx.metrics))
	for m := range idx.metrics {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

func (idx *Index) RegisterMetric(metric string) {
	if metric == "" {
		return
	}
	idx.mu.Lock()
	idx.metrics[metric] = struct{}{}
	idx.mu.Unlock()
}

func (idx *Index) RegisterSeries(metric string, tags map[string]string) Series {
	if metric == "" {
		return Series{}
	}
	key := seriesKey(metric, tags)

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if series, ok := idx.seriesByKey[key]; ok {
		return series
	}

	idx.nextSeriesID++
	series := Series{ID: idx.nextSeriesID, Metric: metric, Tags: cloneTags(tags)}
	idx.seriesByKey[key] = series
	idx.seriesByID[series.ID] = series
	idx.metrics[metric] = struct{}{}

	metricSet := idx.metricSeries[metric]
	if metricSet == nil {
		metricSet = make(map[uint64]struct{})
		idx.metricSeries[metric] = metricSet
	}
	metricSet[series.ID] = struct{}{}

	for k, v := range tags {
		valueMap := idx.tagSeries[k]
		if valueMap == nil {
			valueMap = make(map[string]map[uint64]struct{})
			idx.tagSeries[k] = valueMap
		}
		set := valueMap[v]
		if set == nil {
			set = make(map[uint64]struct{})
			valueMap[v] = set
		}
		set[series.ID] = struct{}{}
	}

	return series
}

// DeleteMetric removes a metric and all associated series from the index.
// It also removes matching series data from all partitions.
func (idx *Index) DeleteMetric(metric string) bool {
	if metric == "" {
		return false
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if _, exists := idx.metrics[metric]; !exists {
		return false
	}

	// Collect series IDs to remove
	seriesIDs := idx.metricSeries[metric]
	delete(idx.metricSeries, metric)
	delete(idx.metrics, metric)

	// Remove series from indexes
	for sid := range seriesIDs {
		s, ok := idx.seriesByID[sid]
		if !ok {
			continue
		}
		key := seriesKey(s.Metric, s.Tags)
		delete(idx.seriesByKey, key)
		delete(idx.seriesByID, sid)

		// Remove from tag indexes
		for k, v := range s.Tags {
			if valueMap, ok := idx.tagSeries[k]; ok {
				if set, ok := valueMap[v]; ok {
					delete(set, sid)
					if len(set) == 0 {
						delete(valueMap, v)
					}
				}
				if len(valueMap) == 0 {
					delete(idx.tagSeries, k)
				}
			}
		}
	}

	// Remove series data from partitions
	for _, part := range idx.partitions {
		part.mu.Lock()
		for key, sd := range part.series {
			if sd.Series.Metric == metric {
				delete(part.series, key)
			}
		}
		part.mu.Unlock()
	}

	return true
}

func (idx *Index) FilterSeries(metric string, tags map[string]string) map[uint64]struct{} {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if metric == "" && len(tags) == 0 {
		return nil
	}

	var candidate map[uint64]struct{}
	if metric != "" {
		candidate = copySet(idx.metricSeries[metric])
	}

	for k, v := range tags {
		valueMap := idx.tagSeries[k]
		if valueMap == nil {
			return map[uint64]struct{}{}
		}
		set := valueMap[v]
		if set == nil {
			return map[uint64]struct{}{}
		}
		if candidate == nil {
			candidate = copySet(set)
		} else {
			candidate = intersectSets(candidate, set)
			if len(candidate) == 0 {
				return candidate
			}
		}
	}

	return candidate
}

func (idx *Index) rebuildTimeIndexLocked() {
	idx.timeIndex = newBTree(8)
	for _, part := range idx.partitions {
		idx.timeIndex.Insert(part.startTime, part)
	}
}

func copySet(src map[uint64]struct{}) map[uint64]struct{} {
	if src == nil {
		return nil
	}
	out := make(map[uint64]struct{}, len(src))
	for k := range src {
		out[k] = struct{}{}
	}
	return out
}

func intersectSets(a, b map[uint64]struct{}) map[uint64]struct{} {
	if a == nil {
		return copySet(b)
	}
	out := make(map[uint64]struct{})
	for k := range a {
		if _, ok := b[k]; ok {
			out[k] = struct{}{}
		}
	}
	return out
}

// Count returns the number of partitions in the index.
func (idx *Index) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.partitions)
}

// TagKeys returns all unique tag keys in the index.
func (idx *Index) TagKeys() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keys := make([]string, 0, len(idx.tagSeries))
	for k := range idx.tagSeries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TagKeysForMetric returns tag keys used by a specific metric.
func (idx *Index) TagKeysForMetric(metric string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	seriesIDs, ok := idx.metricSeries[metric]
	if !ok {
		return nil
	}

	// Collect all tag keys from series belonging to this metric
	keySet := make(map[string]struct{})
	for seriesID := range seriesIDs {
		if series, ok := idx.seriesByID[seriesID]; ok {
			for k := range series.Tags {
				keySet[k] = struct{}{}
			}
		}
	}

	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TagValues returns all unique values for a given tag key.
func (idx *Index) TagValues(key string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	valueMap, ok := idx.tagSeries[key]
	if !ok {
		return nil
	}

	values := make([]string, 0, len(valueMap))
	for v := range valueMap {
		values = append(values, v)
	}
	sort.Strings(values)
	return values
}

// TagValuesForMetric returns tag values for a key filtered by metric.
func (idx *Index) TagValuesForMetric(metric, key string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	seriesIDs, ok := idx.metricSeries[metric]
	if !ok {
		return nil
	}

	// Collect values for the key from series belonging to this metric
	valueSet := make(map[string]struct{})
	for seriesID := range seriesIDs {
		if series, ok := idx.seriesByID[seriesID]; ok {
			if v, hasKey := series.Tags[key]; hasKey {
				valueSet[v] = struct{}{}
			}
		}
	}

	values := make([]string, 0, len(valueSet))
	for v := range valueSet {
		values = append(values, v)
	}
	sort.Strings(values)
	return values
}

// SeriesCount returns the total number of series in the index.
func (idx *Index) SeriesCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.seriesByID)
}

// GetSeries returns a series by ID.
func (idx *Index) GetSeries(id uint64) (Series, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	s, ok := idx.seriesByID[id]
	return s, ok
}

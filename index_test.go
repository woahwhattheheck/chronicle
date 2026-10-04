package chronicle

import (
	"testing"
)

func TestNewIndex(t *testing.T) {
	idx := newIndex()

	if idx == nil {
		t.Fatal("newIndex should not return nil")
	}
	if idx.byID == nil {
		t.Error("byID map should be initialized")
	}
	if idx.metrics == nil {
		t.Error("metrics map should be initialized")
	}
	if idx.seriesByKey == nil {
		t.Error("seriesByKey map should be initialized")
	}
	if idx.seriesByID == nil {
		t.Error("seriesByID map should be initialized")
	}
	if idx.timeIndex == nil {
		t.Error("timeIndex should be initialized")
	}
}

func TestIndex_GetOrCreatePartition(t *testing.T) {
	idx := newIndex()

	part := idx.GetOrCreatePartition(1, 1000, 2000)
	if part == nil {
		t.Fatal("partition should not be nil")
	}
	if part.id != 1 {
		t.Errorf("expected id 1, got %d", part.id)
	}
	if part.startTime != 1000 {
		t.Errorf("expected startTime 1000, got %d", part.startTime)
	}
	if part.endTime != 2000 {
		t.Errorf("expected endTime 2000, got %d", part.endTime)
	}

	// Get same partition again
	part2 := idx.GetOrCreatePartition(1, 1000, 2000)
	if part != part2 {
		t.Error("should return same partition for same id")
	}
}

func TestIndex_FindPartitions(t *testing.T) {
	idx := newIndex()

	// Insert out of order to exercise the sorted index invariant.
	idx.GetOrCreatePartition(3, 3000, 4000)
	idx.GetOrCreatePartition(1, 1000, 2000)
	idx.GetOrCreatePartition(2, 2000, 3000)

	// Recovery keeps the order used by overlap selection.
	payload, err := encodeIndex(idx)
	if err != nil {
		t.Fatal(err)
	}
	idx, err = decodeIndex(payload)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name       string
		start, end int64
		ids        []uint64
	}{
		{name: "overlap", start: 1500, end: 2500, ids: []uint64{1, 2}},
		{name: "inside", start: 1500, end: 1600, ids: []uint64{1}},
		{name: "exact_boundaries", start: 2000, end: 3000, ids: []uint64{2}},
		{name: "all", start: 0, end: 5000, ids: []uint64{1, 2, 3}},
		{name: "before", start: 0, end: 1000},
		{name: "after", start: 4000, end: 5000},
		{name: "unbounded_end", start: 3500, ids: []uint64{3}},
		{name: "empty", start: 1500, end: 1500},
		{name: "reversed", start: 2500, end: 1500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parts := idx.FindPartitions(tc.start, tc.end)
			if len(parts) != len(tc.ids) {
				t.Fatalf("got %d partitions, want IDs %v", len(parts), tc.ids)
			}
			for i, part := range parts {
				if part.id != tc.ids[i] {
					t.Errorf("partition %d has ID %d, want %d", i, part.id, tc.ids[i])
				}
			}
		})
	}

	t.Run("unbounded_start_before_epoch", func(t *testing.T) {
		negative := newIndex()
		negative.GetOrCreatePartition(5, -3000, -2000)
		negative.GetOrCreatePartition(6, -1000, 1000)
		parts := negative.FindPartitions(0, -2500)
		if len(parts) != 1 || parts[0].id != 5 {
			t.Fatalf("expected the pre-epoch partition, got %+v", parts)
		}
		parts = negative.FindPartitions(0, 0)
		if len(parts) != 2 || parts[0].id != 5 || parts[1].id != 6 {
			t.Fatalf("expected both unbounded partitions, got %+v", parts)
		}
	})

	t.Run("earlier_long_overlap", func(t *testing.T) {
		idx.GetOrCreatePartition(4, 500, 4500)
		parts := idx.FindPartitions(2500, 2600)
		if len(parts) != 2 || parts[0].id != 4 || parts[1].id != 2 {
			t.Fatalf("expected the long and containing partitions, got %+v", parts)
		}
	})
}

func TestIndex_FindPartitions_Empty(t *testing.T) {
	idx := newIndex()

	parts := idx.FindPartitions(0, 1000)
	if parts != nil {
		t.Error("expected nil for empty index")
	}
}

func TestIndex_RemovePartitionsBefore(t *testing.T) {
	idx := newIndex()

	idx.GetOrCreatePartition(1, 1000, 2000)
	idx.GetOrCreatePartition(2, 2000, 3000)
	idx.GetOrCreatePartition(3, 3000, 4000)

	// Remove partitions ending before 2500
	removed := idx.RemovePartitionsBefore(2500)
	if !removed {
		t.Error("should have removed some partitions")
	}

	// Should have 2 partitions left
	parts := idx.FindPartitions(0, 5000)
	if len(parts) != 2 {
		t.Errorf("expected 2 partitions remaining, got %d", len(parts))
	}
}

func TestIndex_RemovePartitionsBefore_Empty(t *testing.T) {
	idx := newIndex()

	removed := idx.RemovePartitionsBefore(1000)
	if removed {
		t.Error("empty index should return false")
	}
}

func TestIndex_RemoveOldestPartition(t *testing.T) {
	idx := newIndex()

	idx.GetOrCreatePartition(1, 1000, 2000)
	idx.GetOrCreatePartition(2, 2000, 3000)

	removed := idx.RemoveOldestPartition()
	if !removed {
		t.Error("should have removed oldest partition")
	}

	// Verify only 1 remains
	parts := idx.FindPartitions(0, 5000)
	if len(parts) != 1 {
		t.Errorf("expected 1 partition, got %d", len(parts))
	}
}

func TestIndex_RemoveOldestPartition_Empty(t *testing.T) {
	idx := newIndex()

	removed := idx.RemoveOldestPartition()
	if removed {
		t.Error("empty index should return false")
	}
}

func TestIndex_RemovePartitionByID(t *testing.T) {
	idx := newIndex()

	idx.GetOrCreatePartition(1, 1000, 2000)
	idx.GetOrCreatePartition(2, 2000, 3000)

	removed := idx.RemovePartitionByID(1)
	if !removed {
		t.Error("should have removed partition")
	}

	// Verify partition is gone
	parts := idx.FindPartitions(0, 5000)
	if len(parts) != 1 {
		t.Errorf("expected 1 partition, got %d", len(parts))
	}
}

func TestIndex_RemovePartitionByID_NotFound(t *testing.T) {
	idx := newIndex()

	removed := idx.RemovePartitionByID(99)
	if removed {
		t.Error("should return false for non-existent partition")
	}
}

func TestIndex_Metrics(t *testing.T) {
	idx := newIndex()

	// Empty at start
	metrics := idx.Metrics()
	if len(metrics) != 0 {
		t.Error("new index should have no metrics")
	}

	// Register metrics
	idx.RegisterMetric("cpu")
	idx.RegisterMetric("memory")
	idx.RegisterMetric("disk")

	metrics = idx.Metrics()
	if len(metrics) != 3 {
		t.Errorf("expected 3 metrics, got %d", len(metrics))
	}

	// Should be sorted
	if metrics[0] != "cpu" || metrics[1] != "disk" || metrics[2] != "memory" {
		t.Error("metrics should be sorted alphabetically")
	}
}

func TestIndex_RegisterMetric_Empty(t *testing.T) {
	idx := newIndex()

	idx.RegisterMetric("")
	metrics := idx.Metrics()
	if len(metrics) != 0 {
		t.Error("empty metric name should be ignored")
	}
}

func TestIndex_RegisterSeries(t *testing.T) {
	idx := newIndex()

	series := idx.RegisterSeries("cpu", map[string]string{"host": "server1"})
	if series.ID == 0 {
		t.Error("series id should not be 0")
	}
	if series.Metric != "cpu" {
		t.Error("series metric not set correctly")
	}

	// Same series should return same id
	series2 := idx.RegisterSeries("cpu", map[string]string{"host": "server1"})
	if series.ID != series2.ID {
		t.Error("same series should have same id")
	}

	// Different series should have different id
	series3 := idx.RegisterSeries("cpu", map[string]string{"host": "server2"})
	if series.ID == series3.ID {
		t.Error("different series should have different id")
	}
}

func TestIndex_RegisterSeries_Empty(t *testing.T) {
	idx := newIndex()

	series := idx.RegisterSeries("", nil)
	if series.ID != 0 {
		t.Error("empty metric should return empty series")
	}
}

func TestIndex_FilterSeries(t *testing.T) {
	idx := newIndex()

	idx.RegisterSeries("cpu", map[string]string{"host": "server1", "region": "us"})
	idx.RegisterSeries("cpu", map[string]string{"host": "server2", "region": "us"})
	idx.RegisterSeries("cpu", map[string]string{"host": "server3", "region": "eu"})
	idx.RegisterSeries("memory", map[string]string{"host": "server1"})

	// Filter by metric
	result := idx.FilterSeries("cpu", nil)
	if len(result) != 3 {
		t.Errorf("expected 3 cpu series, got %d", len(result))
	}

	// Filter by tag
	result = idx.FilterSeries("cpu", map[string]string{"region": "us"})
	if len(result) != 2 {
		t.Errorf("expected 2 us series, got %d", len(result))
	}

	// Filter by multiple tags
	result = idx.FilterSeries("cpu", map[string]string{"host": "server1", "region": "us"})
	if len(result) != 1 {
		t.Errorf("expected 1 series, got %d", len(result))
	}
}

func TestIndex_FilterSeries_Empty(t *testing.T) {
	idx := newIndex()

	result := idx.FilterSeries("", nil)
	if result != nil {
		t.Error("empty filter should return nil")
	}
}

func TestIndex_FilterSeries_NoMatch(t *testing.T) {
	idx := newIndex()

	idx.RegisterSeries("cpu", map[string]string{"host": "server1"})

	// Non-existent tag
	result := idx.FilterSeries("cpu", map[string]string{"region": "us"})
	if len(result) != 0 {
		t.Error("should return empty set for non-matching filter")
	}

	// Non-existent metric
	result = idx.FilterSeries("disk", nil)
	if result != nil {
		t.Error("should return nil for non-existent metric")
	}
}

func TestIndex_Count(t *testing.T) {
	idx := newIndex()

	if idx.Count() != 0 {
		t.Error("new index should have 0 partitions")
	}

	idx.GetOrCreatePartition(1, 1000, 2000)
	idx.GetOrCreatePartition(2, 2000, 3000)

	if idx.Count() != 2 {
		t.Errorf("expected 2 partitions, got %d", idx.Count())
	}
}

func TestCopySet(t *testing.T) {
	original := map[uint64]struct{}{1: {}, 2: {}, 3: {}}
	copied := copySet(original)

	if len(copied) != 3 {
		t.Errorf("expected 3 elements, got %d", len(copied))
	}

	// Modify copy
	delete(copied, 1)
	if len(original) != 3 {
		t.Error("copy should be independent")
	}

	// Nil case
	if copySet(nil) != nil {
		t.Error("nil should return nil")
	}
}

func TestIntersectSets(t *testing.T) {
	a := map[uint64]struct{}{1: {}, 2: {}, 3: {}}
	b := map[uint64]struct{}{2: {}, 3: {}, 4: {}}

	result := intersectSets(a, b)
	if len(result) != 2 {
		t.Errorf("expected 2 elements, got %d", len(result))
	}

	// Check correct elements
	if _, ok := result[2]; !ok {
		t.Error("should contain 2")
	}
	if _, ok := result[3]; !ok {
		t.Error("should contain 3")
	}

	// Nil case
	result = intersectSets(nil, b)
	if len(result) != 3 {
		t.Error("nil a should return copy of b")
	}
}

package chronicle

import "testing"

func TestIndexFindPartitionsOverlappingWindow(t *testing.T) {
	idx := newIndex()
	// Insert out of order, with a long partition spanning shorter ones.
	idx.GetOrCreatePartition(3, 2000, 3000)
	idx.GetOrCreatePartition(1, -1000, -500)
	idx.GetOrCreatePartition(4, 2500, 2600)
	idx.GetOrCreatePartition(2, 1000, 4000)

	for _, tc := range []struct {
		name       string
		start, end int64
		ids        []uint64
	}{
		{"inside_earlier_partition", 1500, 1600, []uint64{2}},
		{"cross_partition", 1900, 2100, []uint64{2, 3}},
		{"overlap_before_last_predecessor", 2700, 2800, []uint64{2, 3}},
		{"exclusive_end", 1500, 2000, []uint64{2}},
		{"ended_partitions_excluded", 3000, 3500, []uint64{2}},
		{"unbounded_start", 0, 1000, []uint64{1}},
		{"unbounded_end", 3500, 0, []uint64{2}},
		{"no_overlap", 4000, 5000, nil},
		{"empty_window", 1500, 1500, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parts := idx.FindPartitions(tc.start, tc.end)
			if len(parts) != len(tc.ids) {
				t.Fatalf("FindPartitions(%d, %d): got %d partitions, want IDs %v", tc.start, tc.end, len(parts), tc.ids)
			}
			for i, id := range tc.ids {
				if parts[i].id != id {
					t.Fatalf("partition %d: got ID %d, want %d", i, parts[i].id, id)
				}
			}
		})
	}
}

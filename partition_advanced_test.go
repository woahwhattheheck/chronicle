package chronicle

import (
	"strconv"
	"testing"
)

func TestHashPartitioner(t *testing.T) {
	hp := NewHashPartitioner(4)
	if hp.NumPartitions() != 4 {
		t.Fatalf("expected 4 partitions, got %d", hp.NumPartitions())
	}

	// Same metric should always go to the same partition
	p1 := hp.Partition("cpu_usage")
	p2 := hp.Partition("cpu_usage")
	if p1 != p2 {
		t.Fatal("same metric should go to same partition")
	}

	// Partition index should be within range
	for _, metric := range []string{"cpu", "memory", "disk", "network"} {
		idx := hp.Partition(metric)
		if idx < 0 || idx >= 4 {
			t.Fatalf("partition index %d out of range for metric %s", idx, metric)
		}
	}
}

func TestHashPartitioner_KnownBuckets(t *testing.T) {
	// These FNV-1a mappings are the same on 32-bit and 64-bit platforms.
	// "cpu" (0xf94af989) and "memory" (0x84e9f1ae) have the high bit set.
	for _, tt := range []struct {
		metric     string
		partitions int
		want       int
	}{
		{"cpu", 3, 1},
		{"memory", 3, 0},
		{"disk", 3, 0},
		{"network", 3, 2},
		{"cpu", 7, 4},
		{"cpu_usage", 16, 13},
		{"", 257, 196},
	} {
		if got := NewHashPartitioner(tt.partitions).Partition(tt.metric); got != tt.want {
			t.Errorf("Partition(%q) with %d buckets = %d, want %d", tt.metric, tt.partitions, got, tt.want)
		}
	}
	if strconv.IntSize == 64 {
		// Do not truncate a valid 64-bit partition count to uint32.
		count := uint64(1)<<32 + 1
		if got := NewHashPartitioner(int(count)).Partition("cpu"); uint64(got) != 4182440329 {
			t.Errorf("Partition(cpu) with %d buckets = %d, want 4182440329", count, got)
		}
	}
}

func TestHashPartitioner_PartitionPoints(t *testing.T) {
	hp := NewHashPartitioner(3)
	points := []Point{
		{Metric: "cpu", Value: 1.0, Timestamp: 1000},
		{Metric: "memory", Value: 2.0, Timestamp: 1000},
		{Metric: "disk", Value: 3.0, Timestamp: 1000},
		{Metric: "cpu", Value: 4.0, Timestamp: 2000},
	}

	buckets := hp.PartitionPoints(points)
	if len(buckets) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(buckets))
	}

	// Total points should be preserved
	total := 0
	wantBuckets := map[string]int{"cpu": 1, "memory": 0, "disk": 0}
	seen := make(map[float64]bool)
	for index, b := range buckets {
		total += len(b)
		for _, point := range b {
			if want, ok := wantBuckets[point.Metric]; !ok || index != want {
				t.Errorf("metric %q in bucket %d, want %d", point.Metric, index, want)
			}
			if seen[point.Value] {
				t.Errorf("point value %v appears more than once", point.Value)
			}
			seen[point.Value] = true
		}
	}
	if total != 4 {
		t.Fatalf("expected 4 total points, got %d", total)
	}
}

func TestRangePartitioner(t *testing.T) {
	rp := NewRangePartitioner("region", []string{"eu", "us"})

	// Values before first boundary go to bucket 0
	idx := rp.Partition("ap")
	if idx != 0 {
		t.Fatalf("expected bucket 0 for 'ap', got %d", idx)
	}

	// "eu" matches boundary, goes to bucket where SearchStrings places it
	idx = rp.Partition("eu")
	if idx < 0 || idx > 2 {
		t.Fatalf("expected valid bucket for 'eu', got %d", idx)
	}

	// Values after "us" go to last bucket
	idx = rp.Partition("za")
	if idx != 2 {
		t.Fatalf("expected bucket 2 for 'za', got %d", idx)
	}
}

func TestRangePartitioner_PartitionPoints(t *testing.T) {
	rp := NewRangePartitioner("region", []string{"eu", "us"})
	points := []Point{
		{Metric: "cpu", Tags: map[string]string{"region": "ap"}, Timestamp: 1000},
		{Metric: "cpu", Tags: map[string]string{"region": "eu"}, Timestamp: 1000},
		{Metric: "cpu", Tags: map[string]string{"region": "us"}, Timestamp: 1000},
	}

	buckets := rp.PartitionPoints(points)
	if len(buckets) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(buckets))
	}

	total := 0
	for _, b := range buckets {
		total += len(b)
	}
	if total != 3 {
		t.Fatalf("expected 3 total points, got %d", total)
	}
}

func TestDynamicPartitionManager_EvaluateSplit(t *testing.T) {
	cfg := DefaultDynamicPartitionConfig()
	cfg.MaxPointCount = 100 // low threshold for testing
	mgr := NewDynamicPartitionManager(cfg)

	p := &Partition{
		id:         1,
		startTime:  0,
		endTime:    10000,
		minTime:    100,
		maxTime:    9900,
		pointCount: 200, // exceeds threshold
		series:     make(map[string]*SeriesData),
	}

	decision := mgr.EvaluateSplit(p)
	if decision == nil {
		t.Fatal("expected split decision")
	}
	if decision.Reason == "" {
		t.Fatal("expected reason for split")
	}
}

func TestDynamicPartitionManager_NoSplit(t *testing.T) {
	cfg := DefaultDynamicPartitionConfig()
	mgr := NewDynamicPartitionManager(cfg)

	p := &Partition{
		id:         1,
		pointCount: 10,
		size:       1024,
		series:     make(map[string]*SeriesData),
	}

	decision := mgr.EvaluateSplit(p)
	if decision != nil {
		t.Fatal("expected no split decision")
	}
}

func TestDynamicPartitionManager_SplitPartition(t *testing.T) {
	mgr := NewDynamicPartitionManager(DefaultDynamicPartitionConfig())

	p := &Partition{
		id:        1,
		startTime: 0,
		endTime:   10000,
		series: map[string]*SeriesData{
			"cpu": {
				Series:     Series{Metric: "cpu"},
				Timestamps: []int64{1000, 3000, 5000, 7000, 9000},
				Values:     []float64{1, 2, 3, 4, 5},
				MinTime:    1000,
				MaxTime:    9000,
			},
		},
		pointCount: 5,
		minTime:    1000,
		maxTime:    9000,
	}

	left, right, err := mgr.SplitPartition(p, 5000)
	if err != nil {
		t.Fatalf("split: %v", err)
	}

	if left.pointCount+right.pointCount != 5 {
		t.Fatalf("point count mismatch: %d + %d != 5", left.pointCount, right.pointCount)
	}
	if left.endTime != 5000 {
		t.Fatalf("expected left end time 5000, got %d", left.endTime)
	}
	if right.startTime != 5000 {
		t.Fatalf("expected right start time 5000, got %d", right.startTime)
	}
}

func TestBloomFilter(t *testing.T) {
	bf := NewPartitionBloomFilter(1000, 0.01)

	// Add some items
	bf.Add("region", "us-east")
	bf.Add("region", "eu-west")
	bf.AddKey("region")

	// Should report possibly present
	if !bf.MayContain("region", "us-east") {
		t.Fatal("expected MayContain to return true for added item")
	}
	if !bf.MayContain("region", "eu-west") {
		t.Fatal("expected MayContain to return true for added item")
	}
	if !bf.MayContainKey("region") {
		t.Fatal("expected MayContainKey to return true for added key")
	}

	// Non-existent key should likely return false
	if bf.MayContainKey("nonexistent-key-xyz-123") {
		// Bloom filters can have false positives, but this specific one shouldn't
		t.Log("unexpected false positive for non-existent key (acceptable for bloom filter)")
	}

	if bf.Count() != 2 {
		t.Fatalf("expected count 2, got %d", bf.Count())
	}

	fpRate := bf.FalsePositiveRate()
	if fpRate < 0 || fpRate > 1 {
		t.Fatalf("invalid false positive rate: %f", fpRate)
	}
}

func TestPrunePartitionsWithBloom(t *testing.T) {
	// Create partitions with bloom filters
	p1 := &Partition{id: 1, series: make(map[string]*SeriesData)}
	p2 := &Partition{id: 2, series: make(map[string]*SeriesData)}

	bf1 := NewPartitionBloomFilter(100, 0.01)
	bf1.Add("region", "us-east")

	bf2 := NewPartitionBloomFilter(100, 0.01)
	bf2.Add("region", "eu-west")

	blooms := map[uint64]*PartitionBloomFilter{
		1: bf1,
		2: bf2,
	}

	// Query for region=us-east should prune partition 2
	remaining, result := PrunePartitionsWithBloom(
		[]*Partition{p1, p2},
		blooms,
		map[string]string{"region": "us-east"},
	)

	if result.PrunedPartitions != 1 {
		t.Fatalf("expected 1 pruned partition, got %d", result.PrunedPartitions)
	}
	if len(remaining) != 1 {
		t.Fatalf("expected 1 remaining partition, got %d", len(remaining))
	}
	if remaining[0].id != 1 {
		t.Fatalf("expected partition 1 to remain, got %d", remaining[0].id)
	}
}

func TestDynamicPartitionManager_EvaluateMerge(t *testing.T) {
	cfg := DefaultDynamicPartitionConfig()
	cfg.MinPartitionSize = 1024 * 1024 // 1MB
	mgr := NewDynamicPartitionManager(cfg)

	p1 := &Partition{id: 1, startTime: 0, endTime: 5000, size: 100, pointCount: 10, series: make(map[string]*SeriesData)}
	p2 := &Partition{id: 2, startTime: 5000, endTime: 10000, size: 200, pointCount: 20, series: make(map[string]*SeriesData)}

	decisions := mgr.EvaluateMerge([]*Partition{p1, p2})
	if len(decisions) != 1 {
		t.Fatalf("expected 1 merge decision, got %d", len(decisions))
	}
	if len(decisions[0].PartitionIDs) != 2 {
		t.Fatal("expected 2 partition IDs in merge decision")
	}
}

package chronicle

import (
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// --- Partition Strategy Types ---

// PartitionStrategy defines the partitioning approach.
type PartitionStrategy int

const (
	// PartitionByTime partitions by time range (default).
	PartitionByTime PartitionStrategy = iota
	// PartitionByHash partitions by metric name hash for parallel scan scaling.
	PartitionByHash
	// PartitionByRange partitions by tag cardinality ranges.
	PartitionByRange
	// PartitionByHybrid combines time and hash partitioning.
	PartitionByHybrid
)

// --- Hash Partitioning ---

// HashPartitioner distributes points across N partitions by metric name hash.
type HashPartitioner struct {
	numPartitions int
	mu            sync.RWMutex
}

// NewHashPartitioner creates a hash partitioner with the given partition count.
func NewHashPartitioner(numPartitions int) *HashPartitioner {
	if numPartitions <= 0 {
		numPartitions = 16
	}
	return &HashPartitioner{numPartitions: numPartitions}
}

// Partition returns the partition index for a given metric name.
func (hp *HashPartitioner) Partition(metric string) int {
	h := fnv.New32a()
	h.Write([]byte(metric))
	// Reduce the unsigned hash before converting to a platform-sized index.
	return int(uint(h.Sum32()) % uint(hp.numPartitions))
}

// PartitionPoints distributes points across hash partitions.
func (hp *HashPartitioner) PartitionPoints(points []Point) [][]Point {
	hp.mu.RLock()
	n := hp.numPartitions
	hp.mu.RUnlock()

	buckets := make([][]Point, n)
	for i := range buckets {
		buckets[i] = make([]Point, 0)
	}

	for _, p := range points {
		idx := hp.Partition(p.Metric)
		buckets[idx] = append(buckets[idx], p)
	}
	return buckets
}

// NumPartitions returns the number of hash partitions.
func (hp *HashPartitioner) NumPartitions() int {
	hp.mu.RLock()
	defer hp.mu.RUnlock()
	return hp.numPartitions
}

// --- Range Partitioning ---

// RangePartitioner partitions by tag cardinality with configurable boundaries.
type RangePartitioner struct {
	tagKey     string
	boundaries []string
	mu         sync.RWMutex
}

// NewRangePartitioner creates a range partitioner for the given tag key with boundaries.
func NewRangePartitioner(tagKey string, boundaries []string) *RangePartitioner {
	sorted := make([]string, len(boundaries))
	copy(sorted, boundaries)
	sort.Strings(sorted)

	return &RangePartitioner{
		tagKey:     tagKey,
		boundaries: sorted,
	}
}

// Partition returns the partition index for a given tag value.
func (rp *RangePartitioner) Partition(tagValue string) int {
	rp.mu.RLock()
	defer rp.mu.RUnlock()

	idx := sort.SearchStrings(rp.boundaries, tagValue)
	return idx
}

// PartitionPoints distributes points across range partitions.
func (rp *RangePartitioner) PartitionPoints(points []Point) [][]Point {
	rp.mu.RLock()
	n := len(rp.boundaries) + 1
	tagKey := rp.tagKey
	rp.mu.RUnlock()

	buckets := make([][]Point, n)
	for i := range buckets {
		buckets[i] = make([]Point, 0)
	}

	for _, p := range points {
		tagVal := ""
		if p.Tags != nil {
			tagVal = p.Tags[tagKey]
		}
		idx := rp.Partition(tagVal)
		if idx >= n {
			idx = n - 1
		}
		buckets[idx] = append(buckets[idx], p)
	}
	return buckets
}

// SetBoundaries updates the partition boundaries.
func (rp *RangePartitioner) SetBoundaries(boundaries []string) {
	sorted := make([]string, len(boundaries))
	copy(sorted, boundaries)
	sort.Strings(sorted)

	rp.mu.Lock()
	defer rp.mu.Unlock()
	rp.boundaries = sorted
}

// Boundaries returns the current partition boundaries.
func (rp *RangePartitioner) Boundaries() []string {
	rp.mu.RLock()
	defer rp.mu.RUnlock()
	result := make([]string, len(rp.boundaries))
	copy(result, rp.boundaries)
	return result
}

// --- Dynamic Partition Split/Merge ---

// DynamicPartitionConfig configures dynamic partition split/merge behavior.
type DynamicPartitionConfig struct {
	// MaxPartitionSize triggers a split when exceeded (bytes).
	MaxPartitionSize int64 `json:"max_partition_size"`
	// MinPartitionSize triggers a merge when below this threshold.
	MinPartitionSize int64 `json:"min_partition_size"`
	// MaxPointCount triggers a split when exceeded.
	MaxPointCount int64 `json:"max_point_count"`
	// MinPointCount triggers a merge when below.
	MinPointCount int64 `json:"min_point_count"`
	// EvaluationInterval defines how often to check for split/merge.
	EvaluationInterval time.Duration `json:"evaluation_interval"`
}

// DefaultDynamicPartitionConfig returns sensible defaults.
func DefaultDynamicPartitionConfig() DynamicPartitionConfig {
	return DynamicPartitionConfig{
		MaxPartitionSize:   256 * 1024 * 1024, // 256 MB
		MinPartitionSize:   1 * 1024 * 1024,   // 1 MB
		MaxPointCount:      10_000_000,
		MinPointCount:      1_000,
		EvaluationInterval: 5 * time.Minute,
	}
}

// SplitDecision describes a decision to split a partition.
type SplitDecision struct {
	PartitionID uint64 `json:"partition_id"`
	Reason      string `json:"reason"`
	SplitPoint  int64  `json:"split_point"` // timestamp to split at
	LeftSize    int64  `json:"left_size"`
	RightSize   int64  `json:"right_size"`
}

// MergeDecision describes a decision to merge two partitions.
type MergeDecision struct {
	PartitionIDs []uint64 `json:"partition_ids"`
	Reason       string   `json:"reason"`
	MergedSize   int64    `json:"merged_size"`
}

// DynamicPartitionManager evaluates partitions for split/merge operations.
type DynamicPartitionManager struct {
	config DynamicPartitionConfig
	mu     sync.RWMutex
	splits atomic.Uint64
	merges atomic.Uint64
}

// NewDynamicPartitionManager creates a new partition manager.
func NewDynamicPartitionManager(config DynamicPartitionConfig) *DynamicPartitionManager {
	return &DynamicPartitionManager{config: config}
}

// EvaluateSplit checks if a partition should be split.
func (m *DynamicPartitionManager) EvaluateSplit(p *Partition) *SplitDecision {
	if p == nil {
		return nil
	}

	p.mu.RLock()
	size := p.size
	count := p.pointCount
	minT := p.minTime
	maxT := p.maxTime
	p.mu.RUnlock()

	m.mu.RLock()
	cfg := m.config
	m.mu.RUnlock()

	if size > cfg.MaxPartitionSize {
		midpoint := minT + (maxT-minT)/2
		return &SplitDecision{
			PartitionID: p.id,
			Reason:      "size exceeds threshold",
			SplitPoint:  midpoint,
			LeftSize:    size / 2,
			RightSize:   size / 2,
		}
	}

	if count > cfg.MaxPointCount {
		midpoint := minT + (maxT-minT)/2
		return &SplitDecision{
			PartitionID: p.id,
			Reason:      "point count exceeds threshold",
			SplitPoint:  midpoint,
			LeftSize:    size / 2,
			RightSize:   size / 2,
		}
	}

	return nil
}

// EvaluateMerge checks if adjacent partitions should be merged.
func (m *DynamicPartitionManager) EvaluateMerge(partitions []*Partition) []MergeDecision {
	if len(partitions) < 2 {
		return nil
	}

	m.mu.RLock()
	cfg := m.config
	m.mu.RUnlock()

	// Sort by start time
	sorted := make([]*Partition, len(partitions))
	copy(sorted, partitions)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].startTime < sorted[j].startTime
	})

	var decisions []MergeDecision
	for i := 0; i < len(sorted)-1; i++ {
		a := sorted[i]
		b := sorted[i+1]

		a.mu.RLock()
		aSize := a.size
		aCount := a.pointCount
		a.mu.RUnlock()

		b.mu.RLock()
		bSize := b.size
		bCount := b.pointCount
		b.mu.RUnlock()

		combinedSize := aSize + bSize
		combinedCount := aCount + bCount

		if aSize < cfg.MinPartitionSize && bSize < cfg.MinPartitionSize &&
			combinedSize < cfg.MaxPartitionSize && combinedCount < cfg.MaxPointCount {
			decisions = append(decisions, MergeDecision{
				PartitionIDs: []uint64{a.id, b.id},
				Reason:       "both partitions below minimum size",
				MergedSize:   combinedSize,
			})
			i++ // skip next partition since it's part of this merge
		}
	}

	return decisions
}

// SplitPartition performs a copy-on-write split of a partition at the given timestamp.
func (m *DynamicPartitionManager) SplitPartition(p *Partition, splitPoint int64) (*Partition, *Partition, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	left := &Partition{
		id:        p.id * 2,
		startTime: p.startTime,
		endTime:   splitPoint,
		series:    make(map[string]*SeriesData),
	}

	right := &Partition{
		id:        p.id*2 + 1,
		startTime: splitPoint,
		endTime:   p.endTime,
		series:    make(map[string]*SeriesData),
	}

	for key, sd := range p.series {
		leftSD := &SeriesData{Series: sd.Series}
		rightSD := &SeriesData{Series: sd.Series}

		for i, ts := range sd.Timestamps {
			if ts < splitPoint {
				leftSD.Timestamps = append(leftSD.Timestamps, ts)
				leftSD.Values = append(leftSD.Values, sd.Values[i])
				if leftSD.MinTime == 0 || ts < leftSD.MinTime {
					leftSD.MinTime = ts
				}
				if ts > leftSD.MaxTime {
					leftSD.MaxTime = ts
				}
				left.pointCount++
			} else {
				rightSD.Timestamps = append(rightSD.Timestamps, ts)
				rightSD.Values = append(rightSD.Values, sd.Values[i])
				if rightSD.MinTime == 0 || ts < rightSD.MinTime {
					rightSD.MinTime = ts
				}
				if ts > rightSD.MaxTime {
					rightSD.MaxTime = ts
				}
				right.pointCount++
			}
		}

		if len(leftSD.Timestamps) > 0 {
			left.series[key] = leftSD
			if left.minTime == 0 || leftSD.MinTime < left.minTime {
				left.minTime = leftSD.MinTime
			}
			if leftSD.MaxTime > left.maxTime {
				left.maxTime = leftSD.MaxTime
			}
		}
		if len(rightSD.Timestamps) > 0 {
			right.series[key] = rightSD
			if right.minTime == 0 || rightSD.MinTime < right.minTime {
				right.minTime = rightSD.MinTime
			}
			if rightSD.MaxTime > right.maxTime {
				right.maxTime = rightSD.MaxTime
			}
		}
	}

	m.splits.Add(1)
	return left, right, nil
}

// Stats returns split/merge statistics.
func (m *DynamicPartitionManager) Stats() map[string]uint64 {
	return map[string]uint64{
		"splits": m.splits.Load(),
		"merges": m.merges.Load(),
	}
}

// --- Per-Partition Bloom Filter ---

// PartitionBloomFilter provides probabilistic tag existence checks.
type PartitionBloomFilter struct {
	bits    []uint64
	numBits uint32
	numHash uint32
	count   uint32
	mu      sync.RWMutex
}

// NewPartitionBloomFilter creates a bloom filter optimized for n expected items
// with the given false positive rate.
func NewPartitionBloomFilter(expectedItems int, fpRate float64) *PartitionBloomFilter {
	if expectedItems <= 0 {
		expectedItems = 1000
	}
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = 0.01
	}

	// Calculate optimal size: m = -(n * ln(p)) / (ln(2)^2)
	m := -float64(expectedItems) * math.Log(fpRate) / (math.Ln2 * math.Ln2)
	numBits := uint32(math.Ceil(m))
	if numBits < 64 {
		numBits = 64
	}

	// Calculate optimal hash count: k = (m/n) * ln(2)
	k := float64(numBits) / float64(expectedItems) * math.Ln2
	numHash := uint32(math.Ceil(k))
	if numHash < 1 {
		numHash = 1
	}
	if numHash > 20 {
		numHash = 20
	}

	words := (numBits + 63) / 64
	return &PartitionBloomFilter{
		bits:    make([]uint64, words),
		numBits: numBits,
		numHash: numHash,
	}
}

// Add adds a tag key-value pair to the bloom filter.
func (bf *PartitionBloomFilter) Add(tagKey, tagValue string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	item := tagKey + "=" + tagValue
	for i := uint32(0); i < bf.numHash; i++ {
		pos := bf.hash(item, i) % bf.numBits
		bf.bits[pos/64] |= 1 << (pos % 64)
	}
	bf.count++
}

// MayContain checks if a tag key-value pair might exist in the partition.
// Returns false if definitely not present, true if possibly present.
func (bf *PartitionBloomFilter) MayContain(tagKey, tagValue string) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	item := tagKey + "=" + tagValue
	for i := uint32(0); i < bf.numHash; i++ {
		pos := bf.hash(item, i) % bf.numBits
		if bf.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

// MayContainKey checks if any value for the given tag key might exist.
func (bf *PartitionBloomFilter) MayContainKey(tagKey string) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	// Check if the key marker was added
	item := tagKey + "=*"
	for i := uint32(0); i < bf.numHash; i++ {
		pos := bf.hash(item, i) % bf.numBits
		if bf.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

// AddKey adds a tag key marker to the bloom filter.
func (bf *PartitionBloomFilter) AddKey(tagKey string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	item := tagKey + "=*"
	for i := uint32(0); i < bf.numHash; i++ {
		pos := bf.hash(item, i) % bf.numBits
		bf.bits[pos/64] |= 1 << (pos % 64)
	}
}

// Count returns the number of items added.
func (bf *PartitionBloomFilter) Count() uint32 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.count
}

// FalsePositiveRate returns the estimated current false positive rate.
func (bf *PartitionBloomFilter) FalsePositiveRate() float64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	if bf.count == 0 {
		return 0
	}
	// p = (1 - e^(-kn/m))^k
	return math.Pow(1-math.Exp(-float64(bf.numHash)*float64(bf.count)/float64(bf.numBits)), float64(bf.numHash))
}

func (bf *PartitionBloomFilter) hash(item string, seed uint32) uint32 {
	h := fnv.New32a()
	h.Write([]byte{byte(seed), byte(seed >> 8)})
	h.Write([]byte(item))
	return h.Sum32()
}

// --- Partition Pruning Integration ---

// PartitionPruneResult holds the result of bloom-filter-based partition pruning.
type PartitionPruneResult struct {
	ScannedPartitions   int `json:"scanned_partitions"`
	PrunedPartitions    int `json:"pruned_partitions"`
	RemainingPartitions int `json:"remaining_partitions"`
}

// PrunePartitionsWithBloom filters partitions using bloom filters for tag predicates.
func PrunePartitionsWithBloom(partitions []*Partition, blooms map[uint64]*PartitionBloomFilter, tagFilters map[string]string) ([]*Partition, PartitionPruneResult) {
	result := PartitionPruneResult{
		ScannedPartitions: len(partitions),
	}

	if len(tagFilters) == 0 || len(blooms) == 0 {
		result.RemainingPartitions = len(partitions)
		return partitions, result
	}

	var remaining []*Partition
	for _, p := range partitions {
		bf, hasBF := blooms[p.id]
		if !hasBF {
			remaining = append(remaining, p)
			continue
		}

		pruned := false
		for key, value := range tagFilters {
			if !bf.MayContain(key, value) {
				pruned = true
				break
			}
		}

		if pruned {
			result.PrunedPartitions++
		} else {
			remaining = append(remaining, p)
		}
	}

	result.RemainingPartitions = len(remaining)
	return remaining, result
}

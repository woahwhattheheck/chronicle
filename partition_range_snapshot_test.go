package chronicle

import (
	"fmt"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
)

func TestRangePartitionPointsSnapshotCompatibility(t *testing.T) {
	bounds := []string{"z", "a", "m", "m"}
	rp := NewRangePartitioner("host", bounds)
	bounds[0] = "changed"
	points := []Point{
		{Metric: "nil", Value: 1},
		{Metric: "missing", Tags: map[string]string{"other": "x"}, Value: 2},
		{Metric: "low", Tags: map[string]string{"host": "a"}, Value: 3},
		{Metric: "equal", Tags: map[string]string{"host": "m"}, Value: 4},
		{Metric: "middle", Tags: map[string]string{"host": "n"}, Value: 5},
		{Metric: "high", Tags: map[string]string{"host": "zz"}, Value: 6},
	}
	original := append([]Point(nil), points...)
	want := [][]Point{points[:3], points[3:4], {}, points[4:5], points[5:]}
	if got := rp.PartitionPoints(points); !reflect.DeepEqual(got, want) {
		t.Fatalf("wrong buckets: got %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(points, original) {
		t.Fatal("input points changed")
	}
	for _, bucket := range rp.PartitionPoints(nil) {
		if bucket == nil || len(bucket) != 0 {
			t.Fatal("empty bucket contract changed")
		}
	}
	rp.SetBoundaries([]string{"zz"})
	if got := rp.PartitionPoints(points); len(got) != 2 || len(got[0]) != len(points) {
		t.Fatalf("next batch did not observe new boundaries: %#v", got)
	}
}

func TestRangePartitionPointsSnapshotConcurrentUpdate(t *testing.T) {
	rp := NewRangePartitioner("host", []string{"a"})
	points := make([]Point, 10000)
	for i := range points {
		points[i] = Point{Tags: map[string]string{"host": "m"}, Value: float64(i)}
	}
	stop, done, started := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var updates atomic.Uint64
	go func() {
		defer close(done)
		rp.SetBoundaries([]string{"z"})
		updates.Add(1)
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
				rp.SetBoundaries([]string{"a"})
				rp.SetBoundaries([]string{"z"})
				updates.Add(2)
				runtime.Gosched()
			}
		}
	}()
	<-started
	defer func() { close(stop); <-done }()
	for batch := 0; batch < 32; batch++ {
		got := rp.PartitionPoints(points)
		if len(got) != 2 || len(got[0])+len(got[1]) != len(points) {
			t.Fatal("point loss or bucket-count change")
		}
		if len(got[0]) != len(points) && len(got[1]) != len(points) {
			t.Fatalf("batch %d mixed boundary generations: %d/%d points", batch, len(got[0]), len(got[1]))
		}
	}
	t.Logf("concurrent boundary updates: %d", updates.Load())
}

var rangeSnapshotBenchmarkResult [][]Point

func BenchmarkRangePartitionPointsSnapshot(b *testing.B) {
	for _, size := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("points_%d", size), func(b *testing.B) {
			bounds := make([]string, 16)
			for i := range bounds {
				bounds[i] = fmt.Sprintf("host-%02d", i*4)
			}
			rp := NewRangePartitioner("host", bounds)
			points := make([]Point, size)
			for i := range points {
				points[i] = Point{Metric: "cpu", Tags: map[string]string{"host": fmt.Sprintf("host-%02d", i%64)}, Value: float64(i), Timestamp: int64(i)}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rangeSnapshotBenchmarkResult = rp.PartitionPoints(points)
			}
		})
	}
}

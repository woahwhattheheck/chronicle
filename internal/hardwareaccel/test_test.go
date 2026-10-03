package hardwareaccel

import (
	"encoding/json"
	"math"
	"runtime"
	"sync"
	"testing"

	chronicle "github.com/chronicle-db/chronicle"
)

func TestNewHardwareAccelSDK(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultHardwareAccelConfig()
	sdk, err := NewHardwareAccelSDK(db, config)
	if err != nil {
		t.Fatalf("NewHardwareAccelSDK() error = %v", err)
	}
	defer sdk.Close()

	devices := sdk.GetAvailableDevices()
	if len(devices) == 0 {
		t.Error("Expected at least CPU device")
	}

	// Should always have CPU
	hasCPU := false
	for _, d := range devices {
		if d.Type() == AcceleratorCPU {
			hasCPU = true
			break
		}
	}
	if !hasCPU {
		t.Error("CPU device should always be available")
	}
}

func TestDefaultHardwareAccelConfig(t *testing.T) {
	config := DefaultHardwareAccelConfig()

	if !config.EnableGPU {
		t.Error("EnableGPU should default to true")
	}
	if !config.EnableSIMD {
		t.Error("EnableSIMD should default to true")
	}
	if !config.FallbackToCPU {
		t.Error("FallbackToCPU should default to true")
	}
	if config.GPUBatchSize <= 0 {
		t.Error("GPUBatchSize should be positive")
	}
}

func TestAcceleratorTypeString(t *testing.T) {
	tests := []struct {
		t        AcceleratorType
		expected string
	}{
		{AcceleratorCPU, "CPU"},
		{AcceleratorSIMD, "SIMD"},
		{AcceleratorGPU, "GPU"},
		{AcceleratorFPGA, "FPGA"},
		{AcceleratorType(99), "Unknown"},
	}

	for _, tt := range tests {
		result := tt.t.String()
		if result != tt.expected {
			t.Errorf("AcceleratorType(%d).String() = %s, want %s", tt.t, result, tt.expected)
		}
	}
}

func TestSelectDevice(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sdk, _ := NewHardwareAccelSDK(db, nil)
	defer sdk.Close()

	// Should return a device for compression
	device := sdk.SelectDevice(CapabilityCompression)
	if device == nil {
		t.Error("Should return a device")
	}

	// CPU should support all basic capabilities
	cpuDevice := sdk.SelectDevice(CapabilityAggregation)
	if cpuDevice == nil {
		t.Error("Should return a device for aggregation")
	}
}

func TestCompressCPU(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultHardwareAccelConfig()
	config.EnableGPU = false
	config.EnableSIMD = false

	sdk, _ := NewHardwareAccelSDK(db, config)
	defer sdk.Close()

	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	compressed, err := sdk.CompressAccelerated(data)
	if err != nil {
		t.Fatalf("CompressAccelerated() error = %v", err)
	}
	if len(compressed) == 0 {
		t.Error("Expected compressed data")
	}
}

func TestDecompressCPU(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultHardwareAccelConfig()
	config.EnableGPU = false
	config.EnableSIMD = false

	sdk, _ := NewHardwareAccelSDK(db, config)
	defer sdk.Close()

	original := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
	compressed, _ := sdk.CompressAccelerated(original)

	decompressed, err := sdk.DecompressAccelerated(compressed, len(original))
	if err != nil {
		t.Fatalf("DecompressAccelerated() error = %v", err)
	}

	for i := range original {
		if math.Abs(decompressed[i]-original[i]) > 1e-10 {
			t.Errorf("Decompressed[%d] = %v, want %v", i, decompressed[i], original[i])
		}
	}
}

func TestAggregateCPU(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultHardwareAccelConfig()
	config.EnableGPU = false
	config.AggregationAcceleration = true

	sdk, _ := NewHardwareAccelSDK(db, config)
	defer sdk.Close()

	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}

	// Test sum
	sum, err := sdk.AggregateAccelerated(AggregateSum, data)
	if err != nil {
		t.Fatalf("AggregateSum error = %v", err)
	}
	if sum != 15.0 {
		t.Errorf("Sum = %v, want 15.0", sum)
	}

	// Test min
	min, err := sdk.AggregateAccelerated(AggregateMin, data)
	if err != nil {
		t.Fatalf("AggregateMin error = %v", err)
	}
	if min != 1.0 {
		t.Errorf("Min = %v, want 1.0", min)
	}

	// Test max
	max, err := sdk.AggregateAccelerated(AggregateMax, data)
	if err != nil {
		t.Fatalf("AggregateMax error = %v", err)
	}
	if max != 5.0 {
		t.Errorf("Max = %v, want 5.0", max)
	}

	// Test mean
	mean, err := sdk.AggregateAccelerated(AggregateMean, data)
	if err != nil {
		t.Fatalf("AggregateMean error = %v", err)
	}
	if mean != 3.0 {
		t.Errorf("Mean = %v, want 3.0", mean)
	}

	// Test count
	count, err := sdk.AggregateAccelerated(AggregateCount, data)
	if err != nil {
		t.Fatalf("AggregateCount error = %v", err)
	}
	if count != 5.0 {
		t.Errorf("Count = %v, want 5.0", count)
	}
}

func TestAggregateStdDev(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sdk, _ := NewHardwareAccelSDK(db, nil)
	defer sdk.Close()

	data := []float64{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0}

	stddev, err := sdk.AggregateAccelerated(AggregateStdDev, data)
	if err != nil {
		t.Fatalf("AggregateStdDev error = %v", err)
	}
	if math.Abs(stddev-2.0) > 0.001 {
		t.Errorf("StdDev = %v, want ~2.0", stddev)
	}
}

func TestAggregateVariance(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sdk, _ := NewHardwareAccelSDK(db, nil)
	defer sdk.Close()

	data := []float64{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0}

	variance, err := sdk.AggregateAccelerated(AggregateVariance, data)
	if err != nil {
		t.Fatalf("AggregateVariance error = %v", err)
	}
	if math.Abs(variance-4.0) > 0.001 {
		t.Errorf("Variance = %v, want ~4.0", variance)
	}
}

func TestFilterAccelerated(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sdk, _ := NewHardwareAccelSDK(db, nil)
	defer sdk.Close()

	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	predicate := func(v float64) bool { return v > 5.0 }

	filtered, err := sdk.FilterAccelerated(data, predicate)
	if err != nil {
		t.Fatalf("FilterAccelerated error = %v", err)
	}

	if len(filtered) != 5 {
		t.Errorf("Filtered length = %d, want 5", len(filtered))
	}

	for _, v := range filtered {
		if v <= 5.0 {
			t.Errorf("Filtered value %v should be > 5.0", v)
		}
	}
}

func TestSortAccelerated(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sdk, _ := NewHardwareAccelSDK(db, nil)
	defer sdk.Close()

	data := []float64{5.0, 2.0, 8.0, 1.0, 9.0, 3.0, 7.0, 4.0, 6.0, 10.0}
	err = sdk.SortAccelerated(data)
	if err != nil {
		t.Fatalf("SortAccelerated error = %v", err)
	}

	for i := 1; i < len(data); i++ {
		if data[i] < data[i-1] {
			t.Errorf("Data not sorted at index %d: %v < %v", i, data[i], data[i-1])
		}
	}
}

func TestVectorMathCPU(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultHardwareAccelConfig()
	config.EnableGPU = false
	config.EnableSIMD = false

	sdk, _ := NewHardwareAccelSDK(db, config)
	defer sdk.Close()

	a := []float64{1.0, 2.0, 3.0, 4.0}
	b := []float64{5.0, 6.0, 7.0, 8.0}

	// Test add
	result, err := sdk.VectorMathAccelerated(VectorAdd, a, b)
	if err != nil {
		t.Fatalf("VectorAdd error = %v", err)
	}
	expected := []float64{6.0, 8.0, 10.0, 12.0}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("Add[%d] = %v, want %v", i, result[i], expected[i])
		}
	}

	// Test sub
	result, _ = sdk.VectorMathAccelerated(VectorSub, a, b)
	expected = []float64{-4.0, -4.0, -4.0, -4.0}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("Sub[%d] = %v, want %v", i, result[i], expected[i])
		}
	}

	// Test mul
	result, _ = sdk.VectorMathAccelerated(VectorMul, a, b)
	expected = []float64{5.0, 12.0, 21.0, 32.0}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("Mul[%d] = %v, want %v", i, result[i], expected[i])
		}
	}

	// Test dot
	result, _ = sdk.VectorMathAccelerated(VectorDot, a, b)
	if result[0] != 70.0 {
		t.Errorf("Dot = %v, want 70.0", result[0])
	}
}

func TestVectorMathLengthMismatch(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sdk, _ := NewHardwareAccelSDK(db, nil)
	defer sdk.Close()

	a := []float64{1.0, 2.0, 3.0}
	b := []float64{4.0, 5.0}

	_, err = sdk.VectorMathAccelerated(VectorAdd, a, b)
	if err == nil {
		t.Error("Expected error for length mismatch")
	}
}

func TestVectorDiv(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultHardwareAccelConfig()
	config.EnableGPU = false
	config.EnableSIMD = false

	sdk, _ := NewHardwareAccelSDK(db, config)
	defer sdk.Close()

	a := []float64{10.0, 20.0, 30.0}
	b := []float64{2.0, 4.0, 5.0}

	result, err := sdk.VectorMathAccelerated(VectorDiv, a, b)
	if err != nil {
		t.Fatalf("VectorDiv error = %v", err)
	}

	expected := []float64{5.0, 5.0, 6.0}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("Div[%d] = %v, want %v", i, result[i], expected[i])
		}
	}
}

func TestVectorDivByZero(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultHardwareAccelConfig()
	config.EnableGPU = false
	config.EnableSIMD = false

	sdk, _ := NewHardwareAccelSDK(db, config)
	defer sdk.Close()

	a := []float64{10.0, 20.0}
	b := []float64{2.0, 0.0}

	result, _ := sdk.VectorMathAccelerated(VectorDiv, a, b)
	if !math.IsNaN(result[1]) {
		t.Errorf("Div by zero should be NaN, got %v", result[1])
	}
}

func TestFFTCPU(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sdk, _ := NewHardwareAccelSDK(db, nil)
	defer sdk.Close()

	// Simple test data
	data := make([]float64, 8)
	for i := range data {
		data[i] = float64(i)
	}

	result, err := sdk.FFTAccelerated(data)
	if err != nil {
		t.Fatalf("FFTAccelerated error = %v", err)
	}

	if len(result) != len(data) {
		t.Errorf("FFT result length = %d, want %d", len(result), len(data))
	}
}

func TestAggregateSIMD(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultHardwareAccelConfig()
	config.EnableGPU = false
	config.EnableSIMD = true

	sdk, _ := NewHardwareAccelSDK(db, config)
	defer sdk.Close()

	// Create large dataset for SIMD processing
	data := make([]float64, 1000)
	expectedSum := 0.0
	for i := range data {
		data[i] = float64(i)
		expectedSum += float64(i)
	}

	sum, err := sdk.aggregateSIMD(AggregateSum, data)
	if err != nil {
		t.Fatalf("SIMD sum error = %v", err)
	}
	if math.Abs(sum-expectedSum) > 0.001 {
		t.Errorf("SIMD sum = %v, want %v", sum, expectedSum)
	}

	// Test min
	min, _ := sdk.aggregateSIMD(AggregateMin, data)
	if min != 0.0 {
		t.Errorf("SIMD min = %v, want 0.0", min)
	}

	// Test max
	max, _ := sdk.aggregateSIMD(AggregateMax, data)
	if max != 999.0 {
		t.Errorf("SIMD max = %v, want 999.0", max)
	}
}

func TestFilterSIMD(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultHardwareAccelConfig()
	config.EnableGPU = false
	config.EnableSIMD = true
	config.QueryAcceleration = true

	sdk, _ := NewHardwareAccelSDK(db, config)
	defer sdk.Close()

	data := make([]float64, 100)
	for i := range data {
		data[i] = float64(i)
	}

	filtered, err := sdk.filterSIMD(data, func(v float64) bool { return v >= 50 })
	if err != nil {
		t.Fatalf("SIMD filter error = %v", err)
	}

	if len(filtered) != 50 {
		t.Errorf("Filtered length = %d, want 50", len(filtered))
	}
}

func TestVectorMathSIMD(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultHardwareAccelConfig()
	config.EnableGPU = false
	config.EnableSIMD = true

	sdk, _ := NewHardwareAccelSDK(db, config)
	defer sdk.Close()

	a := make([]float64, 100)
	b := make([]float64, 100)
	for i := range a {
		a[i] = float64(i)
		b[i] = float64(i * 2)
	}

	result, err := sdk.vectorMathSIMD(VectorAdd, a, b)
	if err != nil {
		t.Fatalf("SIMD vector add error = %v", err)
	}

	for i := range result {
		expected := float64(i) + float64(i*2)
		if result[i] != expected {
			t.Errorf("Add[%d] = %v, want %v", i, result[i], expected)
		}
	}
}

func TestCPUDevice(t *testing.T) {
	device := &CPUDevice{}

	if device.Type() != AcceleratorCPU {
		t.Errorf("Type = %v, want CPU", device.Type())
	}
	if device.Name() != "CPU" {
		t.Errorf("Name = %s, want CPU", device.Name())
	}
	if device.Capabilities()&CapabilityCompression == 0 {
		t.Error("CPU should support compression")
	}

	buf, err := device.Allocate(1024)
	if err != nil {
		t.Fatalf("Allocate error = %v", err)
	}
	defer buf.Free()

	if buf.Size() != 1024 {
		t.Errorf("Buffer size = %d, want 1024", buf.Size())
	}

	data := []byte{1, 2, 3, 4, 5}
	if err := buf.CopyToDevice(data); err != nil {
		t.Errorf("CopyToDevice error = %v", err)
	}

	host, err := buf.CopyToHost()
	if err != nil {
		t.Errorf("CopyToHost error = %v", err)
	}
	for i := range data {
		if host[i] != data[i] {
			t.Errorf("Host[%d] = %d, want %d", i, host[i], data[i])
		}
	}
}

func TestSIMDDevice(t *testing.T) {
	device := &SIMDDevice{alignment: 32}

	if device.Type() != AcceleratorSIMD {
		t.Errorf("Type = %v, want SIMD", device.Type())
	}
	if device.Capabilities()&CapabilityVectorMath == 0 {
		t.Error("SIMD should support vector math")
	}

	buf, _ := device.Allocate(100)
	// Should be aligned
	if buf.Size() < 100 {
		t.Errorf("Buffer size = %d, should be >= 100", buf.Size())
	}
	buf.Free()
}

func TestGPUDevice(t *testing.T) {
	device, err := NewGPUDevice(-1)
	if err != nil {
		t.Fatalf("NewGPUDevice error = %v", err)
	}
	defer device.Close()

	if device.Type() != AcceleratorGPU {
		t.Errorf("Type = %v, want GPU", device.Type())
	}
	if device.MemoryTotal() == 0 {
		t.Error("GPU should report memory")
	}

	// Allocate buffer
	buf, err := device.Allocate(1024)
	if err != nil {
		t.Fatalf("Allocate error = %v", err)
	}

	available := device.MemoryAvailable()
	if available >= device.MemoryTotal() {
		t.Error("Memory should decrease after allocation")
	}

	buf.Free()

	// Memory should be released
	if device.MemoryAvailable() != device.MemoryTotal() {
		t.Error("Memory should be released after free")
	}
}

func TestGPUDeviceOutOfMemory(t *testing.T) {
	device, _ := NewGPUDevice(-1)
	defer device.Close()

	// Try to allocate more than total memory
	_, err := device.Allocate(device.MemoryTotal() + 1)
	if err == nil {
		t.Error("Should fail when out of memory")
	}
}

func TestFPGADevice(t *testing.T) {
	device, err := NewFPGADevice("/dev/fpga0")
	if err != nil {
		t.Fatalf("NewFPGADevice error = %v", err)
	}
	defer device.Close()

	if device.Type() != AcceleratorFPGA {
		t.Errorf("Type = %v, want FPGA", device.Type())
	}

	buf, _ := device.Allocate(512)
	buf.Free()
}

func TestAcceleratorMemoryPool(t *testing.T) {
	device := &CPUDevice{}
	pool := NewAcceleratorMemoryPool(device, 1<<20) // 1MB

	buf1, err := pool.Allocate(1024)
	if err != nil {
		t.Fatalf("Allocate error = %v", err)
	}

	buf2, err := pool.Allocate(1024)
	if err != nil {
		t.Fatalf("Second allocate error = %v", err)
	}

	// Free first buffer
	buf1.Free()

	// Allocate again - should reuse
	buf3, err := pool.Allocate(512)
	if err != nil {
		t.Fatalf("Third allocate error = %v", err)
	}

	buf2.Free()
	buf3.Free()
}

func TestAcceleratorMemoryPoolExhausted(t *testing.T) {
	device := &CPUDevice{}
	pool := NewAcceleratorMemoryPool(device, 1024)

	buf1, _ := pool.Allocate(512)
	buf2, _ := pool.Allocate(512)

	// Pool is full
	_, err := pool.Allocate(512)
	if err == nil {
		t.Error("Should fail when pool is exhausted")
	}

	buf1.Free()
	buf2.Free()
}

func TestBatchProcessor(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sdk, _ := NewHardwareAccelSDK(db, nil)
	defer sdk.Close()

	processor := NewBatchProcessor(sdk, 100)

	points := make([]chronicle.Point, 10)
	for i := range points {
		points[i] = chronicle.Point{
			Metric:    "test",
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}
	}

	result, err := processor.ProcessBatch(points)
	if err != nil {
		t.Fatalf("ProcessBatch error = %v", err)
	}
	if len(result) != len(points) {
		t.Errorf("Result length = %d, want %d", len(result), len(points))
	}
}

func TestAsyncCompressAccelerated(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sdk, _ := NewHardwareAccelSDK(db, nil)
	defer sdk.Close()

	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	op := sdk.AsyncCompressAccelerated(data)

	result, err := op.Wait()
	if err != nil {
		t.Fatalf("AsyncCompress error = %v", err)
	}
	if result == nil {
		t.Error("Expected result")
	}
}

func TestAsyncAggregateAccelerated(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sdk, _ := NewHardwareAccelSDK(db, nil)
	defer sdk.Close()

	data := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	op := sdk.AsyncAggregateAccelerated(AggregateSum, data)

	result, err := op.Wait()
	if err != nil {
		t.Fatalf("AsyncAggregate error = %v", err)
	}

	sum, ok := result.(float64)
	if !ok {
		t.Fatalf("Result type = %T, want float64", result)
	}
	if sum != 15.0 {
		t.Errorf("Sum = %v, want 15.0", sum)
	}
}

func TestHAStats(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sdk, _ := NewHardwareAccelSDK(db, nil)
	defer sdk.Close()

	// Perform some operations
	data := []float64{1.0, 2.0, 3.0}
	sdk.CompressAccelerated(data)
	sdk.AggregateAccelerated(AggregateSum, data)

	stats := sdk.Stats()

	// Should have some operations counted
	totalOps := stats.GPUOperations + stats.FPGAOperations + stats.SIMDOperations + stats.CPUFallbacks
	if totalOps == 0 {
		t.Log("Expected some operations to be counted")
	}
}

func TestHAStatsConcurrentOperations(t *testing.T) {
	const workers, iterations = 8, 16
	const calls = workers * iterations
	data := []float64{1, 2, 3, 4}
	const bytesPerCall = 4 * 8

	tests := []struct {
		name           string
		accelerator    AcceleratorType
		failAllocation bool
		want           HardwareAccelStats
	}{
		{
			name:        "GPU",
			accelerator: AcceleratorGPU,
			want: HardwareAccelStats{
				GPUOperations:     3 * calls,
				GPUBytesProcessed: 2 * calls * bytesPerCall,
			},
		},
		{
			name:        "FPGA",
			accelerator: AcceleratorFPGA,
			want: HardwareAccelStats{
				FPGAOperations:     calls,
				FPGABytesProcessed: calls * bytesPerCall,
			},
		},
		{
			name:        "SIMD",
			accelerator: AcceleratorSIMD,
			want: HardwareAccelStats{
				SIMDOperations:     3 * calls,
				SIMDBytesProcessed: 2 * calls * bytesPerCall,
			},
		},
		{
			name:           "GPU CPU fallback",
			accelerator:    AcceleratorGPU,
			failAllocation: true,
			want: HardwareAccelStats{
				GPUErrors:    calls,
				CPUFallbacks: 3 * calls,
			},
		},
		{
			name:           "FPGA CPU fallback",
			accelerator:    AcceleratorFPGA,
			failAllocation: true,
			want: HardwareAccelStats{
				FPGAErrors:   calls,
				CPUFallbacks: calls,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultHardwareAccelConfig()
			config.EnableGPU = tt.accelerator == AcceleratorGPU
			config.EnableFPGA = tt.accelerator == AcceleratorFPGA
			config.EnableSIMD = tt.accelerator == AcceleratorSIMD
			config.GPUBatchSize = 1
			sdk, err := NewHardwareAccelSDK(nil, config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { sdk.Close() })

			if tt.failAllocation {
				switch device := sdk.devices[tt.accelerator].(type) {
				case *GPUDevice:
					device.memoryTotal = 0
				case *FPGADevice:
					device.memoryTotal = 0
				}
			}

			initial := sdk.Stats()
			stopReader := make(chan struct{})
			readerDone := make(chan struct{})
			go func() {
				defer close(readerDone)
				for {
					_ = sdk.Stats()
					select {
					case <-stopReader:
						return
					default:
						runtime.Gosched()
					}
				}
			}()

			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for j := 0; j < iterations; j++ {
						compressed, err := sdk.CompressAccelerated(data)
						if err != nil {
							t.Errorf("CompressAccelerated: %v", err)
							return
						}
						if tt.accelerator == AcceleratorFPGA {
							continue
						}
						if _, err := sdk.DecompressAccelerated(compressed, len(data)); err != nil {
							t.Errorf("DecompressAccelerated: %v", err)
							return
						}
						if _, err := sdk.AggregateAccelerated(AggregateSum, data); err != nil {
							t.Errorf("AggregateAccelerated: %v", err)
							return
						}
					}
				}()
			}
			close(start)
			wg.Wait()
			close(stopReader)
			<-readerDone

			got := sdk.Stats()
			if got.TotalGPUTimeNs < 0 || got.TotalFPGATimeNs < 0 || got.TotalSIMDTimeNs < 0 {
				t.Errorf("negative operation duration: %+v", got)
			}
			// Durations vary; all operation, byte, error and fallback counts are exact.
			got.TotalGPUTimeNs, got.TotalFPGATimeNs, got.TotalSIMDTimeNs = 0, 0, 0
			if got != tt.want {
				t.Errorf("Stats() = %+v, want %+v", got, tt.want)
			}
			if initial != (HardwareAccelStats{}) {
				t.Errorf("initial snapshot changed: %+v", initial)
			}
			got.CPUFallbacks++
			if sdk.Stats().CPUFallbacks != tt.want.CPUFallbacks {
				t.Error("editing a snapshot changed the live counters")
			}

			encoded, err := json.Marshal(sdk.Stats())
			if err != nil {
				t.Fatal(err)
			}
			var decoded map[string]int64
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatalf("snapshot JSON must retain numeric fields: %v", err)
			}
			if len(decoded) != 14 || decoded["CPUFallbacks"] != tt.want.CPUFallbacks {
				t.Errorf("unexpected snapshot JSON: %s", encoded)
			}
		})
	}
}

func TestCompressEmptyData(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sdk, _ := NewHardwareAccelSDK(db, nil)
	defer sdk.Close()

	result, err := sdk.CompressAccelerated([]float64{})
	if err != nil {
		t.Fatalf("CompressAccelerated error = %v", err)
	}
	if result != nil && len(result) > 0 {
		t.Error("Empty data should produce empty or nil result")
	}
}

func TestAggregateEmptyData(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	sdk, _ := NewHardwareAccelSDK(db, nil)
	defer sdk.Close()

	result, err := sdk.AggregateAccelerated(AggregateSum, []float64{})
	if err != nil {
		t.Fatalf("AggregateAccelerated error = %v", err)
	}
	if result != 0 {
		t.Errorf("Empty sum = %v, want 0", result)
	}
}

func TestCompressionKernel(t *testing.T) {
	kernel := &CompressionKernel{}

	if kernel.Name() != "compression" {
		t.Errorf("Name = %s, want compression", kernel.Name())
	}

	err := kernel.Compile(AcceleratorGPU)
	if err != nil {
		t.Errorf("Compile error = %v", err)
	}
}

func TestAggregationKernel(t *testing.T) {
	kernel := &AggregationKernel{}

	if kernel.Name() != "aggregation" {
		t.Errorf("Name = %s, want aggregation", kernel.Name())
	}
}

func TestFilterKernel(t *testing.T) {
	kernel := &FilterKernel{}

	if kernel.Name() != "filter" {
		t.Errorf("Name = %s, want filter", kernel.Name())
	}
}

func TestQuicksort(t *testing.T) {
	data := []float64{5.0, 2.0, 8.0, 1.0, 9.0}
	quicksort(data, 0, len(data)-1)

	for i := 1; i < len(data); i++ {
		if data[i] < data[i-1] {
			t.Errorf("Not sorted at %d: %v", i, data)
		}
	}
}

func TestBitReverse(t *testing.T) {
	// bitReverse(1, 3) should be 4 (001 -> 100)
	result := bitReverse(1, 3)
	if result != 4 {
		t.Errorf("bitReverse(1, 3) = %d, want 4", result)
	}

	// bitReverse(0, 3) should be 0
	result = bitReverse(0, 3)
	if result != 0 {
		t.Errorf("bitReverse(0, 3) = %d, want 0", result)
	}
}

func TestIntLog2(t *testing.T) {
	tests := []struct {
		n        int
		expected int
	}{
		{1, 0},
		{2, 1},
		{4, 2},
		{8, 3},
		{16, 4},
	}

	for _, tt := range tests {
		result := intLog2(tt.n)
		if result != tt.expected {
			t.Errorf("intLog2(%d) = %d, want %d", tt.n, result, tt.expected)
		}
	}
}

func TestHasSIMDSupport(t *testing.T) {
	// Should return true on most modern systems
	result := hasSIMDSupport()
	t.Logf("hasSIMDSupport() = %v", result)
}

func TestFallbackToCPU(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultHardwareAccelConfig()
	config.EnableGPU = true
	config.FallbackToCPU = true

	sdk, _ := NewHardwareAccelSDK(db, config)
	defer sdk.Close()

	// Operations should still work even if GPU fails internally
	data := []float64{1.0, 2.0, 3.0}
	_, err = sdk.CompressAccelerated(data)
	if err != nil {
		t.Errorf("Should fallback to CPU, but got error: %v", err)
	}
}

func TestDisableAcceleration(t *testing.T) {
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	config := DefaultHardwareAccelConfig()
	config.CompressionAcceleration = false
	config.AggregationAcceleration = false
	config.QueryAcceleration = false

	sdk, _ := NewHardwareAccelSDK(db, config)
	defer sdk.Close()

	data := []float64{1.0, 2.0, 3.0}

	// Should still work via CPU path
	_, err = sdk.CompressAccelerated(data)
	if err != nil {
		t.Errorf("CompressAccelerated error = %v", err)
	}

	_, err = sdk.AggregateAccelerated(AggregateSum, data)
	if err != nil {
		t.Errorf("AggregateAccelerated error = %v", err)
	}

	_, err = sdk.FilterAccelerated(data, func(v float64) bool { return v > 1 })
	if err != nil {
		t.Errorf("FilterAccelerated error = %v", err)
	}
}

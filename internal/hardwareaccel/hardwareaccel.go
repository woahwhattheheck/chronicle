package hardwareaccel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

// HardwareAccelConfig configures the hardware acceleration SDK.
type HardwareAccelConfig struct {
	// Enable GPU acceleration
	EnableGPU bool

	// Enable FPGA acceleration
	EnableFPGA bool

	// Enable SIMD optimizations
	EnableSIMD bool

	// GPU device index (-1 for auto)
	GPUDeviceIndex int

	// FPGA device path
	FPGADevicePath string

	// Memory pool size for accelerators (bytes)
	AcceleratorMemoryPool int64

	// Batch size for GPU operations
	GPUBatchSize int

	// Enable async operations
	EnableAsync bool

	// Compression acceleration
	CompressionAcceleration bool

	// Aggregation acceleration
	AggregationAcceleration bool

	// Query acceleration
	QueryAcceleration bool

	// Fallback to CPU on errors
	FallbackToCPU bool

	// Memory alignment for SIMD (bytes)
	SIMDAlignment int
}

// DefaultHardwareAccelConfig returns default configuration.
func DefaultHardwareAccelConfig() *HardwareAccelConfig {
	return &HardwareAccelConfig{
		EnableGPU:               true,
		EnableFPGA:              false,
		EnableSIMD:              true,
		GPUDeviceIndex:          -1,
		FPGADevicePath:          "/dev/fpga0",
		AcceleratorMemoryPool:   1 << 30, // 1GB
		GPUBatchSize:            10000,
		EnableAsync:             true,
		CompressionAcceleration: true,
		AggregationAcceleration: true,
		QueryAcceleration:       true,
		FallbackToCPU:           true,
		SIMDAlignment:           32,
	}
}

// AcceleratorType represents a type of hardware accelerator.
type AcceleratorType int

const (
	AcceleratorCPU AcceleratorType = iota
	AcceleratorSIMD
	AcceleratorGPU
	AcceleratorFPGA
)

func (t AcceleratorType) String() string {
	switch t {
	case AcceleratorCPU:
		return "CPU"
	case AcceleratorSIMD:
		return "SIMD"
	case AcceleratorGPU:
		return "GPU"
	case AcceleratorFPGA:
		return "FPGA"
	default:
		return "Unknown"
	}
}

// AcceleratorCapability represents a capability of an accelerator.
type AcceleratorCapability int

const (
	CapabilityCompression AcceleratorCapability = 1 << iota
	CapabilityDecompression
	CapabilityAggregation
	CapabilityFiltering
	CapabilitySort
	CapabilityJoin
	CapabilityVectorMath
	CapabilityFFT
)

// AcceleratorDevice represents a hardware accelerator device.
type AcceleratorDevice interface {
	// Type returns the accelerator type
	Type() AcceleratorType

	// Name returns device name
	Name() string

	// Capabilities returns supported capabilities
	Capabilities() AcceleratorCapability

	// MemoryTotal returns total memory in bytes
	MemoryTotal() int64

	// MemoryAvailable returns available memory in bytes
	MemoryAvailable() int64

	// Allocate allocates device memory
	Allocate(size int64) (DeviceBuffer, error)

	// Execute runs a kernel on the device
	Execute(kernel AcceleratorKernel, args ...any) error

	// Sync waits for all pending operations
	Sync() error

	// Close releases device resources
	Close() error
}

// DeviceBuffer represents memory allocated on an accelerator.
type DeviceBuffer interface {
	// Size returns buffer size in bytes
	Size() int64

	// CopyToDevice copies data from host to device
	CopyToDevice(data []byte) error

	// CopyToHost copies data from device to host
	CopyToHost() ([]byte, error)

	// Free releases the buffer
	Free() error
}

// AcceleratorKernel represents a computational kernel for accelerators.
type AcceleratorKernel interface {
	// Name returns kernel name
	Name() string

	// Compile compiles the kernel for a device type
	Compile(deviceType AcceleratorType) error
}

// HardwareAccelSDK provides hardware acceleration for time-series operations.
type HardwareAccelSDK struct {
	db     *chronicle.DB
	config *HardwareAccelConfig

	mu      sync.RWMutex
	devices map[AcceleratorType]AcceleratorDevice

	// Memory pools
	gpuPool  *AcceleratorMemoryPool
	fpgaPool *AcceleratorMemoryPool

	// Kernels
	compressionKernel AcceleratorKernel
	aggregationKernel AcceleratorKernel
	filterKernel      AcceleratorKernel

	// Stats
	stats hardwareAccelCounters

	// Async operation tracking
	pendingOps sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

// hardwareAccelCounters keeps live counters aligned on 32-bit platforms.
// Stats loads them into the copyable HardwareAccelStats snapshot.
type hardwareAccelCounters struct {
	gpuOperations      atomic.Int64
	fpgaOperations     atomic.Int64
	simdOperations     atomic.Int64
	cpuFallbacks       atomic.Int64
	gpuBytesProcessed  atomic.Int64
	fpgaBytesProcessed atomic.Int64
	simdBytesProcessed atomic.Int64
	totalGPUTimeNs     atomic.Int64
	totalFPGATimeNs    atomic.Int64
	totalSIMDTimeNs    atomic.Int64
	gpuMemoryUsed      atomic.Int64
	fpgaMemoryUsed     atomic.Int64
	gpuErrors          atomic.Int64
	fpgaErrors         atomic.Int64
}

// HardwareAccelStats contains acceleration statistics.
type HardwareAccelStats struct {
	// Operations accelerated
	GPUOperations  int64
	FPGAOperations int64
	SIMDOperations int64
	CPUFallbacks   int64

	// Data processed (bytes)
	GPUBytesProcessed  int64
	FPGABytesProcessed int64
	SIMDBytesProcessed int64

	// Timing (nanoseconds)
	TotalGPUTimeNs  int64
	TotalFPGATimeNs int64
	TotalSIMDTimeNs int64

	// Memory usage
	GPUMemoryUsed  int64
	FPGAMemoryUsed int64

	// Errors
	GPUErrors  int64
	FPGAErrors int64
}

// NewHardwareAccelSDK creates a new hardware acceleration SDK.
func NewHardwareAccelSDK(db *chronicle.DB, config *HardwareAccelConfig) (*HardwareAccelSDK, error) {
	if config == nil {
		config = DefaultHardwareAccelConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	sdk := &HardwareAccelSDK{
		db:      db,
		config:  config,
		devices: make(map[AcceleratorType]AcceleratorDevice),
		ctx:     ctx,
		cancel:  cancel,
	}

	// Initialize devices
	if err := sdk.initDevices(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize devices: %w", err)
	}

	// Initialize memory pools
	if err := sdk.initMemoryPools(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize memory pools: %w", err)
	}

	// Initialize kernels
	sdk.initKernels()

	return sdk, nil
}

// initDevices initializes available hardware devices.
func (sdk *HardwareAccelSDK) initDevices() error {
	// Always add CPU device
	sdk.devices[AcceleratorCPU] = &CPUDevice{}

	// Add SIMD device if enabled
	if sdk.config.EnableSIMD && hasSIMDSupport() {
		sdk.devices[AcceleratorSIMD] = &SIMDDevice{
			alignment: sdk.config.SIMDAlignment,
		}
	}

	// Add GPU device if enabled
	if sdk.config.EnableGPU {
		gpu, err := NewGPUDevice(sdk.config.GPUDeviceIndex)
		if err != nil {
			if !sdk.config.FallbackToCPU {
				return fmt.Errorf("GPU initialization failed: %w", err)
			}
			// Log warning but continue
		} else {
			sdk.devices[AcceleratorGPU] = gpu
		}
	}

	// Add FPGA device if enabled
	if sdk.config.EnableFPGA {
		fpga, err := NewFPGADevice(sdk.config.FPGADevicePath)
		if err != nil {
			if !sdk.config.FallbackToCPU {
				return fmt.Errorf("FPGA initialization failed: %w", err)
			}
		} else {
			sdk.devices[AcceleratorFPGA] = fpga
		}
	}

	return nil
}

// initMemoryPools initializes memory pools for accelerators.
func (sdk *HardwareAccelSDK) initMemoryPools() error {
	poolSize := sdk.config.AcceleratorMemoryPool

	if gpu, ok := sdk.devices[AcceleratorGPU]; ok {
		sdk.gpuPool = NewAcceleratorMemoryPool(gpu, poolSize/2)
	}

	if fpga, ok := sdk.devices[AcceleratorFPGA]; ok {
		sdk.fpgaPool = NewAcceleratorMemoryPool(fpga, poolSize/2)
	}

	return nil
}

// initKernels initializes computational kernels.
func (sdk *HardwareAccelSDK) initKernels() {
	sdk.compressionKernel = &CompressionKernel{}
	sdk.aggregationKernel = &AggregationKernel{}
	sdk.filterKernel = &FilterKernel{}
}

// Close shuts down the SDK and releases resources.
func (sdk *HardwareAccelSDK) Close() error {
	sdk.cancel()
	sdk.pendingOps.Wait()

	sdk.mu.Lock()
	defer sdk.mu.Unlock()

	var errs []error
	for _, device := range sdk.devices {
		if err := device.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing devices: %w", errors.Join(errs...))
	}
	return nil
}

// GetAvailableDevices returns list of available accelerator devices.
func (sdk *HardwareAccelSDK) GetAvailableDevices() []AcceleratorDevice {
	sdk.mu.RLock()
	defer sdk.mu.RUnlock()

	devices := make([]AcceleratorDevice, 0, len(sdk.devices))
	for _, d := range sdk.devices {
		devices = append(devices, d)
	}
	return devices
}

// SelectDevice selects the best device for a capability.
func (sdk *HardwareAccelSDK) SelectDevice(cap AcceleratorCapability) AcceleratorDevice {
	sdk.mu.RLock()
	defer sdk.mu.RUnlock()

	// Priority: FPGA > GPU > SIMD > CPU
	priorities := []AcceleratorType{AcceleratorFPGA, AcceleratorGPU, AcceleratorSIMD, AcceleratorCPU}

	for _, t := range priorities {
		if d, ok := sdk.devices[t]; ok {
			if d.Capabilities()&cap != 0 {
				return d
			}
		}
	}

	// Fallback to CPU
	return sdk.devices[AcceleratorCPU]
}

// CompressAccelerated performs hardware-accelerated compression.
func (sdk *HardwareAccelSDK) CompressAccelerated(data []float64) ([]byte, error) {
	if !sdk.config.CompressionAcceleration {
		return sdk.compressCPU(data)
	}

	device := sdk.SelectDevice(CapabilityCompression)
	start := time.Now()

	var result []byte
	var err error

	switch device.Type() {
	case AcceleratorGPU:
		result, err = sdk.compressGPU(data)
		if err == nil {
			sdk.stats.gpuOperations.Add(1)
			sdk.stats.gpuBytesProcessed.Add(int64(len(data) * 8))
			sdk.stats.totalGPUTimeNs.Add(time.Since(start).Nanoseconds())
		} else {
			sdk.stats.gpuErrors.Add(1)
			if sdk.config.FallbackToCPU {
				result, err = sdk.compressCPU(data)
				sdk.stats.cpuFallbacks.Add(1)
			}
		}
	case AcceleratorFPGA:
		result, err = sdk.compressFPGA(data)
		if err == nil {
			sdk.stats.fpgaOperations.Add(1)
			sdk.stats.fpgaBytesProcessed.Add(int64(len(data) * 8))
			sdk.stats.totalFPGATimeNs.Add(time.Since(start).Nanoseconds())
		} else {
			sdk.stats.fpgaErrors.Add(1)
			if sdk.config.FallbackToCPU {
				result, err = sdk.compressCPU(data)
				sdk.stats.cpuFallbacks.Add(1)
			}
		}
	case AcceleratorSIMD:
		result, err = sdk.compressSIMD(data)
		if err == nil {
			sdk.stats.simdOperations.Add(1)
			sdk.stats.simdBytesProcessed.Add(int64(len(data) * 8))
			sdk.stats.totalSIMDTimeNs.Add(time.Since(start).Nanoseconds())
		}
	default:
		result, err = sdk.compressCPU(data)
	}

	return result, err
}

// DecompressAccelerated performs hardware-accelerated decompression.
func (sdk *HardwareAccelSDK) DecompressAccelerated(data []byte, count int) ([]float64, error) {
	if !sdk.config.CompressionAcceleration {
		return sdk.decompressCPU(data, count)
	}

	device := sdk.SelectDevice(CapabilityDecompression)
	start := time.Now()

	var result []float64
	var err error

	switch device.Type() {
	case AcceleratorGPU:
		result, err = sdk.decompressGPU(data, count)
		if err == nil {
			sdk.stats.gpuOperations.Add(1)
			sdk.stats.totalGPUTimeNs.Add(time.Since(start).Nanoseconds())
		} else if sdk.config.FallbackToCPU {
			result, err = sdk.decompressCPU(data, count)
			sdk.stats.cpuFallbacks.Add(1)
		}
	case AcceleratorSIMD:
		result, err = sdk.decompressSIMD(data, count)
		if err == nil {
			sdk.stats.simdOperations.Add(1)
			sdk.stats.totalSIMDTimeNs.Add(time.Since(start).Nanoseconds())
		}
	default:
		result, err = sdk.decompressCPU(data, count)
	}

	return result, err
}

// AggregateAccelerated performs hardware-accelerated aggregation.
func (sdk *HardwareAccelSDK) AggregateAccelerated(op AggregateOp, data []float64) (float64, error) {
	if !sdk.config.AggregationAcceleration || len(data) < sdk.config.GPUBatchSize {
		return sdk.aggregateCPU(op, data)
	}

	device := sdk.SelectDevice(CapabilityAggregation)
	start := time.Now()

	var result float64
	var err error

	switch device.Type() {
	case AcceleratorGPU:
		result, err = sdk.aggregateGPU(op, data)
		if err == nil {
			sdk.stats.gpuOperations.Add(1)
			sdk.stats.gpuBytesProcessed.Add(int64(len(data) * 8))
			sdk.stats.totalGPUTimeNs.Add(time.Since(start).Nanoseconds())
		} else if sdk.config.FallbackToCPU {
			result, err = sdk.aggregateCPU(op, data)
			sdk.stats.cpuFallbacks.Add(1)
		}
	case AcceleratorSIMD:
		result, err = sdk.aggregateSIMD(op, data)
		if err == nil {
			sdk.stats.simdOperations.Add(1)
			sdk.stats.simdBytesProcessed.Add(int64(len(data) * 8))
			sdk.stats.totalSIMDTimeNs.Add(time.Since(start).Nanoseconds())
		}
	default:
		result, err = sdk.aggregateCPU(op, data)
	}

	return result, err
}

// AggregateOp represents an aggregation operation.
type AggregateOp int

const (
	AggregateSum AggregateOp = iota
	AggregateMin
	AggregateMax
	AggregateMean
	AggregateCount
	AggregateStdDev
	AggregateVariance
)

// FilterAccelerated performs hardware-accelerated filtering.
func (sdk *HardwareAccelSDK) FilterAccelerated(data []float64, predicate func(float64) bool) ([]float64, error) {
	if !sdk.config.QueryAcceleration {
		return sdk.filterCPU(data, predicate)
	}

	// For predicates we need CPU evaluation, but can parallelize with SIMD
	device := sdk.SelectDevice(CapabilityFiltering)

	switch device.Type() {
	case AcceleratorSIMD:
		return sdk.filterSIMD(data, predicate)
	default:
		return sdk.filterCPU(data, predicate)
	}
}

// SortAccelerated performs hardware-accelerated sorting.
func (sdk *HardwareAccelSDK) SortAccelerated(data []float64) error {
	if !sdk.config.QueryAcceleration || len(data) < sdk.config.GPUBatchSize {
		return sdk.sortCPU(data)
	}

	device := sdk.SelectDevice(CapabilitySort)

	switch device.Type() {
	case AcceleratorGPU:
		return sdk.sortGPU(data)
	default:
		return sdk.sortCPU(data)
	}
}

// VectorMathAccelerated performs hardware-accelerated vector math.
func (sdk *HardwareAccelSDK) VectorMathAccelerated(op VectorOp, a, b []float64) ([]float64, error) {
	if len(a) != len(b) {
		return nil, fmt.Errorf("vector length mismatch: %d vs %d", len(a), len(b))
	}
	device := sdk.SelectDevice(CapabilityVectorMath)

	switch device.Type() {
	case AcceleratorGPU:
		return sdk.vectorMathGPU(op, a, b)
	case AcceleratorSIMD:
		return sdk.vectorMathSIMD(op, a, b)
	default:
		return sdk.vectorMathCPU(op, a, b)
	}
}

// VectorOp represents a vector operation.
type VectorOp int

const (
	VectorAdd VectorOp = iota
	VectorSub
	VectorMul
	VectorDiv
	VectorDot
)

// FFTAccelerated performs hardware-accelerated FFT.
func (sdk *HardwareAccelSDK) FFTAccelerated(data []float64) ([]complex128, error) {
	device := sdk.SelectDevice(CapabilityFFT)

	switch device.Type() {
	case AcceleratorGPU:
		return sdk.fftGPU(data)
	default:
		return sdk.fftCPU(data)
	}
}

// Stats returns acceleration statistics.
func (sdk *HardwareAccelSDK) Stats() HardwareAccelStats {
	return HardwareAccelStats{
		GPUOperations:      sdk.stats.gpuOperations.Load(),
		FPGAOperations:     sdk.stats.fpgaOperations.Load(),
		SIMDOperations:     sdk.stats.simdOperations.Load(),
		CPUFallbacks:       sdk.stats.cpuFallbacks.Load(),
		GPUBytesProcessed:  sdk.stats.gpuBytesProcessed.Load(),
		FPGABytesProcessed: sdk.stats.fpgaBytesProcessed.Load(),
		SIMDBytesProcessed: sdk.stats.simdBytesProcessed.Load(),
		TotalGPUTimeNs:     sdk.stats.totalGPUTimeNs.Load(),
		TotalFPGATimeNs:    sdk.stats.totalFPGATimeNs.Load(),
		TotalSIMDTimeNs:    sdk.stats.totalSIMDTimeNs.Load(),
		GPUMemoryUsed:      sdk.stats.gpuMemoryUsed.Load(),
		FPGAMemoryUsed:     sdk.stats.fpgaMemoryUsed.Load(),
		GPUErrors:          sdk.stats.gpuErrors.Load(),
		FPGAErrors:         sdk.stats.fpgaErrors.Load(),
	}
}

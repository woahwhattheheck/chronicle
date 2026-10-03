package chronicle

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// EXPERIMENTAL: This API is unstable and may change without notice.
// WASMPluginConfig configures a single WASM plugin instance.
type WASMPluginConfig struct {
	Name          string
	Path          string // path to .wasm file
	WASMBytes     []byte // alternatively, raw WASM bytes
	MaxMemoryMB   int
	MaxExecTimeMs int64
	Permissions   WASMPermissions
}

// WASMPermissions controls what host functions a plugin can call.
type WASMPermissions struct {
	CanReadPoints  bool
	CanWritePoints bool
	CanFilter      bool
	CanAggregate   bool
	CanAccessTags  bool
	CanLog         bool
}

// DefaultWASMPermissions returns read-only permissions.
func DefaultWASMPermissions() WASMPermissions {
	return WASMPermissions{
		CanReadPoints:  true,
		CanWritePoints: false,
		CanFilter:      true,
		CanAggregate:   true,
		CanAccessTags:  true,
		CanLog:         true,
	}
}

// WASMPluginState represents the lifecycle state of a plugin.
type WASMPluginState int

const (
	WASMPluginStateUnloaded WASMPluginState = iota
	WASMPluginStateLoading
	WASMPluginStateReady
	WASMPluginStateRunning
	WASMPluginStateError
	WASMPluginStateStopped
)

func (s WASMPluginState) String() string {
	switch s {
	case WASMPluginStateUnloaded:
		return "unloaded"
	case WASMPluginStateLoading:
		return "loading"
	case WASMPluginStateReady:
		return "ready"
	case WASMPluginStateRunning:
		return "running"
	case WASMPluginStateError:
		return "error"
	case WASMPluginStateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// WASMHostABI defines the interface that host (Chronicle) provides to WASM plugins.
// Implementations bridge between the WASM runtime and Chronicle's internal APIs.
type WASMHostABI interface {
	// Point operations
	ReadPoint(index int) (*Point, error)
	WritePoint(p Point) error
	PointCount() int

	// Tag operations
	GetTag(pointIndex int, key string) (string, error)
	SetTag(pointIndex int, key, value string) error

	// Aggregation
	Sum(metricName string) (float64, error)
	Avg(metricName string) (float64, error)
	Min(metricName string) (float64, error)
	Max(metricName string) (float64, error)

	// Logging
	Log(level, message string)
}

// WASMEngine is the interface for a WASM execution runtime.
// Implementations wrap specific runtimes (wazero, wasmtime, wasmer).
type WASMEngine interface {
	// Compile compiles WASM bytes into a module.
	Compile(ctx context.Context, wasm []byte) (WASMModule, error)
	// Close releases all engine resources.
	Close(ctx context.Context) error
}

// WASMModule represents a compiled WASM module.
type WASMModule interface {
	// Instantiate creates a running instance with host bindings.
	Instantiate(ctx context.Context, abi WASMHostABI) (WASMInstance, error)
	// Close releases module resources.
	Close(ctx context.Context) error
}

// WASMInstance represents a running WASM plugin instance.
type WASMInstance interface {
	// Call invokes a named exported function.
	Call(ctx context.Context, funcName string, args ...uint64) ([]uint64, error)
	// Close terminates the instance.
	Close(ctx context.Context) error
}

// WASMPluginInfo describes a loaded plugin.
type WASMPluginInfo struct {
	Name        string          `json:"name"`
	State       WASMPluginState `json:"state"`
	LoadedAt    time.Time       `json:"loaded_at,omitempty"`
	LastExecAt  time.Time       `json:"last_exec_at,omitempty"`
	ExecCount   int64           `json:"exec_count"`
	ErrorCount  int64           `json:"error_count"`
	AvgExecMs   float64         `json:"avg_exec_ms"`
	Permissions WASMPermissions `json:"permissions"`
}

// WASMPlugin holds the runtime state for a single plugin.
type WASMPlugin struct {
	config      WASMPluginConfig
	state       WASMPluginState
	module      WASMModule
	instance    WASMInstance
	loadedAt    time.Time
	lastExec    time.Time
	execCount   atomic.Int64
	errorCount  atomic.Int64
	totalExecNs atomic.Int64
	mu          sync.Mutex
}

// WASMRuntimeConfig configures the plugin runtime host.
type WASMRuntimeConfig struct {
	MaxPlugins       int
	DefaultMemoryMB  int
	DefaultTimeoutMs int64
	EnableSandbox    bool
}

// DefaultWASMRuntimeConfig returns production defaults.
func DefaultWASMRuntimeConfig() WASMRuntimeConfig {
	return WASMRuntimeConfig{
		MaxPlugins:       32,
		DefaultMemoryMB:  64,
		DefaultTimeoutMs: 5000,
		EnableSandbox:    true,
	}
}

// WASMRuntime manages the lifecycle of WASM plugins.
type WASMRuntime struct {
	config WASMRuntimeConfig
	engine WASMEngine

	mu      sync.RWMutex
	plugins map[string]*WASMPlugin

	onLoad   func(name string)
	onUnload func(name string)
	onError  func(name string, err error)
}

// NewWASMRuntime creates a new WASM plugin runtime.
// Pass nil for engine to create a runtime without execution capability
// (useful for plugin management without a WASM engine installed).
func NewWASMRuntime(engine WASMEngine, config WASMRuntimeConfig) *WASMRuntime {
	if config.MaxPlugins <= 0 {
		config.MaxPlugins = 32
	}
	if config.DefaultMemoryMB <= 0 {
		config.DefaultMemoryMB = 64
	}
	if config.DefaultTimeoutMs <= 0 {
		config.DefaultTimeoutMs = 5000
	}

	return &WASMRuntime{
		config:  config,
		engine:  engine,
		plugins: make(map[string]*WASMPlugin),
	}
}

// OnLoad registers a callback for plugin load events.
func (rt *WASMRuntime) OnLoad(fn func(string)) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.onLoad = fn
}

// OnUnload registers a callback for plugin unload events.
func (rt *WASMRuntime) OnUnload(fn func(string)) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.onUnload = fn
}

// OnError registers a callback for plugin error events.
func (rt *WASMRuntime) OnError(fn func(string, error)) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.onError = fn
}

// LoadPlugin compiles and loads a WASM plugin.
func (rt *WASMRuntime) LoadPlugin(ctx context.Context, config WASMPluginConfig) error {
	if config.Name == "" {
		return fmt.Errorf("wasm: plugin name is required")
	}
	if len(config.WASMBytes) == 0 && config.Path == "" {
		return fmt.Errorf("wasm: either WASMBytes or Path is required")
	}

	rt.mu.Lock()
	if len(rt.plugins) >= rt.config.MaxPlugins {
		rt.mu.Unlock()
		return fmt.Errorf("wasm: max plugins (%d) reached", rt.config.MaxPlugins)
	}
	rt.mu.Unlock()

	plugin := &WASMPlugin{
		config: config,
		state:  WASMPluginStateLoading,
	}

	// Compile if engine is available
	if rt.engine != nil && len(config.WASMBytes) > 0 {
		module, err := rt.engine.Compile(ctx, config.WASMBytes)
		if err != nil {
			plugin.state = WASMPluginStateError
			rt.mu.Lock()
			rt.plugins[config.Name] = plugin
			rt.mu.Unlock()
			return fmt.Errorf("wasm: compile failed: %w", err)
		}
		plugin.module = module
	}

	plugin.state = WASMPluginStateReady
	plugin.loadedAt = time.Now()

	rt.mu.Lock()
	rt.plugins[config.Name] = plugin
	onLoad := rt.onLoad
	rt.mu.Unlock()

	if onLoad != nil {
		onLoad(config.Name)
	}

	return nil
}

// UnloadPlugin removes a plugin and releases its resources.
func (rt *WASMRuntime) UnloadPlugin(ctx context.Context, name string) error {
	rt.mu.Lock()
	plugin, ok := rt.plugins[name]
	if !ok {
		rt.mu.Unlock()
		return fmt.Errorf("wasm: plugin %q not found", name)
	}
	delete(rt.plugins, name)
	onUnload := rt.onUnload
	rt.mu.Unlock()

	plugin.mu.Lock()
	defer plugin.mu.Unlock()

	if plugin.instance != nil {
		plugin.instance.Close(ctx)
	}
	if plugin.module != nil {
		plugin.module.Close(ctx)
	}
	plugin.state = WASMPluginStateStopped

	if onUnload != nil {
		onUnload(name)
	}
	return nil
}

// ExecPlugin invokes a function on a loaded plugin.
func (rt *WASMRuntime) ExecPlugin(ctx context.Context, name, funcName string, args ...uint64) ([]uint64, error) {
	rt.mu.RLock()
	plugin, ok := rt.plugins[name]
	rt.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("wasm: plugin %q not found", name)
	}

	plugin.mu.Lock()
	defer plugin.mu.Unlock()

	if plugin.state != WASMPluginStateReady && plugin.state != WASMPluginStateRunning {
		return nil, fmt.Errorf("wasm: plugin %q not ready (state: %s)", name, plugin.state)
	}

	if plugin.instance == nil && plugin.module != nil {
		inst, err := plugin.module.Instantiate(ctx, nil)
		if err != nil {
			plugin.state = WASMPluginStateError
			plugin.errorCount.Add(1)
			return nil, fmt.Errorf("wasm: instantiate failed: %w", err)
		}
		plugin.instance = inst
	}

	if plugin.instance == nil {
		return nil, fmt.Errorf("wasm: no instance available for plugin %q", name)
	}

	// Apply timeout
	timeoutMs := rt.config.DefaultTimeoutMs
	if plugin.config.MaxExecTimeMs > 0 {
		timeoutMs = plugin.config.MaxExecTimeMs
	}
	execCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	start := time.Now()
	plugin.state = WASMPluginStateRunning
	result, err := plugin.instance.Call(execCtx, funcName, args...)
	elapsed := time.Since(start)

	plugin.lastExec = time.Now()
	plugin.execCount.Add(1)
	plugin.totalExecNs.Add(elapsed.Nanoseconds())

	if err != nil {
		plugin.errorCount.Add(1)
		plugin.state = WASMPluginStateReady
		return nil, fmt.Errorf("wasm: exec %s.%s failed: %w", name, funcName, err)
	}

	plugin.state = WASMPluginStateReady
	return result, nil
}

// GetPlugin returns info about a loaded plugin.
func (rt *WASMRuntime) GetPlugin(name string) (*WASMPluginInfo, error) {
	rt.mu.RLock()
	plugin, ok := rt.plugins[name]
	rt.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("wasm: plugin %q not found", name)
	}

	execCount := plugin.execCount.Load()
	totalNs := plugin.totalExecNs.Load()
	var avgMs float64
	if execCount > 0 {
		avgMs = float64(totalNs) / float64(execCount) / 1e6
	}

	return &WASMPluginInfo{
		Name:        plugin.config.Name,
		State:       plugin.state,
		LoadedAt:    plugin.loadedAt,
		LastExecAt:  plugin.lastExec,
		ExecCount:   execCount,
		ErrorCount:  plugin.errorCount.Load(),
		AvgExecMs:   avgMs,
		Permissions: plugin.config.Permissions,
	}, nil
}

// ListPlugins returns info for all loaded plugins.
func (rt *WASMRuntime) ListPlugins() []WASMPluginInfo {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	infos := make([]WASMPluginInfo, 0, len(rt.plugins))
	for _, plugin := range rt.plugins {
		execCount := plugin.execCount.Load()
		totalNs := plugin.totalExecNs.Load()
		var avgMs float64
		if execCount > 0 {
			avgMs = float64(totalNs) / float64(execCount) / 1e6
		}
		infos = append(infos, WASMPluginInfo{
			Name:        plugin.config.Name,
			State:       plugin.state,
			LoadedAt:    plugin.loadedAt,
			ExecCount:   execCount,
			ErrorCount:  plugin.errorCount.Load(),
			AvgExecMs:   avgMs,
			Permissions: plugin.config.Permissions,
		})
	}
	return infos
}

// PluginCount returns the number of loaded plugins.
func (rt *WASMRuntime) PluginCount() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return len(rt.plugins)
}

// Close shuts down the runtime and all plugins.

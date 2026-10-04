package chronicle

// config.go defines all database configuration.
//
// Use DefaultConfig(path) for sensible defaults. Use ConfigBuilder for
// fluent construction. Config.Validate() checks all fields for logical
// errors. Legacy flat fields (MaxMemory, SyncInterval, etc.) are
// normalized to their nested equivalents during validation.

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Config defines database configuration.
type Config struct {
	// Path is the file path for the database. Required unless StorageBackend is provided.
	Path string

	// StorageBackend is an optional backend for partition storage.
	// If provided, partitions are stored using this backend instead of the local file.
	// The local file is still used for the index and WAL.
	StorageBackend StorageBackend

	// Storage holds core storage settings.
	Storage StorageConfig

	// WAL configures write-ahead logging.
	WAL WALConfig

	// Retention configures data retention and compaction.
	Retention RetentionConfig

	// Query configures query execution limits.
	Query QueryConfig

	// HTTP configures the HTTP API server.
	HTTP HTTPConfig

	// Replication configures outbound replication to a remote endpoint.
	Replication *ReplicationConfig

	// Schemas defines metric schemas for validation.
	// If nil, no validation is performed.
	Schemas []MetricSchema

	// StrictSchema if true rejects points for metrics without a registered schema.
	StrictSchema bool

	// Encryption configures encryption at rest.
	// If nil or Enabled is false, data is stored unencrypted.
	Encryption *EncryptionConfig

	// Auth configures HTTP API authentication.
	// If nil or Enabled is false, no authentication is required.
	Auth *AuthConfig

	// RateLimitPerSecond is the maximum requests per second per IP.
	// Default: 1000. Set to 0 to disable rate limiting.
	RateLimitPerSecond int

	// ClickHouse configures ClickHouse-compatible HTTP interface.
	// If nil or Enabled is false, ClickHouse protocol is disabled.
	ClickHouse *ClickHouseConfig

	// LogLevel controls the logging verbosity. Supported values: "debug", "info",
	// "warn", "error". Default: "info". Can be overridden by the CHRONICLE_LOG_LEVEL
	// environment variable.
	LogLevel string

	// Deprecated: use Storage.MaxMemory.
	MaxMemory int64
	// Deprecated: use Storage.MaxStorageBytes.
	MaxStorageBytes int64
	// Deprecated: use WAL.SyncInterval.
	SyncInterval time.Duration
	// Deprecated: use Storage.PartitionDuration.
	PartitionDuration time.Duration
	// Deprecated: use WAL.WALMaxSize.
	WALMaxSize int64
	// Deprecated: use WAL.WALRetain.
	WALRetain int
	// Deprecated: use Retention.RetentionDuration.
	RetentionDuration time.Duration
	// Deprecated: use Retention.DownsampleRules.
	DownsampleRules []DownsampleRule
	// Deprecated: use Storage.BufferSize.
	BufferSize int
	// Deprecated: use Retention.CompactionWorkers.
	CompactionWorkers int
	// Deprecated: use Retention.CompactionInterval.
	CompactionInterval time.Duration
	// Deprecated: use Query.QueryTimeout.
	QueryTimeout time.Duration
	// Deprecated: use HTTP.HTTPEnabled.
	HTTPEnabled bool
	// Deprecated: use HTTP.HTTPPort.
	HTTPPort int
	// Deprecated: use HTTP.PrometheusRemoteWriteEnabled.
	PrometheusRemoteWriteEnabled bool
	// Legacy: no grouped replacement yet.
	ContinuousQueries []ContinuousQuery

	// usedDeprecatedFields tracks which deprecated config fields were set during normalize().
	// This allows the DeprecationEngine to emit structured warnings after DB Open().
	usedDeprecatedFields []string
}

// StorageConfig groups core storage settings.
type StorageConfig struct {
	// MaxMemory is the maximum memory budget for buffers and caches in bytes.
	// Default: 64MB.
	MaxMemory int64

	// MaxStorageBytes is the maximum database file size in bytes.
	// When exceeded, oldest partitions are removed. 0 means unlimited.
	MaxStorageBytes int64

	// PartitionDuration is the time span covered by each partition.
	// Default: 1 hour.
	PartitionDuration time.Duration

	// BufferSize is the number of points to buffer before flushing to storage.
	// Default: 10,000.
	BufferSize int
}

// WALConfig groups write-ahead log settings.
type WALConfig struct {
	// SyncInterval is how often the WAL is synced to disk.
	// Default: 1 second.
	SyncInterval time.Duration

	// WALMaxSize is the maximum size of a single WAL file before rotation.
	// Default: 128MB.
	WALMaxSize int64

	// WALRetain is the number of old WAL files to keep after rotation.
	// Default: 3.
	WALRetain int
}

// RetentionConfig groups retention and compaction settings.
type RetentionConfig struct {
	// RetentionDuration is how long data is kept before automatic deletion.
	// 0 means data is kept indefinitely.
	RetentionDuration time.Duration

	// DownsampleRules defines automatic downsampling policies.
	DownsampleRules []DownsampleRule

	// CompactionWorkers is the number of background compaction workers.
	// Default: 1.
	CompactionWorkers int

	// CompactionInterval is how often compaction is triggered.
	// Default: 30 minutes.
	CompactionInterval time.Duration
}

// QueryConfig groups query execution settings.
type QueryConfig struct {
	// QueryTimeout is the maximum duration for query execution.
	// Default: 30 seconds.
	QueryTimeout time.Duration
}

// HTTPConfig groups HTTP server settings.
type HTTPConfig struct {
	// HTTPEnabled enables the HTTP API server.
	// Default: false.
	HTTPEnabled bool

	// HTTPPort is the port for the HTTP API server.
	// Default: 8086.
	HTTPPort int

	// HTTPBindAddress is the host or IP address for the HTTP API listener, without a port.
	// Empty defaults to 127.0.0.1. Set explicitly to expose the API on another interface.
	HTTPBindAddress string

	// PrometheusRemoteWriteEnabled enables the Prometheus remote write endpoint.
	// Default: false.
	PrometheusRemoteWriteEnabled bool

	// CORSAllowedOrigins is the list of origins permitted for cross-origin requests.
	// Use ["*"] to allow all origins. Empty disables CORS headers.
	CORSAllowedOrigins []string
}

// normalize populates grouped config from legacy fields when needed.
func (c *Config) normalize() {
	c.usedDeprecatedFields = nil
	if c.Storage.MaxMemory == 0 && c.MaxMemory != 0 {
		slog.Warn("Config.MaxMemory is deprecated, use Config.Storage.MaxMemory instead")
		c.Storage.MaxMemory = c.MaxMemory
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.MaxMemory")
	}
	if c.Storage.MaxStorageBytes == 0 && c.MaxStorageBytes != 0 {
		slog.Warn("Config.MaxStorageBytes is deprecated, use Config.Storage.MaxStorageBytes instead")
		c.Storage.MaxStorageBytes = c.MaxStorageBytes
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.MaxStorageBytes")
	}
	if c.Storage.PartitionDuration == 0 && c.PartitionDuration != 0 {
		slog.Warn("Config.PartitionDuration is deprecated, use Config.Storage.PartitionDuration instead")
		c.Storage.PartitionDuration = c.PartitionDuration
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.PartitionDuration")
	}
	if c.Storage.BufferSize == 0 && c.BufferSize != 0 {
		slog.Warn("Config.BufferSize is deprecated, use Config.Storage.BufferSize instead")
		c.Storage.BufferSize = c.BufferSize
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.BufferSize")
	}
	if c.WAL.SyncInterval == 0 && c.SyncInterval != 0 {
		slog.Warn("Config.SyncInterval is deprecated, use Config.WAL.SyncInterval instead")
		c.WAL.SyncInterval = c.SyncInterval
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.SyncInterval")
	}
	if c.WAL.WALMaxSize == 0 && c.WALMaxSize != 0 {
		slog.Warn("Config.WALMaxSize is deprecated, use Config.WAL.WALMaxSize instead")
		c.WAL.WALMaxSize = c.WALMaxSize
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.WALMaxSize")
	}
	if c.WAL.WALRetain == 0 && c.WALRetain != 0 {
		slog.Warn("Config.WALRetain is deprecated, use Config.WAL.WALRetain instead")
		c.WAL.WALRetain = c.WALRetain
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.WALRetain")
	}
	if c.Retention.RetentionDuration == 0 && c.RetentionDuration != 0 {
		slog.Warn("Config.RetentionDuration is deprecated, use Config.Retention.RetentionDuration instead")
		c.Retention.RetentionDuration = c.RetentionDuration
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.RetentionDuration")
	}
	if len(c.Retention.DownsampleRules) == 0 && len(c.DownsampleRules) > 0 {
		slog.Warn("Config.DownsampleRules is deprecated, use Config.Retention.DownsampleRules instead")
		c.Retention.DownsampleRules = c.DownsampleRules
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.DownsampleRules")
	}
	if c.Retention.CompactionWorkers == 0 && c.CompactionWorkers != 0 {
		slog.Warn("Config.CompactionWorkers is deprecated, use Config.Retention.CompactionWorkers instead")
		c.Retention.CompactionWorkers = c.CompactionWorkers
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.CompactionWorkers")
	}
	if c.Retention.CompactionInterval == 0 && c.CompactionInterval != 0 {
		slog.Warn("Config.CompactionInterval is deprecated, use Config.Retention.CompactionInterval instead")
		c.Retention.CompactionInterval = c.CompactionInterval
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.CompactionInterval")
	}
	if c.Query.QueryTimeout == 0 && c.QueryTimeout != 0 {
		slog.Warn("Config.QueryTimeout is deprecated, use Config.Query.QueryTimeout instead")
		c.Query.QueryTimeout = c.QueryTimeout
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.QueryTimeout")
	}
	if !c.HTTP.HTTPEnabled && c.HTTPEnabled {
		slog.Warn("Config.HTTPEnabled is deprecated, use Config.HTTP.HTTPEnabled instead")
		c.HTTP.HTTPEnabled = c.HTTPEnabled
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.HTTPEnabled")
	}
	if c.HTTP.HTTPPort == 0 && c.HTTPPort != 0 {
		slog.Warn("Config.HTTPPort is deprecated, use Config.HTTP.HTTPPort instead")
		c.HTTP.HTTPPort = c.HTTPPort
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.HTTPPort")
	}
	if !c.HTTP.PrometheusRemoteWriteEnabled && c.PrometheusRemoteWriteEnabled {
		slog.Warn("Config.PrometheusRemoteWriteEnabled is deprecated, use Config.HTTP.PrometheusRemoteWriteEnabled instead")
		c.HTTP.PrometheusRemoteWriteEnabled = c.PrometheusRemoteWriteEnabled
		c.usedDeprecatedFields = append(c.usedDeprecatedFields, "Config.PrometheusRemoteWriteEnabled")
	}
}

// UsedDeprecatedFields returns the list of deprecated config fields that were
// set during normalization. Useful for post-Open() deprecation reporting.
func (c *Config) UsedDeprecatedFields() []string {
	return c.usedDeprecatedFields
}

// initLogLevel configures the default slog handler based on LogLevel.
// The CHRONICLE_LOG_LEVEL environment variable overrides the Config value.
func (c *Config) initLogLevel() {
	level := c.LogLevel
	if env := os.Getenv("CHRONICLE_LOG_LEVEL"); env != "" {
		level = env
	}
	if level == "" {
		return
	}
	var sl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		sl = slog.LevelDebug
	case "info":
		sl = slog.LevelInfo
	case "warn":
		sl = slog.LevelWarn
	case "error":
		sl = slog.LevelError
	default:
		return
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: sl})))
}

// syncLegacyFields mirrors grouped config into legacy fields for compatibility.
func (c *Config) syncLegacyFields() {
	if c.Storage.MaxMemory != 0 {
		c.MaxMemory = c.Storage.MaxMemory
	}
	c.MaxStorageBytes = c.Storage.MaxStorageBytes
	if c.Storage.PartitionDuration != 0 {
		c.PartitionDuration = c.Storage.PartitionDuration
	}
	if c.Storage.BufferSize != 0 {
		c.BufferSize = c.Storage.BufferSize
	}
	if c.WAL.SyncInterval != 0 {
		c.SyncInterval = c.WAL.SyncInterval
	}
	if c.WAL.WALMaxSize != 0 {
		c.WALMaxSize = c.WAL.WALMaxSize
	}
	if c.WAL.WALRetain != 0 {
		c.WALRetain = c.WAL.WALRetain
	}
	if c.Retention.RetentionDuration != 0 {
		c.RetentionDuration = c.Retention.RetentionDuration
	}
	if len(c.Retention.DownsampleRules) > 0 {
		c.DownsampleRules = c.Retention.DownsampleRules
	}
	if c.Retention.CompactionWorkers != 0 {
		c.CompactionWorkers = c.Retention.CompactionWorkers
	}
	if c.Retention.CompactionInterval != 0 {
		c.CompactionInterval = c.Retention.CompactionInterval
	}
	if c.Query.QueryTimeout != 0 {
		c.QueryTimeout = c.Query.QueryTimeout
	}
	if c.HTTP.HTTPEnabled {
		c.HTTPEnabled = c.HTTP.HTTPEnabled
	}
	if c.HTTP.HTTPPort != 0 {
		c.HTTPPort = c.HTTP.HTTPPort
	}
	if c.HTTP.PrometheusRemoteWriteEnabled {
		c.PrometheusRemoteWriteEnabled = c.HTTP.PrometheusRemoteWriteEnabled
	}
}

// AuthConfig configures HTTP API authentication.
type AuthConfig struct {
	// Enabled enables authentication on HTTP endpoints.
	Enabled bool

	// APIKeys is a list of valid API keys. At least one must be provided if Enabled is true.
	APIKeys []string

	// ReadOnlyKeys is a list of API keys that only allow read operations (queries).
	// These keys cannot write data.
	ReadOnlyKeys []string

	// ExcludePaths are paths that don't require authentication (e.g., /health).
	ExcludePaths []string
}

// HashAPIKey returns a bcrypt hash of the given plaintext API key.
func HashAPIKey(plaintext string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hashing API key: %w", err)
	}
	return string(hash), nil
}

// CompareAPIKey performs a constant-time comparison of a candidate API key
// against a stored key. If storedKey looks like a bcrypt hash (starts with
// "$2") it uses bcrypt comparison; otherwise it falls back to
// crypto/subtle.ConstantTimeCompare for backward compatibility with
// plaintext keys.
func CompareAPIKey(storedKey, candidate string) bool {
	if strings.HasPrefix(storedKey, "$2") {
		return bcrypt.CompareHashAndPassword([]byte(storedKey), []byte(candidate)) == nil
	}
	return subtle.ConstantTimeCompare([]byte(storedKey), []byte(candidate)) == 1
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig(path string) Config {
	cfg := Config{
		Path: path,
		Storage: StorageConfig{
			MaxMemory:         64 * 1024 * 1024,
			MaxStorageBytes:   0,
			PartitionDuration: time.Hour,
			BufferSize:        10_000,
		},
		WAL: WALConfig{
			SyncInterval: time.Second,
			WALMaxSize:   128 * 1024 * 1024,
			WALRetain:    3,
		},
		Retention: RetentionConfig{
			RetentionDuration:  0,
			CompactionWorkers:  1,
			CompactionInterval: 30 * time.Minute,
		},
		Query: QueryConfig{
			QueryTimeout: 30 * time.Second,
		},
		HTTP: HTTPConfig{
			HTTPEnabled: false,
			HTTPPort:    8086,
		},
		RateLimitPerSecond: 1000,
	}
	cfg.syncLegacyFields()
	return cfg
}

// NormalizedCopy returns a copy of the configuration with grouped and legacy fields aligned.
func (c Config) NormalizedCopy() Config {
	c.normalize()
	c.syncLegacyFields()
	return c
}

// Validate checks the configuration for logical errors and returns a combined error
// describing all problems found. It normalizes legacy fields before validating.
// A nil return means the configuration is valid.
func (c *Config) Validate() error {
	c.initLogLevel()
	c.normalize()

	var errs []error

	if c.Path == "" && c.StorageBackend == nil {
		errs = append(errs, fmt.Errorf("either Path or StorageBackend must be set"))
	}

	if c.Storage.PartitionDuration < 0 {
		errs = append(errs, fmt.Errorf("Storage.PartitionDuration must be non-negative, got %v", c.Storage.PartitionDuration))
	} else if c.Storage.PartitionDuration == 0 {
		errs = append(errs, fmt.Errorf("Storage.PartitionDuration must be positive (zero causes division-by-zero in partition math)"))
	}
	if c.Storage.BufferSize < 0 {
		errs = append(errs, fmt.Errorf("Storage.BufferSize must be non-negative, got %d", c.Storage.BufferSize))
	} else if c.Storage.BufferSize == 0 {
		errs = append(errs, fmt.Errorf("Storage.BufferSize must be positive (zero disables write buffering)"))
	}
	if c.Storage.MaxMemory < 0 {
		errs = append(errs, fmt.Errorf("Storage.MaxMemory must be non-negative, got %d", c.Storage.MaxMemory))
	}

	if c.WAL.SyncInterval < 0 {
		errs = append(errs, fmt.Errorf("WAL.SyncInterval must be non-negative, got %v", c.WAL.SyncInterval))
	}
	if c.WAL.WALMaxSize < 0 {
		errs = append(errs, fmt.Errorf("WAL.WALMaxSize must be non-negative, got %d", c.WAL.WALMaxSize))
	}
	if c.WAL.WALRetain < 0 {
		errs = append(errs, fmt.Errorf("WAL.WALRetain must be non-negative, got %d", c.WAL.WALRetain))
	}

	if c.Retention.RetentionDuration < 0 {
		errs = append(errs, fmt.Errorf("Retention.RetentionDuration must be non-negative, got %v", c.Retention.RetentionDuration))
	}
	if c.Retention.CompactionWorkers < 0 {
		errs = append(errs, fmt.Errorf("Retention.CompactionWorkers must be non-negative, got %d", c.Retention.CompactionWorkers))
	}
	if c.Retention.CompactionInterval < 0 {
		errs = append(errs, fmt.Errorf("Retention.CompactionInterval must be non-negative, got %v", c.Retention.CompactionInterval))
	}

	if c.Query.QueryTimeout < 0 {
		errs = append(errs, fmt.Errorf("Query.QueryTimeout must be non-negative, got %v", c.Query.QueryTimeout))
	}

	if c.HTTP.HTTPPort < 0 || c.HTTP.HTTPPort > 65535 {
		errs = append(errs, fmt.Errorf("HTTP.HTTPPort must be 0-65535, got %d", c.HTTP.HTTPPort))
	}

	if c.RateLimitPerSecond < 0 {
		errs = append(errs, fmt.Errorf("RateLimitPerSecond must be non-negative, got %d", c.RateLimitPerSecond))
	}

	if c.Auth != nil && c.Auth.Enabled && len(c.Auth.APIKeys) == 0 {
		errs = append(errs, fmt.Errorf("Auth.APIKeys must not be empty when Auth is enabled"))
	}

	// Warn about plaintext (non-bcrypt) API keys
	if c.Auth != nil && c.Auth.Enabled {
		for _, key := range c.Auth.APIKeys {
			if !strings.HasPrefix(key, "$2") {
				slog.Warn("plaintext API key detected, use chronicle.HashAPIKey() for bcrypt-hashed keys")
				break
			}
		}
		for _, key := range c.Auth.ReadOnlyKeys {
			if !strings.HasPrefix(key, "$2") {
				slog.Warn("plaintext read-only API key detected, use chronicle.HashAPIKey() for bcrypt-hashed keys")
				break
			}
		}
	}

	// Cross-field validation
	if c.Retention.RetentionDuration > 0 && c.Storage.PartitionDuration > 0 &&
		c.Retention.RetentionDuration < c.Storage.PartitionDuration {
		errs = append(errs, fmt.Errorf("Retention.RetentionDuration (%v) must be >= Storage.PartitionDuration (%v)",
			c.Retention.RetentionDuration, c.Storage.PartitionDuration))
	}

	if c.Retention.CompactionInterval > 0 && c.Storage.PartitionDuration > 0 &&
		c.Retention.CompactionInterval > c.Storage.PartitionDuration {
		errs = append(errs, fmt.Errorf("Retention.CompactionInterval (%v) should not exceed Storage.PartitionDuration (%v)",
			c.Retention.CompactionInterval, c.Storage.PartitionDuration))
	}

	if c.Replication != nil && c.Replication.Enabled {
		if c.Replication.TargetURL == "" {
			errs = append(errs, fmt.Errorf("Replication.TargetURL must be set when replication is enabled"))
		}
	}

	if c.Encryption != nil && c.Encryption.Enabled && len(c.Encryption.Key) == 0 && c.Encryption.KeyPassword == "" {
		errs = append(errs, fmt.Errorf("Encryption.Key or Encryption.KeyPassword must be set when encryption is enabled"))
	}

	// Cross-field validations
	if c.ClickHouse != nil && c.ClickHouse.Enabled && !c.HTTP.HTTPEnabled {
		errs = append(errs, fmt.Errorf("ClickHouse protocol requires HTTP.HTTPEnabled=true"))
	}

	if c.Auth != nil && c.Auth.Enabled && c.HTTP.HTTPEnabled {
		// Ensure health endpoints are excluded from auth so K8s probes work
		hasHealthExclude := false
		for _, p := range c.Auth.ExcludePaths {
			if p == "/health" || p == "/health/ready" || p == "/health/live" {
				hasHealthExclude = true
				break
			}
		}
		if !hasHealthExclude {
			slog.Warn("Auth enabled but /health endpoints are not in Auth.ExcludePaths; Kubernetes probes may fail")
		}
	}

	return errors.Join(errs...)
}

// ApplyEnvOverrides reads CHRONICLE_* environment variables and overrides
// corresponding Config fields. Only non-empty env vars are applied.
//
// Supported variables:
//
//	CHRONICLE_PATH               - database file path
//	CHRONICLE_LOG_LEVEL          - "debug", "info", "warn", "error"
//	CHRONICLE_MAX_MEMORY         - max memory in bytes
//	CHRONICLE_PARTITION_DURATION - partition span (e.g., "1h", "30m")
//	CHRONICLE_BUFFER_SIZE        - write buffer capacity
//	CHRONICLE_HTTP_ENABLED       - "true" or "false"
//	CHRONICLE_HTTP_PORT          - HTTP listen port
//	CHRONICLE_RATE_LIMIT         - requests per second (0 to disable)
//	CHRONICLE_RETENTION          - retention duration (e.g., "168h")
//	CHRONICLE_QUERY_TIMEOUT      - query timeout (e.g., "30s")
//	CHRONICLE_WAL_SYNC_INTERVAL  - WAL sync interval (e.g., "1s")
func (c *Config) ApplyEnvOverrides() error {
	var errs []error

	if v := os.Getenv("CHRONICLE_PATH"); v != "" {
		c.Path = v
	}
	if v := os.Getenv("CHRONICLE_LOG_LEVEL"); v != "" {
		c.LogLevel = v
	}
	if v := os.Getenv("CHRONICLE_MAX_MEMORY"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("CHRONICLE_MAX_MEMORY: %w", err))
		} else {
			c.Storage.MaxMemory = n
		}
	}
	if v := os.Getenv("CHRONICLE_PARTITION_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("CHRONICLE_PARTITION_DURATION: %w", err))
		} else {
			c.Storage.PartitionDuration = d
		}
	}
	if v := os.Getenv("CHRONICLE_BUFFER_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("CHRONICLE_BUFFER_SIZE: %w", err))
		} else {
			c.Storage.BufferSize = n
		}
	}
	if v := os.Getenv("CHRONICLE_HTTP_ENABLED"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("CHRONICLE_HTTP_ENABLED: %w", err))
		} else {
			c.HTTP.HTTPEnabled = b
		}
	}
	if v := os.Getenv("CHRONICLE_HTTP_PORT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("CHRONICLE_HTTP_PORT: %w", err))
		} else {
			c.HTTP.HTTPPort = n
		}
	}
	if v := os.Getenv("CHRONICLE_RATE_LIMIT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("CHRONICLE_RATE_LIMIT: %w", err))
		} else {
			c.RateLimitPerSecond = n
		}
	}
	if v := os.Getenv("CHRONICLE_RETENTION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("CHRONICLE_RETENTION: %w", err))
		} else {
			c.Retention.RetentionDuration = d
		}
	}
	if v := os.Getenv("CHRONICLE_QUERY_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("CHRONICLE_QUERY_TIMEOUT: %w", err))
		} else {
			c.Query.QueryTimeout = d
		}
	}
	if v := os.Getenv("CHRONICLE_WAL_SYNC_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("CHRONICLE_WAL_SYNC_INTERVAL: %w", err))
		} else {
			c.WAL.SyncInterval = d
		}
	}
	return errors.Join(errs...)
}

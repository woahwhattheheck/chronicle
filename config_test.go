package chronicle

import (
	"strings"
	"testing"
	"time"
)

func TestConfig_Defaults(t *testing.T) {
	cfg := DefaultConfig("/test/path.db")
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.Path != "/test/path.db" {
		t.Errorf("expected path /test/path.db, got %s", cfg.Path)
	}
	if cfg.MaxMemory != 64*1024*1024 {
		t.Error("default MaxMemory should be 64MB")
	}
	if cfg.Storage.MaxMemory != 64*1024*1024 {
		t.Error("default Storage.MaxMemory should be 64MB")
	}
	if cfg.PartitionDuration != time.Hour {
		t.Error("default PartitionDuration should be 1 hour")
	}
	if cfg.Storage.PartitionDuration != time.Hour {
		t.Error("default Storage.PartitionDuration should be 1 hour")
	}
	if cfg.BufferSize != 10_000 {
		t.Error("default BufferSize should be 10000")
	}
	if cfg.Storage.BufferSize != 10_000 {
		t.Error("default Storage.BufferSize should be 10000")
	}
	if cfg.SyncInterval != time.Second {
		t.Error("default SyncInterval should be 1 second")
	}
	if cfg.WAL.SyncInterval != time.Second {
		t.Error("default WAL.SyncInterval should be 1 second")
	}
	if cfg.QueryTimeout != 30*time.Second {
		t.Error("default QueryTimeout should be 30 seconds")
	}
	if cfg.Query.QueryTimeout != 30*time.Second {
		t.Error("default Query.QueryTimeout should be 30 seconds")
	}
	if cfg.HTTPPort != 8086 {
		t.Error("default HTTPPort should be 8086")
	}
	if cfg.HTTP.HTTPPort != 8086 {
		t.Error("default HTTP.HTTPPort should be 8086")
	}
	if cfg.WALMaxSize != 128*1024*1024 {
		t.Error("default WALMaxSize should be 128MB")
	}
	if cfg.WAL.WALMaxSize != 128*1024*1024 {
		t.Error("default WAL.WALMaxSize should be 128MB")
	}
	if cfg.WALRetain != 3 {
		t.Error("default WALRetain should be 3")
	}
	if cfg.WAL.WALRetain != 3 {
		t.Error("default WAL.WALRetain should be 3")
	}
	if cfg.CompactionWorkers != 1 {
		t.Error("default CompactionWorkers should be 1")
	}
	if cfg.Retention.CompactionWorkers != 1 {
		t.Error("default Retention.CompactionWorkers should be 1")
	}
	if cfg.CompactionInterval != 30*time.Minute {
		t.Error("default CompactionInterval should be 30 minutes")
	}
	if cfg.Retention.CompactionInterval != 30*time.Minute {
		t.Error("default Retention.CompactionInterval should be 30 minutes")
	}
}

func TestDefaultConfig_Defaults(t *testing.T) {
	cfg := DefaultConfig("")
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.Path != "" {
		t.Error("path should be empty when not provided")
	}
	if cfg.RetentionDuration != 0 {
		t.Error("default RetentionDuration should be 0 (unlimited)")
	}
	if cfg.Retention.RetentionDuration != 0 {
		t.Error("default Retention.RetentionDuration should be 0 (unlimited)")
	}
	if cfg.MaxStorageBytes != 0 {
		t.Error("default MaxStorageBytes should be 0 (unlimited)")
	}
	if cfg.Storage.MaxStorageBytes != 0 {
		t.Error("default Storage.MaxStorageBytes should be 0 (unlimited)")
	}
	if cfg.HTTPEnabled {
		t.Error("HTTPEnabled should default to false")
	}
	if cfg.HTTP.HTTPEnabled {
		t.Error("HTTP.HTTPEnabled should default to false")
	}
	if cfg.PrometheusRemoteWriteEnabled {
		t.Error("PrometheusRemoteWriteEnabled should default to false")
	}
	if cfg.HTTP.PrometheusRemoteWriteEnabled {
		t.Error("HTTP.PrometheusRemoteWriteEnabled should default to false")
	}
	if cfg.StrictSchema {
		t.Error("StrictSchema should default to false")
	}
}

func TestConfig_CustomValues(t *testing.T) {
	cfg := Config{
		Path:              "/custom/path",
		MaxMemory:         100 * 1024 * 1024,
		PartitionDuration: 2 * time.Hour,
		RetentionDuration: 24 * time.Hour,
		BufferSize:        5000,
		HTTPEnabled:       true,
		HTTPPort:          9000,
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.Path != "/custom/path" {
		t.Error("custom path not set")
	}
	if cfg.MaxMemory != 100*1024*1024 {
		t.Error("custom MaxMemory not set")
	}
	if cfg.Storage.MaxMemory != 100*1024*1024 {
		t.Error("custom Storage.MaxMemory not set")
	}
	if cfg.PartitionDuration != 2*time.Hour {
		t.Error("custom PartitionDuration not set")
	}
	if cfg.Storage.PartitionDuration != 2*time.Hour {
		t.Error("custom Storage.PartitionDuration not set")
	}
	if cfg.RetentionDuration != 24*time.Hour {
		t.Error("custom RetentionDuration not set")
	}
	if cfg.Retention.RetentionDuration != 24*time.Hour {
		t.Error("custom Retention.RetentionDuration not set")
	}
	if cfg.BufferSize != 5000 {
		t.Error("custom BufferSize not set")
	}
	if cfg.Storage.BufferSize != 5000 {
		t.Error("custom Storage.BufferSize not set")
	}
	if !cfg.HTTPEnabled {
		t.Error("custom HTTPEnabled not set")
	}
	if !cfg.HTTP.HTTPEnabled {
		t.Error("custom HTTP.HTTPEnabled not set")
	}
	if cfg.HTTPPort != 9000 {
		t.Error("custom HTTPPort not set")
	}
	if cfg.HTTP.HTTPPort != 9000 {
		t.Error("custom HTTP.HTTPPort not set")
	}
}

func TestConfig_EncryptionConfig(t *testing.T) {
	cfg := Config{
		Path: "/test",
		Encryption: &EncryptionConfig{
			Enabled:     true,
			KeyPassword: "secret",
		},
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.Encryption == nil {
		t.Fatal("Encryption config should not be nil")
	}
	if !cfg.Encryption.Enabled {
		t.Error("Encryption should be enabled")
	}
	if cfg.Encryption.KeyPassword != "secret" {
		t.Error("KeyPassword not set correctly")
	}
}

func TestConfig_ReplicationConfig(t *testing.T) {
	cfg := Config{
		Path: "/test",
		Replication: &ReplicationConfig{
			TargetURL:     "http://remote:8086",
			BatchSize:     100,
			FlushInterval: 5 * time.Second,
		},
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.Replication == nil {
		t.Fatal("Replication config should not be nil")
	}
	if cfg.Replication.TargetURL != "http://remote:8086" {
		t.Error("TargetURL not set correctly")
	}
	if cfg.Replication.BatchSize != 100 {
		t.Error("BatchSize not set correctly")
	}
}

func TestConfig_DownsampleRules(t *testing.T) {
	cfg := Config{
		Path: "/test",
		DownsampleRules: []DownsampleRule{
			{
				SourceResolution: time.Minute,
				TargetResolution: time.Hour,
				Aggregations:     []AggFunc{AggMean, AggMax},
			},
		},
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if len(cfg.DownsampleRules) != 1 {
		t.Fatalf("expected 1 downsample rule, got %d", len(cfg.DownsampleRules))
	}
	if len(cfg.Retention.DownsampleRules) != 1 {
		t.Fatalf("expected 1 retention downsample rule, got %d", len(cfg.Retention.DownsampleRules))
	}

	rule := cfg.DownsampleRules[0]
	if rule.SourceResolution != time.Minute {
		t.Error("SourceResolution not set correctly")
	}
	if rule.TargetResolution != time.Hour {
		t.Error("TargetResolution not set correctly")
	}
	if len(rule.Aggregations) != 2 {
		t.Error("Aggregations count not correct")
	}
}

func TestConfig_ContinuousQueries(t *testing.T) {
	cfg := Config{
		Path: "/test",
		ContinuousQueries: []ContinuousQuery{
			{
				Name:         "hourly_avg",
				SourceMetric: "cpu",
				Function:     AggMean,
				Window:       time.Hour,
				Every:        time.Hour,
				TargetMetric: "cpu_hourly",
			},
		},
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if len(cfg.ContinuousQueries) != 1 {
		t.Fatalf("expected 1 continuous query, got %d", len(cfg.ContinuousQueries))
	}

	cq := cfg.ContinuousQueries[0]
	if cq.Name != "hourly_avg" {
		t.Error("CQ name not set correctly")
	}
	if cq.Window != time.Hour {
		t.Error("CQ window not set correctly")
	}
}

func TestConfig_Schemas(t *testing.T) {
	cfg := Config{
		Path:         "/test",
		StrictSchema: true,
		Schemas: []MetricSchema{
			{
				Name: "cpu",
				Tags: []TagSchema{
					{Name: "host", Required: true},
				},
			},
		},
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if !cfg.StrictSchema {
		t.Error("StrictSchema should be true")
	}
	if len(cfg.Schemas) != 1 {
		t.Fatal("expected 1 schema")
	}
	if cfg.Schemas[0].Name != "cpu" {
		t.Error("Schema name not set correctly")
	}
}

func TestConfig_Auth(t *testing.T) {
	cfg := Config{
		Path: "/test",
		Auth: &AuthConfig{
			Enabled:      true,
			APIKeys:      []string{"key1", "key2"},
			ReadOnlyKeys: []string{"readonly"},
			ExcludePaths: []string{"/custom"},
		},
	}
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.Auth == nil {
		t.Fatal("Auth config should not be nil")
	}
	if !cfg.Auth.Enabled {
		t.Error("Auth should be enabled")
	}
	if len(cfg.Auth.APIKeys) != 2 {
		t.Errorf("expected 2 API keys, got %d", len(cfg.Auth.APIKeys))
	}
	if len(cfg.Auth.ReadOnlyKeys) != 1 {
		t.Errorf("expected 1 read-only key, got %d", len(cfg.Auth.ReadOnlyKeys))
	}
	if len(cfg.Auth.ExcludePaths) != 1 {
		t.Errorf("expected 1 exclude path, got %d", len(cfg.Auth.ExcludePaths))
	}
}

func TestConfig_RateLimitDefault(t *testing.T) {
	cfg := DefaultConfig("/test")
	cfg.normalize()
	cfg.syncLegacyFields()

	if cfg.RateLimitPerSecond != 1000 {
		t.Errorf("expected default RateLimitPerSecond 1000, got %d", cfg.RateLimitPerSecond)
	}
}

func TestConfig_RateLimitCustom(t *testing.T) {
	cfg := Config{
		Path:               "/test",
		RateLimitPerSecond: 500,
	}

	if cfg.RateLimitPerSecond != 500 {
		t.Errorf("expected RateLimitPerSecond 500, got %d", cfg.RateLimitPerSecond)
	}
}

func TestConfig_RateLimitDisabled(t *testing.T) {
	cfg := Config{
		Path:               "/test",
		RateLimitPerSecond: 0, // Disabled
	}

	if cfg.RateLimitPerSecond != 0 {
		t.Error("RateLimitPerSecond should be 0 when disabled")
	}
}

func TestAuthConfig_Defaults(t *testing.T) {
	// Nil auth config means disabled
	var cfg *AuthConfig
	if cfg != nil && cfg.Enabled {
		t.Error("nil AuthConfig should not be enabled")
	}

	// Empty struct also means disabled
	cfg = &AuthConfig{}
	if cfg.Enabled {
		t.Error("empty AuthConfig should not be enabled")
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	cfg := DefaultConfig("/test/path.db")
	if err := cfg.Validate(); err != nil {
		t.Errorf("DefaultConfig should be valid, got: %v", err)
	}
}

func TestConfig_Validate_NoPathOrBackend(t *testing.T) {
	cfg := Config{}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty config")
	}
	if !strings.Contains(err.Error(), "Path or StorageBackend") {
		t.Errorf("expected path/backend error, got: %v", err)
	}
}

func TestConfig_Validate_NegativeValues(t *testing.T) {
	cfg := DefaultConfig("/test/path.db")
	cfg.Storage.PartitionDuration = -time.Hour
	cfg.Storage.BufferSize = -1
	cfg.HTTP.HTTPPort = 99999

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation errors")
	}
	msg := err.Error()
	if !strings.Contains(msg, "PartitionDuration") {
		t.Error("expected PartitionDuration error")
	}
	if !strings.Contains(msg, "BufferSize") {
		t.Error("expected BufferSize error")
	}
	if !strings.Contains(msg, "HTTPPort") {
		t.Error("expected HTTPPort error")
	}
}

func TestConfig_Validate_AuthNoKeys(t *testing.T) {
	cfg := DefaultConfig("/test/path.db")
	cfg.Auth = &AuthConfig{Enabled: true}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected auth error")
	}
	if !strings.Contains(err.Error(), "APIKeys") {
		t.Errorf("expected APIKeys error, got: %v", err)
	}
}

func TestConfig_Validate_AuthWithKeys(t *testing.T) {
	cfg := DefaultConfig("/test/path.db")
	cfg.Auth = &AuthConfig{Enabled: true, APIKeys: []string{"key1"}}
	if err := cfg.Validate(); err != nil {
		t.Errorf("auth with keys should be valid, got: %v", err)
	}
}

func TestConfig_Validate_ZeroValues(t *testing.T) {
	cfg := DefaultConfig("/test/path.db")
	cfg.Storage.PartitionDuration = 0
	cfg.PartitionDuration = 0 // also clear legacy field to prevent normalize() from restoring
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error for zero PartitionDuration")
	}
	if !strings.Contains(err.Error(), "PartitionDuration") {
		t.Errorf("expected PartitionDuration error, got: %v", err)
	}

	cfg2 := DefaultConfig("/test/path.db")
	cfg2.Storage.BufferSize = 0
	cfg2.BufferSize = 0 // also clear legacy field
	err = cfg2.Validate()
	if err == nil {
		t.Fatal("expected validation error for zero BufferSize")
	}
	if !strings.Contains(err.Error(), "BufferSize") {
		t.Errorf("expected BufferSize error, got: %v", err)
	}
}

func TestConfig_Validate_LegacyFields(t *testing.T) {
	cfg := Config{
		Path:              "/test/path.db",
		PartitionDuration: time.Hour,
		BufferSize:        5000,
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("config with legacy fields should validate after normalization, got: %v", err)
	}
}

func TestConfig_Validate_RetentionShorterThanPartition(t *testing.T) {
	cfg := DefaultConfig("/test/path.db")
	cfg.Retention.RetentionDuration = 30 * time.Minute
	cfg.Storage.PartitionDuration = time.Hour
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error when RetentionDuration < PartitionDuration")
	}
	if !strings.Contains(err.Error(), "RetentionDuration") {
		t.Errorf("expected RetentionDuration error, got: %v", err)
	}
}

func TestConfig_Validate_CompactionExceedsPartition(t *testing.T) {
	cfg := DefaultConfig("/test/path.db")
	cfg.Retention.CompactionInterval = 2 * time.Hour
	cfg.Storage.PartitionDuration = time.Hour
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error when CompactionInterval > PartitionDuration")
	}
	if !strings.Contains(err.Error(), "CompactionInterval") {
		t.Errorf("expected CompactionInterval error, got: %v", err)
	}
}

func TestConfig_Validate_ReplicationNoURL(t *testing.T) {
	cfg := DefaultConfig("/test/path.db")
	cfg.Replication = &ReplicationConfig{Enabled: true}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error when replication enabled without TargetURL")
	}
	if !strings.Contains(err.Error(), "TargetURL") {
		t.Errorf("expected TargetURL error, got: %v", err)
	}
}

func TestConfig_Validate_EncryptionNoKey(t *testing.T) {
	cfg := DefaultConfig("/test/path.db")
	cfg.Encryption = &EncryptionConfig{Enabled: true}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error when encryption enabled without key")
	}
	if !strings.Contains(err.Error(), "Encryption") {
		t.Errorf("expected encryption error, got: %v", err)
	}
}

func TestConfig_Validate_EncryptionWithPassword(t *testing.T) {
	cfg := DefaultConfig("/test/path.db")
	cfg.Encryption = &EncryptionConfig{Enabled: true, KeyPassword: "secret"}
	if err := cfg.Validate(); err != nil {
		t.Errorf("encryption with password should be valid, got: %v", err)
	}
}

func TestConfig_DeprecationNormalization(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*Config)
		check func(*testing.T, *Config)
	}{
		{
			name: "MaxMemory migrates to Storage.MaxMemory",
			setup: func(c *Config) {
				c.MaxMemory = 128 * 1024 * 1024
				c.Storage.MaxMemory = 0
			},
			check: func(t *testing.T, c *Config) {
				if c.Storage.MaxMemory != 128*1024*1024 {
					t.Errorf("Storage.MaxMemory = %d, want %d", c.Storage.MaxMemory, 128*1024*1024)
				}
			},
		},
		{
			name: "MaxStorageBytes migrates to Storage.MaxStorageBytes",
			setup: func(c *Config) {
				c.MaxStorageBytes = 1 << 30
				c.Storage.MaxStorageBytes = 0
			},
			check: func(t *testing.T, c *Config) {
				if c.Storage.MaxStorageBytes != 1<<30 {
					t.Errorf("Storage.MaxStorageBytes = %d, want %d", c.Storage.MaxStorageBytes, 1<<30)
				}
			},
		},
		{
			name: "PartitionDuration migrates to Storage.PartitionDuration",
			setup: func(c *Config) {
				c.PartitionDuration = 2 * time.Hour
				c.Storage.PartitionDuration = 0
			},
			check: func(t *testing.T, c *Config) {
				if c.Storage.PartitionDuration != 2*time.Hour {
					t.Errorf("Storage.PartitionDuration = %v, want %v", c.Storage.PartitionDuration, 2*time.Hour)
				}
			},
		},
		{
			name: "BufferSize migrates to Storage.BufferSize",
			setup: func(c *Config) {
				c.BufferSize = 5000
				c.Storage.BufferSize = 0
			},
			check: func(t *testing.T, c *Config) {
				if c.Storage.BufferSize != 5000 {
					t.Errorf("Storage.BufferSize = %d, want %d", c.Storage.BufferSize, 5000)
				}
			},
		},
		{
			name: "SyncInterval migrates to WAL.SyncInterval",
			setup: func(c *Config) {
				c.SyncInterval = 5 * time.Second
				c.WAL.SyncInterval = 0
			},
			check: func(t *testing.T, c *Config) {
				if c.WAL.SyncInterval != 5*time.Second {
					t.Errorf("WAL.SyncInterval = %v, want %v", c.WAL.SyncInterval, 5*time.Second)
				}
			},
		},
		{
			name: "WALMaxSize migrates to WAL.WALMaxSize",
			setup: func(c *Config) {
				c.WALMaxSize = 256 * 1024 * 1024
				c.WAL.WALMaxSize = 0
			},
			check: func(t *testing.T, c *Config) {
				if c.WAL.WALMaxSize != 256*1024*1024 {
					t.Errorf("WAL.WALMaxSize = %d, want %d", c.WAL.WALMaxSize, 256*1024*1024)
				}
			},
		},
		{
			name: "WALRetain migrates to WAL.WALRetain",
			setup: func(c *Config) {
				c.WALRetain = 5
				c.WAL.WALRetain = 0
			},
			check: func(t *testing.T, c *Config) {
				if c.WAL.WALRetain != 5 {
					t.Errorf("WAL.WALRetain = %d, want %d", c.WAL.WALRetain, 5)
				}
			},
		},
		{
			name: "RetentionDuration migrates to Retention.RetentionDuration",
			setup: func(c *Config) {
				c.RetentionDuration = 24 * time.Hour
				c.Retention.RetentionDuration = 0
			},
			check: func(t *testing.T, c *Config) {
				if c.Retention.RetentionDuration != 24*time.Hour {
					t.Errorf("Retention.RetentionDuration = %v, want %v", c.Retention.RetentionDuration, 24*time.Hour)
				}
			},
		},
		{
			name: "DownsampleRules migrates to Retention.DownsampleRules",
			setup: func(c *Config) {
				c.DownsampleRules = []DownsampleRule{{SourceResolution: time.Minute}}
				c.Retention.DownsampleRules = nil
			},
			check: func(t *testing.T, c *Config) {
				if len(c.Retention.DownsampleRules) != 1 {
					t.Errorf("Retention.DownsampleRules len = %d, want 1", len(c.Retention.DownsampleRules))
				}
			},
		},
		{
			name: "CompactionWorkers migrates to Retention.CompactionWorkers",
			setup: func(c *Config) {
				c.CompactionWorkers = 4
				c.Retention.CompactionWorkers = 0
			},
			check: func(t *testing.T, c *Config) {
				if c.Retention.CompactionWorkers != 4 {
					t.Errorf("Retention.CompactionWorkers = %d, want %d", c.Retention.CompactionWorkers, 4)
				}
			},
		},
		{
			name: "CompactionInterval migrates to Retention.CompactionInterval",
			setup: func(c *Config) {
				c.CompactionInterval = 15 * time.Minute
				c.Retention.CompactionInterval = 0
			},
			check: func(t *testing.T, c *Config) {
				if c.Retention.CompactionInterval != 15*time.Minute {
					t.Errorf("Retention.CompactionInterval = %v, want %v", c.Retention.CompactionInterval, 15*time.Minute)
				}
			},
		},
		{
			name: "QueryTimeout migrates to Query.QueryTimeout",
			setup: func(c *Config) {
				c.QueryTimeout = time.Minute
				c.Query.QueryTimeout = 0
			},
			check: func(t *testing.T, c *Config) {
				if c.Query.QueryTimeout != time.Minute {
					t.Errorf("Query.QueryTimeout = %v, want %v", c.Query.QueryTimeout, time.Minute)
				}
			},
		},
		{
			name: "HTTPEnabled migrates to HTTP.HTTPEnabled",
			setup: func(c *Config) {
				c.HTTPEnabled = true
				c.HTTP.HTTPEnabled = false
			},
			check: func(t *testing.T, c *Config) {
				if !c.HTTP.HTTPEnabled {
					t.Error("HTTP.HTTPEnabled should be true after normalization")
				}
			},
		},
		{
			name: "HTTPPort migrates to HTTP.HTTPPort",
			setup: func(c *Config) {
				c.HTTPPort = 9090
				c.HTTP.HTTPPort = 0
			},
			check: func(t *testing.T, c *Config) {
				if c.HTTP.HTTPPort != 9090 {
					t.Errorf("HTTP.HTTPPort = %d, want %d", c.HTTP.HTTPPort, 9090)
				}
			},
		},
		{
			name: "PrometheusRemoteWriteEnabled migrates to HTTP.PrometheusRemoteWriteEnabled",
			setup: func(c *Config) {
				c.PrometheusRemoteWriteEnabled = true
				c.HTTP.PrometheusRemoteWriteEnabled = false
			},
			check: func(t *testing.T, c *Config) {
				if !c.HTTP.PrometheusRemoteWriteEnabled {
					t.Error("HTTP.PrometheusRemoteWriteEnabled should be true after normalization")
				}
			},
		},
		{
			name: "new field set takes precedence over deprecated",
			setup: func(c *Config) {
				c.MaxMemory = 64 * 1024 * 1024
				c.Storage.MaxMemory = 256 * 1024 * 1024
			},
			check: func(t *testing.T, c *Config) {
				if c.Storage.MaxMemory != 256*1024*1024 {
					t.Errorf("Storage.MaxMemory = %d, want %d (new field should take precedence)", c.Storage.MaxMemory, 256*1024*1024)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Path: "/test/path.db",
				Storage: StorageConfig{
					PartitionDuration: time.Hour,
					BufferSize:        10_000,
				},
			}
			tt.setup(&cfg)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			tt.check(t, &cfg)
		})
	}
}

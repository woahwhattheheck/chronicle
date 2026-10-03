package chprotocol

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

// Protocol constants
const (
	CHClientHello        uint64 = 0
	CHClientQuery        uint64 = 1
	CHClientData         uint64 = 2
	CHClientCancel       uint64 = 3
	CHClientPing         uint64 = 4
	CHClientTablesStatus uint64 = 5

	CHServerHello       uint64 = 0
	CHServerData        uint64 = 1
	CHServerException   uint64 = 2
	CHServerProgress    uint64 = 3
	CHServerPong        uint64 = 4
	CHServerEndOfStream uint64 = 5
	CHServerProfileInfo uint64 = 6
	CHServerTotals      uint64 = 7
	CHServerExtremes    uint64 = 8
	CHServerTableStatus uint64 = 9

	CHProtocolVersionMajor = 21
	CHProtocolVersionMinor = 1
	CHProtocolRevision     = 54447

	DefaultCHPort = 9000
)

// ClickHouse data types
const (
	CHTypeUInt8    = "UInt8"
	CHTypeUInt16   = "UInt16"
	CHTypeUInt32   = "UInt32"
	CHTypeUInt64   = "UInt64"
	CHTypeInt8     = "Int8"
	CHTypeInt16    = "Int16"
	CHTypeInt32    = "Int32"
	CHTypeInt64    = "Int64"
	CHTypeFloat32  = "Float32"
	CHTypeFloat64  = "Float64"
	CHTypeString   = "String"
	CHTypeDateTime = "DateTime"
	CHTypeDate     = "Date"
)

// ClickHouseProtocolConfig configures the ClickHouse protocol server
type ClickHouseProtocolConfig struct {
	// Address to listen on (default: ":9000")
	Address string

	// Database name presented to clients
	DatabaseName string

	// Server display name
	ServerName string

	// Server timezone
	Timezone string

	// Maximum concurrent connections
	MaxConnections int

	// chronicle.Query timeout
	QueryTimeout time.Duration

	// Enable query logging
	QueryLogging bool

	// Enable compression (LZ4)
	EnableCompression bool

	// Read buffer size
	ReadBufferSize int

	// Write buffer size
	WriteBufferSize int
}

// DefaultClickHouseProtocolConfig returns default configuration
func DefaultClickHouseProtocolConfig() *ClickHouseProtocolConfig {
	return &ClickHouseProtocolConfig{
		Address:           ":9000",
		DatabaseName:      "chronicle",
		ServerName:        "Chronicle",
		Timezone:          "UTC",
		MaxConnections:    100,
		QueryTimeout:      30 * time.Second,
		QueryLogging:      true,
		EnableCompression: false,
		ReadBufferSize:    64 * 1024,
		WriteBufferSize:   64 * 1024,
	}
}

// CHNativeServer implements the ClickHouse native protocol
type CHNativeServer struct {
	db     *chronicle.DB
	config *ClickHouseProtocolConfig

	listener net.Listener
	sessions sync.Map // connectionID -> *CHSession

	mu       sync.RWMutex
	running  bool
	shutdown chan struct{}

	// Typed atomics keep the counters aligned on 32-bit platforms.
	totalConnections atomic.Int64
	activeConns      atomic.Int64
	totalQueries     atomic.Int64
	queryErrors      atomic.Int64
}

// NewCHNativeServer creates a new ClickHouse protocol server
func NewCHNativeServer(db *chronicle.DB, config *ClickHouseProtocolConfig) (*CHNativeServer, error) {
	if config == nil {
		config = DefaultClickHouseProtocolConfig()
	}

	return &CHNativeServer{
		db:       db,
		config:   config,
		shutdown: make(chan struct{}),
	}, nil
}

// CHServerStats contains server statistics
type CHServerStats struct {
	TotalConnections  int64
	ActiveConnections int64
	TotalQueries      int64
	QueryErrors       int64
}

// CHSession represents a client connection session
type CHSession struct {
	id     string
	server *CHNativeServer
	conn   net.Conn
	reader *bytes.Buffer
	writer *bytes.Buffer

	clientName string
	clientVer  uint64
	clientRev  uint64
	database   string

	ctx    context.Context
	cancel context.CancelFunc

	mu sync.Mutex
}

func newCHSession(server *CHNativeServer, conn net.Conn) *CHSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &CHSession{
		id:     fmt.Sprintf("%d", time.Now().UnixNano()),
		server: server,
		conn:   conn,
		reader: bytes.NewBuffer(make([]byte, 0, server.config.ReadBufferSize)),
		writer: bytes.NewBuffer(make([]byte, 0, server.config.WriteBufferSize)),
		ctx:    ctx,
		cancel: cancel,
	}
}

// CHQueryResult represents a query result
type CHQueryResult struct {
	Columns  []CHColumn
	Rows     [][]any
	RowCount int
}

// CHColumn represents a result column
type CHColumn struct {
	Name string
	Type string
}

// CHQueryTranslator translates ClickHouse SQL to Chronicle queries
type CHQueryTranslator struct {
	db *chronicle.DB
}

type parsedSelect struct {
	columns    []string
	table      string
	where      string
	startTime  int64
	endTime    int64
	groupBy    string
	orderBy    string
	orderDesc  bool
	limit      int
	aggregates map[string]string // column -> aggregate function
}

// Conversion helpers
func toUint64(v any) (uint64, error) {
	switch val := v.(type) {
	case uint64:
		return val, nil
	case uint32:
		return uint64(val), nil
	case uint16:
		return uint64(val), nil
	case uint8:
		return uint64(val), nil
	case int64:
		return uint64(val), nil
	case int32:
		return uint64(val), nil
	case int:
		return uint64(val), nil
	case float64:
		return uint64(val), nil
	case float32:
		return uint64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", v)
	}
}

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int32:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int:
		return int64(val), nil
	case uint64:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case float32:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case uint64:
		return float64(val), nil
	case uint32:
		return float64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

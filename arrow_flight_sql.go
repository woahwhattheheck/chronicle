package chronicle

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

// FlightSQLCommand represents Arrow Flight SQL command types.
type FlightSQLCommand int

const (
	// CommandGetCatalogs requests the list of available catalogs.
	CommandGetCatalogs FlightSQLCommand = iota + 100
	// CommandGetSchemas requests the list of schemas in a catalog.
	CommandGetSchemas
	// CommandGetTables requests the list of tables in a schema.
	CommandGetTables
	// CommandGetTableTypes requests the list of table types.
	CommandGetTableTypes
	// CommandStatementQuery executes a SQL statement that returns results.
	CommandStatementQuery
	// CommandStatementUpdate executes a SQL statement that modifies data.
	CommandStatementUpdate
	// CommandPreparedStatementQuery executes a prepared SQL statement.
	CommandPreparedStatementQuery
)

// FlightSQLConfig holds configuration for the Flight SQL server.
type FlightSQLConfig struct {
	BindAddr             string        `json:"bind_addr"`
	MaxBatchRows         int           `json:"max_batch_rows"`
	MaxConcurrentStreams int           `json:"max_concurrent_streams"`
	EnableSQL            bool          `json:"enable_sql"`
	Catalogs             []string      `json:"catalogs"`
	Schemas              []string      `json:"schemas"`
	SQLTimeout           time.Duration `json:"sql_timeout"`
}

// DefaultFlightSQLConfig returns a default Flight SQL configuration.
func DefaultFlightSQLConfig() FlightSQLConfig {
	return FlightSQLConfig{
		BindAddr:             "127.0.0.1:8816",
		MaxBatchRows:         65536,
		MaxConcurrentStreams: 64,
		EnableSQL:            true,
		Catalogs:             []string{"chronicle"},
		Schemas:              []string{"default"},
		SQLTimeout:           30 * time.Second,
	}
}

// FlightSQLServer implements the Arrow Flight SQL protocol for BI tool connectivity.
type FlightSQLServer struct {
	db       *DB
	config   FlightSQLConfig
	mu       sync.RWMutex
	listener net.Listener
	running  bool
	stopCh   chan struct{}
	wg       sync.WaitGroup

	// Session and prepared statement management.
	sessions           map[string]*flightSQLSession
	preparedStatements map[string]*preparedStatement
	sessionMu          sync.RWMutex

	// Concurrency limiter.
	streamSem chan struct{}
}

type flightSQLSession struct {
	id        string
	createdAt time.Time
	lastUsed  time.Time
}

type preparedStatement struct {
	id      string
	sql     string
	schema  ArrowSchema
	created time.Time
}

// Flight SQL wire protocol message types (offset from base Flight types).
const (
	flightSQLMessageGetCatalogs     uint32 = 100
	flightSQLMessageGetSchemas      uint32 = 101
	flightSQLMessageGetTables       uint32 = 102
	flightSQLMessageGetTableTypes   uint32 = 103
	flightSQLMessageStatementQuery  uint32 = 104
	flightSQLMessageStatementUpdate uint32 = 105
	flightSQLMessagePreparedQuery   uint32 = 106
	flightSQLMessageGetFlightInfo   uint32 = 107
	flightSQLMessageDoGet           uint32 = 108
	flightSQLMessageDoPut           uint32 = 109
	flightSQLMessageResponse        uint32 = 200
	flightSQLMessageError           uint32 = 255
)

// NewFlightSQLServer creates a new Flight SQL server.
func NewFlightSQLServer(db *DB, config FlightSQLConfig) *FlightSQLServer {
	return &FlightSQLServer{
		db:                 db,
		config:             config,
		stopCh:             make(chan struct{}),
		sessions:           make(map[string]*flightSQLSession),
		preparedStatements: make(map[string]*preparedStatement),
		streamSem:          make(chan struct{}, config.MaxConcurrentStreams),
	}
}

// Start starts the Flight SQL server.
func (s *FlightSQLServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("flight sql server already running")
	}

	listener, err := net.Listen("tcp", s.config.BindAddr)
	if err != nil {
		return fmt.Errorf("failed to bind Flight SQL server: %w", err)
	}
	s.listener = listener
	s.running = true
	s.stopCh = make(chan struct{})

	s.wg.Add(1)
	go s.serve()

	slog.Info("Flight SQL server started", "addr", s.config.BindAddr)
	return nil
}

// Stop stops the Flight SQL server.
func (s *FlightSQLServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	close(s.stopCh)
	if s.listener != nil {
		closeQuietly(s.listener)
	}
	s.wg.Wait()
	s.running = false

	slog.Info("Flight SQL server stopped")
	return nil
}

func (s *FlightSQLServer) serve() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				slog.Warn("Flight SQL accept error", "err", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *FlightSQLServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		// Read 8-byte header: [4 bytes msgType][4 bytes msgLen]
		header := make([]byte, 8)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}

		msgType := binary.BigEndian.Uint32(header[:4])
		msgLen := binary.BigEndian.Uint32(header[4:])

		if msgLen > 64*1024*1024 {
			s.sendErrorMsg(conn, fmt.Errorf("message too large: %d bytes", msgLen))
			return
		}

		body := make([]byte, msgLen)
		if msgLen > 0 {
			if _, err := io.ReadFull(conn, body); err != nil {
				return
			}
		}

		// Acquire stream semaphore for concurrency control.
		select {
		case s.streamSem <- struct{}{}:
		case <-s.stopCh:
			return
		}

		s.dispatchMessage(conn, msgType, body)
		<-s.streamSem
	}
}

func (s *FlightSQLServer) dispatchMessage(conn net.Conn, msgType uint32, body []byte) {
	switch msgType {
	case flightSQLMessageGetCatalogs:
		s.handleGetCatalogsMsg(conn)
	case flightSQLMessageGetSchemas:
		s.handleGetSchemasMsg(conn, body)
	case flightSQLMessageGetTables:
		s.handleGetTablesMsg(conn, body)
	case flightSQLMessageGetTableTypes:
		s.handleGetTableTypesMsg(conn)
	case flightSQLMessageStatementQuery:
		s.handleStatementQueryMsg(conn, body)
	case flightSQLMessageStatementUpdate:
		s.handleStatementUpdateMsg(conn, body)
	case flightSQLMessagePreparedQuery:
		s.handlePreparedQueryMsg(conn, body)
	case flightSQLMessageGetFlightInfo:
		s.handleGetFlightInfoMsg(conn, body)
	case flightSQLMessageDoGet:
		s.handleDoGetMsg(conn, body)
	case flightSQLMessageDoPut:
		s.handleDoPutMsg(conn, body)
	default:
		s.sendErrorMsg(conn, fmt.Errorf("unknown Flight SQL message type: %d", msgType))
	}
}

// --- Flight SQL Handlers ---

// HandleGetFlightInfo returns FlightInfo for a given descriptor.
func (s *FlightSQLServer) HandleGetFlightInfo(descriptor FlightDescriptor) (*FlightInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var query FlightQuery
	switch descriptor.Type {
	case FlightDescriptorCmd:
		if err := json.Unmarshal(descriptor.Cmd, &query); err != nil {
			return nil, fmt.Errorf("invalid flight query command: %w", err)
		}
	case FlightDescriptorPath:
		if len(descriptor.Path) == 0 {
			return nil, fmt.Errorf("empty descriptor path")
		}
		query.Metric = descriptor.Path[0]
		query.Start = time.Now().Add(-time.Hour).UnixNano()
		query.End = time.Now().UnixNano()
	default:
		return nil, fmt.Errorf("unsupported descriptor type: %d", descriptor.Type)
	}

	ticketData, _ := json.Marshal(descriptor)
	info := &FlightInfo{
		Schema: ChronicleSchema(),
		Endpoints: []FlightEndpoint{
			{
				Ticket:    FlightTicket{Ticket: ticketData},
				Locations: []FlightLocation{{URI: "grpc://" + s.config.BindAddr}},
			},
		},
		TotalRows:  -1,
		TotalBytes: -1,
	}
	return info, nil
}

func (s *FlightSQLServer) handleGetFlightInfoMsg(conn net.Conn, body []byte) {
	var desc FlightDescriptor
	if err := json.Unmarshal(body, &desc); err != nil {
		s.sendErrorMsg(conn, err)
		return
	}
	info, err := s.HandleGetFlightInfo(desc)
	if err != nil {
		s.sendErrorMsg(conn, err)
		return
	}
	s.sendResponseMsg(conn, flightSQLMessageResponse, info)
}

// HandleDoGet streams record batches for a given ticket.
func (s *FlightSQLServer) HandleDoGet(ticket FlightTicket) ([]ArrowRecordBatch, error) {
	var desc FlightDescriptor
	if err := json.Unmarshal(ticket.Ticket, &desc); err != nil {
		return nil, fmt.Errorf("invalid ticket: %w", err)
	}

	var query FlightQuery
	switch desc.Type {
	case FlightDescriptorCmd:
		if err := json.Unmarshal(desc.Cmd, &query); err != nil {
			return nil, fmt.Errorf("invalid query in ticket: %w", err)
		}
	case FlightDescriptorPath:
		if len(desc.Path) > 0 {
			query.Metric = desc.Path[0]
			query.Start = time.Now().Add(-time.Hour).UnixNano()
			query.End = time.Now().UnixNano()
		}
	}

	chronicleQuery := flightQueryToChronicle(query)
	ctx, cancel := context.WithTimeout(context.Background(), s.config.SQLTimeout)
	defer cancel()

	result, err := s.db.ExecuteContext(ctx, chronicleQuery)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	return buildBatches(result.Points, s.config.MaxBatchRows), nil
}

func (s *FlightSQLServer) handleDoGetMsg(conn net.Conn, body []byte) {
	var ticket FlightTicket
	if err := json.Unmarshal(body, &ticket); err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	batches, err := s.HandleDoGet(ticket)
	if err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	for _, batch := range batches {
		if err := s.sendResponseMsg(conn, flightSQLMessageDoGet, batch); err != nil {
			return
		}
	}
	_ = s.sendResponseMsg(conn, flightSQLMessageDoGet, map[string]any{"complete": true}) //nolint:errcheck // final completion marker, no recovery action
}

// HandleDoPut ingests record batches into the database.
func (s *FlightSQLServer) HandleDoPut(schema ArrowSchema, batches []ArrowRecordBatch) (int, error) {
	totalWritten := 0
	for _, batch := range batches {
		points, err := recordBatchToPoints(batch)
		if err != nil {
			return totalWritten, fmt.Errorf("batch conversion failed: %w", err)
		}
		if err := s.db.WriteBatch(points); err != nil {
			return totalWritten, fmt.Errorf("write failed: %w", err)
		}
		totalWritten += len(points)
	}
	return totalWritten, nil
}

func (s *FlightSQLServer) handleDoPutMsg(conn net.Conn, body []byte) {
	var req struct {
		Schema  ArrowSchema        `json:"schema"`
		Batches []ArrowRecordBatch `json:"batches"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	written, err := s.HandleDoPut(req.Schema, req.Batches)
	if err != nil {
		s.sendErrorMsg(conn, err)
		return
	}
	s.sendResponseMsg(conn, flightSQLMessageResponse, map[string]any{"rows_written": written})
}

// HandleGetCatalogs returns available catalogs.
func (s *FlightSQLServer) HandleGetCatalogs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	catalogs := make([]string, len(s.config.Catalogs))
	copy(catalogs, s.config.Catalogs)
	return catalogs
}

func (s *FlightSQLServer) handleGetCatalogsMsg(conn net.Conn) {
	catalogs := s.HandleGetCatalogs()
	batch := catalogsToRecordBatch(catalogs)
	s.sendResponseMsg(conn, flightSQLMessageResponse, batch)
}

// HandleGetSchemas returns schemas for a catalog.
func (s *FlightSQLServer) HandleGetSchemas(catalog string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, c := range s.config.Catalogs {
		if c == catalog || catalog == "" {
			schemas := make([]string, len(s.config.Schemas))
			copy(schemas, s.config.Schemas)
			return schemas
		}
	}
	return nil
}

func (s *FlightSQLServer) handleGetSchemasMsg(conn net.Conn, body []byte) {
	var req struct {
		Catalog string `json:"catalog"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req) //nolint:errcheck // optional request body; defaults used on parse failure
	}
	schemas := s.HandleGetSchemas(req.Catalog)
	batch := schemasToRecordBatch(req.Catalog, schemas)
	s.sendResponseMsg(conn, flightSQLMessageResponse, batch)
}

// HandleGetTables returns tables (metrics) for a given catalog and schema.
func (s *FlightSQLServer) HandleGetTables(catalog, schema string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	found := false
	for _, c := range s.config.Catalogs {
		if c == catalog || catalog == "" {
			found = true
			break
		}
	}
	if !found {
		return nil
	}

	return s.db.Metrics()
}

func (s *FlightSQLServer) handleGetTablesMsg(conn net.Conn, body []byte) {
	var req struct {
		Catalog string `json:"catalog"`
		Schema  string `json:"schema"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req) //nolint:errcheck // optional request body; defaults used on parse failure
	}
	tables := s.HandleGetTables(req.Catalog, req.Schema)
	batch := tablesToRecordBatch(req.Catalog, req.Schema, tables)
	s.sendResponseMsg(conn, flightSQLMessageResponse, batch)
}

func (s *FlightSQLServer) handleGetTableTypesMsg(conn net.Conn) {
	batch := ArrowRecordBatch{
		Schema: ArrowSchema{
			Fields: []ArrowField{
				{Name: "table_type", Type: ArrowTypeString, Nullable: false},
			},
		},
		Columns: []ArrowColumn{
			{Name: "table_type", Type: ArrowTypeString, Data: []any{"TABLE"}},
		},
		Length: 1,
	}
	s.sendResponseMsg(conn, flightSQLMessageResponse, batch)
}

// HandleStatementQuery executes a SQL query and returns results as record batches.
func (s *FlightSQLServer) HandleStatementQuery(sql string) ([]ArrowRecordBatch, error) {
	if !s.config.EnableSQL {
		return nil, fmt.Errorf("SQL queries are disabled")
	}

	translator := &SQLToQueryTranslator{}
	query, err := translator.Translate(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL translation failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.SQLTimeout)
	defer cancel()

	result, err := s.db.ExecuteContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	return buildBatches(result.Points, s.config.MaxBatchRows), nil
}

func (s *FlightSQLServer) handleStatementQueryMsg(conn net.Conn, body []byte) {
	var req struct {
		SQL string `json:"sql"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	batches, err := s.HandleStatementQuery(req.SQL)
	if err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	for _, batch := range batches {
		if err := s.sendResponseMsg(conn, flightSQLMessageDoGet, batch); err != nil {
			return
		}
	}
	//nolint:errcheck // final completion marker, no recovery action
	_ = s.sendResponseMsg(conn, flightSQLMessageResponse, map[string]any{
		"complete": true,
		"batches":  len(batches),
	})
}

func (s *FlightSQLServer) handleStatementUpdateMsg(conn net.Conn, body []byte) {
	var req struct {
		SQL string `json:"sql"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.sendErrorMsg(conn, err)
		return
	}
	// Chronicle is append-only; updates are not supported.
	s.sendErrorMsg(conn, fmt.Errorf("statement updates are not supported on a time-series database"))
}

// --- Standard Flight SQL RPC Methods for Client Interop ---

// GetFlightInfoStatement returns FlightInfo for a SQL statement (DBeaver, ADBC, DataFusion).
func (s *FlightSQLServer) GetFlightInfoStatement(sql string) (*FlightInfo, error) {
	if !s.config.EnableSQL {
		return nil, fmt.Errorf("SQL queries are disabled")
	}
	if sql == "" {
		return nil, fmt.Errorf("empty SQL statement")
	}

	// Store the prepared statement for later DoGet
	s.sessionMu.Lock()
	ticketID := s.nextStatementIDLocked("stmt")
	s.preparedStatements[ticketID] = &preparedStatement{
		id:      ticketID,
		sql:     sql,
		schema:  ChronicleSchema(),
		created: time.Now(),
	}
	s.sessionMu.Unlock()

	return &FlightInfo{
		Schema: ChronicleSchema(),
		Endpoints: []FlightEndpoint{{
			Ticket:    FlightTicket{Ticket: []byte(ticketID)},
			Locations: []FlightLocation{{URI: s.config.BindAddr}},
		}},
		TotalRows:  -1, // unknown until executed
		TotalBytes: -1,
	}, nil
}

// DoGetStatement executes a SQL statement and returns results as Arrow batches.
func (s *FlightSQLServer) DoGetStatement(ticketID string) ([]ArrowRecordBatch, error) {
	s.sessionMu.RLock()
	ps, exists := s.preparedStatements[ticketID]
	s.sessionMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unknown ticket: %s", ticketID)
	}

	return s.HandleStatementQuery(ps.sql)
}

// CreatePreparedStatement creates a server-side prepared statement.
func (s *FlightSQLServer) CreatePreparedStatement(sql string) (string, *ArrowSchema, error) {
	if !s.config.EnableSQL {
		return "", nil, fmt.Errorf("SQL queries are disabled")
	}

	schema := ChronicleSchema()

	s.sessionMu.Lock()
	stmtID := s.nextStatementIDLocked("prepared")
	s.preparedStatements[stmtID] = &preparedStatement{
		id:      stmtID,
		sql:     sql,
		schema:  schema,
		created: time.Now(),
	}
	s.sessionMu.Unlock()

	return stmtID, &schema, nil
}

// nextStatementIDLocked allocates an opaque ID while sessionMu is held.
// Random IDs avoid clock and server-restart aliases; existing wire IDs stay live.
func (s *FlightSQLServer) nextStatementIDLocked(prefix string) string {
	for {
		id := prefix + "-" + rand.Text()
		if _, exists := s.preparedStatements[id]; !exists {
			return id
		}
	}
}

// ClosePreparedStatement closes a previously created prepared statement.
func (s *FlightSQLServer) ClosePreparedStatement(stmtID string) error {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	if _, exists := s.preparedStatements[stmtID]; !exists {
		return fmt.Errorf("prepared statement not found: %s", stmtID)
	}
	delete(s.preparedStatements, stmtID)
	return nil
}

// GetSqlInfo returns SQL information for client capability detection.
func (s *FlightSQLServer) GetSqlInfo() map[string]any {
	return map[string]any{
		"flight_sql_server_name":          "Chronicle Flight SQL",
		"flight_sql_server_version":       "1.0.0",
		"flight_sql_server_arrow_version": "15.0.0",
		"flight_sql_server_read_only":     false,
		"sql_ddl_catalog":                 false,
		"sql_ddl_schema":                  false,
		"sql_ddl_table":                   false,
		"sql_identifier_case":             "case_insensitive",
		"sql_identifier_quote_char":       "\"",
		"sql_quoted_identifier_case":      "case_sensitive",
		"sql_all_tables_are_selectable":   true,
	}
}

func (s *FlightSQLServer) handlePreparedQueryMsg(conn net.Conn, body []byte) {
	var req struct {
		StatementID string `json:"statement_id"`
		SQL         string `json:"sql"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	// Create or retrieve prepared statement.
	s.sessionMu.Lock()
	ps, exists := s.preparedStatements[req.StatementID]
	if !exists && req.SQL != "" {
		ps = &preparedStatement{
			id:      req.StatementID,
			sql:     req.SQL,
			schema:  ChronicleSchema(),
			created: time.Now(),
		}
		s.preparedStatements[req.StatementID] = ps
	}
	s.sessionMu.Unlock()

	if ps == nil {
		s.sendErrorMsg(conn, fmt.Errorf("prepared statement not found: %s", req.StatementID))
		return
	}

	batches, err := s.HandleStatementQuery(ps.sql)
	if err != nil {
		s.sendErrorMsg(conn, err)
		return
	}

	for _, batch := range batches {
		if err := s.sendResponseMsg(conn, flightSQLMessageDoGet, batch); err != nil {
			return
		}
	}
	//nolint:errcheck // final completion marker, no recovery action
	_ = s.sendResponseMsg(conn, flightSQLMessageResponse, map[string]any{
		"complete":     true,
		"statement_id": ps.id,
	})
}

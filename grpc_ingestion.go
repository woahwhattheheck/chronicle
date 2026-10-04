package chronicle

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"
)

// GRPCIngestionConfig configures the gRPC ingestion server.
type GRPCIngestionConfig struct {
	Enabled          bool
	ListenAddr       string
	MaxMessageSize   int
	MaxConcurrent    int
	WriteTimeout     time.Duration
	QueryTimeout     time.Duration
	EnableReflection bool
	TLSCertFile      string
	TLSKeyFile       string
	EnableProtobuf   bool // enable native protobuf wire format
}

// DefaultGRPCIngestionConfig returns sensible defaults.
func DefaultGRPCIngestionConfig() GRPCIngestionConfig {
	return GRPCIngestionConfig{
		Enabled:          true,
		ListenAddr:       ":9095",
		MaxMessageSize:   16 * 1024 * 1024, // 16MB
		MaxConcurrent:    100,
		WriteTimeout:     10 * time.Second,
		QueryTimeout:     30 * time.Second,
		EnableReflection: true,
		EnableProtobuf:   true,
	}
}

// --- Protobuf wire format encoder/decoder ---
// Implements protobuf binary encoding without external proto dependencies.

// protoWireType represents protobuf wire types.
type protoWireType int

const (
	protoWireVarint protoWireType = 0
	protoWire64Bit  protoWireType = 1
	protoWireBytes  protoWireType = 2
	protoWire32Bit  protoWireType = 5
)

// protoField represents a decoded protobuf field.
type protoField struct {
	Number   int
	WireType protoWireType
	Varint   uint64
	Fixed64  uint64
	Fixed32  uint32
	Bytes    []byte
}

// protoDecoder decodes protobuf wire format bytes.
type protoDecoder struct {
	buf []byte
	pos int
}

func newProtoDecoder(data []byte) *protoDecoder {
	return &protoDecoder{buf: data}
}

func (d *protoDecoder) hasMore() bool {
	return d.pos < len(d.buf)
}

func (d *protoDecoder) readVarint() (uint64, error) {
	var val uint64
	var shift uint
	for d.pos < len(d.buf) {
		b := d.buf[d.pos]
		d.pos++
		val |= uint64(b&0x7F) << shift
		if b < 0x80 {
			return val, nil
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("varint overflow")
		}
	}
	return 0, io.ErrUnexpectedEOF
}

func (d *protoDecoder) readFixed64() (uint64, error) {
	if d.pos+8 > len(d.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	val := binary.LittleEndian.Uint64(d.buf[d.pos : d.pos+8])
	d.pos += 8
	return val, nil
}

func (d *protoDecoder) readFixed32() (uint32, error) {
	if d.pos+4 > len(d.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	val := binary.LittleEndian.Uint32(d.buf[d.pos : d.pos+4])
	d.pos += 4
	return val, nil
}

func (d *protoDecoder) readBytes() ([]byte, error) {
	length, err := d.readVarint()
	if err != nil {
		return nil, err
	}
	if d.pos+int(length) > len(d.buf) {
		return nil, io.ErrUnexpectedEOF
	}
	data := d.buf[d.pos : d.pos+int(length)]
	d.pos += int(length)
	return data, nil
}

func (d *protoDecoder) readField() (*protoField, error) {
	tag, err := d.readVarint()
	if err != nil {
		return nil, err
	}
	field := &protoField{
		Number:   int(tag >> 3),
		WireType: protoWireType(tag & 0x7),
	}
	switch field.WireType {
	case protoWireVarint:
		field.Varint, err = d.readVarint()
	case protoWire64Bit:
		field.Fixed64, err = d.readFixed64()
	case protoWireBytes:
		field.Bytes, err = d.readBytes()
	case protoWire32Bit:
		field.Fixed32, err = d.readFixed32()
	default:
		return nil, fmt.Errorf("unknown wire type: %d", field.WireType)
	}
	return field, err
}

// protoEncoder encodes protobuf wire format bytes.
type protoEncoder struct {
	buf []byte
}

func newProtoEncoder() *protoEncoder {
	return &protoEncoder{}
}

func (e *protoEncoder) writeVarint(fieldNum int, val uint64) {
	tag := uint64(fieldNum<<3) | uint64(protoWireVarint)
	e.appendVarint(tag)
	e.appendVarint(val)
}

func (e *protoEncoder) writeFixed64(fieldNum int, val uint64) {
	tag := uint64(fieldNum<<3) | uint64(protoWire64Bit)
	e.appendVarint(tag)
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	e.buf = append(e.buf, b...)
}

func (e *protoEncoder) writeDouble(fieldNum int, val float64) {
	e.writeFixed64(fieldNum, math.Float64bits(val))
}

func (e *protoEncoder) writeBytes(fieldNum int, data []byte) {
	tag := uint64(fieldNum<<3) | uint64(protoWireBytes)
	e.appendVarint(tag)
	e.appendVarint(uint64(len(data)))
	e.buf = append(e.buf, data...)
}

func (e *protoEncoder) writeString(fieldNum int, val string) {
	e.writeBytes(fieldNum, []byte(val))
}

func (e *protoEncoder) appendVarint(val uint64) {
	for val >= 0x80 {
		e.buf = append(e.buf, byte(val)|0x80)
		val >>= 7
	}
	e.buf = append(e.buf, byte(val))
}

func (e *protoEncoder) Bytes() []byte {
	return e.buf
}

// DecodeWriteRequest decodes a protobuf-encoded WriteRequest.
// Proto schema: WriteRequest { repeated TimeSeries time_series = 1; }
// TimeSeries { repeated Label labels = 1; repeated Sample samples = 2; }
// Label { string name = 1; string value = 2; }
// Sample { double value = 1; int64 timestamp = 2; }
func DecodeWriteRequest(data []byte) (*WriteRequest, error) {
	dec := newProtoDecoder(data)
	req := &WriteRequest{}

	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return nil, fmt.Errorf("decode write request: %w", err)
		}
		if field.Number == 1 && field.WireType == protoWireBytes {
			ts, err := decodeTimeSeries(field.Bytes)
			if err != nil {
				return nil, err
			}
			req.TimeSeries = append(req.TimeSeries, *ts)
		}
	}
	return req, nil
}

func decodeTimeSeries(data []byte) (*ProtoTimeSeries, error) {
	dec := newProtoDecoder(data)
	ts := &ProtoTimeSeries{}

	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return nil, fmt.Errorf("decode time series: %w", err)
		}
		switch field.Number {
		case 1: // labels
			if field.WireType == protoWireBytes {
				label, err := decodeLabel(field.Bytes)
				if err != nil {
					return nil, err
				}
				ts.Labels = append(ts.Labels, *label)
			}
		case 2: // samples
			if field.WireType == protoWireBytes {
				sample, err := decodeSample(field.Bytes)
				if err != nil {
					return nil, err
				}
				ts.Samples = append(ts.Samples, *sample)
			}
		}
	}
	return ts, nil
}

func decodeLabel(data []byte) (*ProtoLabel, error) {
	dec := newProtoDecoder(data)
	label := &ProtoLabel{}
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return nil, err
		}
		switch field.Number {
		case 1:
			label.Name = string(field.Bytes)
		case 2:
			label.Value = string(field.Bytes)
		}
	}
	return label, nil
}

func decodeSample(data []byte) (*ProtoSample, error) {
	dec := newProtoDecoder(data)
	sample := &ProtoSample{}
	for dec.hasMore() {
		field, err := dec.readField()
		if err != nil {
			return nil, err
		}
		switch field.Number {
		case 1: // double value
			if field.WireType == protoWire64Bit {
				sample.Value = math.Float64frombits(field.Fixed64)
			}
		case 2: // int64 timestamp
			if field.WireType == protoWireVarint {
				sample.Timestamp = int64(field.Varint)
			}
		}
	}
	return sample, nil
}

// EncodeWriteRequest encodes a WriteRequest to protobuf wire format.
func EncodeWriteRequest(req *WriteRequest) []byte {
	enc := newProtoEncoder()
	for _, ts := range req.TimeSeries {
		tsEnc := newProtoEncoder()
		for _, l := range ts.Labels {
			lEnc := newProtoEncoder()
			lEnc.writeString(1, l.Name)
			lEnc.writeString(2, l.Value)
			tsEnc.writeBytes(1, lEnc.Bytes())
		}
		for _, s := range ts.Samples {
			sEnc := newProtoEncoder()
			sEnc.writeDouble(1, s.Value)
			sEnc.writeVarint(2, uint64(s.Timestamp))
			tsEnc.writeBytes(2, sEnc.Bytes())
		}
		enc.writeBytes(1, tsEnc.Bytes())
	}
	return enc.Bytes()
}

// gRPC frame format: 1 byte compressed flag + 4 byte big-endian length + payload
func encodeGRPCFrame(data []byte, compressed bool) []byte {
	frame := make([]byte, 5+len(data))
	if compressed {
		frame[0] = 1
	}
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(data)))
	copy(frame[5:], data)
	return frame
}

func decodeGRPCFrame(r io.Reader) ([]byte, bool, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, false, err
	}
	compressed := header[0] == 1
	length := binary.BigEndian.Uint32(header[1:5])
	if length > 16*1024*1024 {
		return nil, false, fmt.Errorf("grpc frame too large: %d bytes", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, false, err
	}
	return payload, compressed, nil
}

// WriteRequest represents a protobuf-style write request.
type WriteRequest struct {
	TimeSeries []ProtoTimeSeries `json:"time_series"`
}

// ProtoTimeSeries represents a time series in the proto format.
type ProtoTimeSeries struct {
	Labels  []ProtoLabel  `json:"labels"`
	Samples []ProtoSample `json:"samples"`
}

// ProtoLabel represents a metric label.
type ProtoLabel struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ProtoSample represents a single data point.
type ProtoSample struct {
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
}

// QueryRequest represents a protobuf-style query request.
type QueryRequest struct {
	Metric    string            `json:"metric"`
	Labels    map[string]string `json:"labels"`
	StartTime int64             `json:"start_time"`
	EndTime   int64             `json:"end_time"`
	Aggregate string            `json:"aggregate"` // reducer over one-second buckets
	GroupBy   []string          `json:"group_by"`
	Limit     int               `json:"limit"`
}

// QueryResponse represents a protobuf-style query response.
type QueryResponse struct {
	Series      []ResultSeries `json:"series"`
	QueryTimeMs int64          `json:"query_time_ms"`
	PointCount  int            `json:"point_count"`
}

// ResultSeries represents a single result series.
type ResultSeries struct {
	Metric  string            `json:"metric"`
	Labels  map[string]string `json:"labels"`
	Samples []ProtoSample     `json:"samples"`
}

// GRPCServiceInfo contains metadata about a registered gRPC service.
type GRPCServiceInfo struct {
	Name    string   `json:"name"`
	Methods []string `json:"methods"`
}

// GRPCIngestionEngine provides a gRPC-compatible ingestion and query server.
// While full gRPC requires protobuf codegen, this engine provides a
// high-performance binary-compatible JSON API following the same patterns
// and can serve as the foundation for full gRPC when proto dependencies are added.
type GRPCIngestionEngine struct {
	db     *DB
	config GRPCIngestionConfig

	mu       sync.RWMutex
	listener net.Listener
	running  bool
	stopCh   chan struct{}

	// Stats
	stats GRPCIngestionStats

	// Registered services for reflection
	services []GRPCServiceInfo

	// Interceptors (middleware chain)
	unaryInterceptors []UnaryInterceptor
}

// GRPCIngestionStats tracks server metrics.
type GRPCIngestionStats struct {
	TotalWriteRequests int64         `json:"total_write_requests"`
	TotalQueryRequests int64         `json:"total_query_requests"`
	TotalPointsWritten int64         `json:"total_points_written"`
	TotalErrors        int64         `json:"total_errors"`
	AvgWriteLatency    time.Duration `json:"avg_write_latency"`
	AvgQueryLatency    time.Duration `json:"avg_query_latency"`
	ActiveConnections  int64         `json:"active_connections"`
	StartedAt          time.Time     `json:"started_at"`
}

// UnaryInterceptor is a function that intercepts unary RPC calls.
type UnaryInterceptor func(ctx context.Context, method string, req interface{}) (interface{}, error)

// NewGRPCIngestionEngine creates a new gRPC ingestion engine.
func NewGRPCIngestionEngine(db *DB, cfg GRPCIngestionConfig) *GRPCIngestionEngine {
	g := &GRPCIngestionEngine{
		db:     db,
		config: cfg,
		stopCh: make(chan struct{}),
		services: []GRPCServiceInfo{
			{
				Name: "chronicle.WriteService",
				Methods: []string{
					"Write",
					"WriteBatch",
					"WriteStream",
				},
			},
			{
				Name: "chronicle.QueryService",
				Methods: []string{
					"Query",
					"QueryRange",
					"QueryStream",
				},
			},
			{
				Name: "chronicle.AdminService",
				Methods: []string{
					"ListMetrics",
					"GetStats",
					"HealthCheck",
				},
			},
			{
				Name: "opentelemetry.proto.collector.metrics.v1.MetricsService",
				Methods: []string{
					"Export",
				},
			},
		},
	}
	return g
}

// Start starts the gRPC ingestion server.
func (g *GRPCIngestionEngine) Start() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.running {
		return nil
	}

	ln, err := net.Listen("tcp", g.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	g.listener = ln
	g.running = true
	g.stats.StartedAt = time.Now()

	go g.serve()
	return nil
}

// Stop stops the gRPC ingestion server.
func (g *GRPCIngestionEngine) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.running {
		return
	}
	g.running = false
	close(g.stopCh)
	if g.listener != nil {
		g.listener.Close()
	}
}

// AddInterceptor adds a unary interceptor to the chain.
func (g *GRPCIngestionEngine) AddInterceptor(interceptor UnaryInterceptor) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.unaryInterceptors = append(g.unaryInterceptors, interceptor)
}

// HandleWrite processes a write request.
func (g *GRPCIngestionEngine) HandleWrite(ctx context.Context, req *WriteRequest) error {
	if req == nil {
		return fmt.Errorf("nil write request")
	}

	start := time.Now()
	var pointCount int64

	for _, ts := range req.TimeSeries {
		metric := ""
		tags := make(map[string]string)
		for _, l := range ts.Labels {
			if l.Name == "__name__" {
				metric = l.Value
			} else {
				tags[l.Name] = l.Value
			}
		}
		if metric == "" {
			continue
		}

		for _, s := range ts.Samples {
			p := Point{
				Metric:    metric,
				Tags:      tags,
				Value:     s.Value,
				Timestamp: s.Timestamp,
			}
			if err := g.db.WriteContext(ctx, p); err != nil {
				g.mu.Lock()
				g.stats.TotalErrors++
				g.mu.Unlock()
				return fmt.Errorf("write point: %w", err)
			}
			pointCount++
		}
	}

	g.mu.Lock()
	g.stats.TotalWriteRequests++
	g.stats.TotalPointsWritten += pointCount
	elapsed := time.Since(start)
	if g.stats.AvgWriteLatency == 0 {
		g.stats.AvgWriteLatency = elapsed
	} else {
		g.stats.AvgWriteLatency = (g.stats.AvgWriteLatency + elapsed) / 2
	}
	g.mu.Unlock()

	return nil
}

// HandleQuery processes a query request.
func (g *GRPCIngestionEngine) HandleQuery(ctx context.Context, req *QueryRequest) (*QueryResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("nil query request")
	}

	start := time.Now()

	q := Query{
		Metric: req.Metric,
		Start:  req.StartTime,
		End:    req.EndTime,
		Tags:   req.Labels,
	}

	if req.Limit > 0 {
		q.Limit = req.Limit
	}

	if req.Aggregate != "" {
		// Match the SQL parser default when the request has no window field.
		agg := &Aggregation{Window: time.Second}
		switch req.Aggregate {
		case "sum":
			agg.Function = AggSum
		case "avg", "mean":
			agg.Function = AggMean
		case "min":
			agg.Function = AggMin
		case "max":
			agg.Function = AggMax
		case "count":
			agg.Function = AggCount
		case "rate":
			agg.Function = AggRate
		default:
			g.mu.Lock()
			g.stats.TotalErrors++
			g.mu.Unlock()
			return nil, fmt.Errorf("unsupported aggregation: %s", req.Aggregate)
		}
		q.Aggregation = agg
	}

	result, err := g.db.ExecuteContext(ctx, &q)
	if err != nil {
		g.mu.Lock()
		g.stats.TotalErrors++
		g.mu.Unlock()
		return nil, fmt.Errorf("execute query: %w", err)
	}

	resp := &QueryResponse{
		Series: make([]ResultSeries, 0),
	}

	if result != nil {
		series := ResultSeries{
			Metric:  req.Metric,
			Labels:  req.Labels,
			Samples: make([]ProtoSample, 0, len(result.Points)),
		}
		for _, p := range result.Points {
			series.Samples = append(series.Samples, ProtoSample{
				Value:     p.Value,
				Timestamp: p.Timestamp,
			})
			resp.PointCount++
		}
		resp.Series = append(resp.Series, series)
	}

	elapsed := time.Since(start)
	resp.QueryTimeMs = elapsed.Milliseconds()

	g.mu.Lock()
	g.stats.TotalQueryRequests++
	if g.stats.AvgQueryLatency == 0 {
		g.stats.AvgQueryLatency = elapsed
	} else {
		g.stats.AvgQueryLatency = (g.stats.AvgQueryLatency + elapsed) / 2
	}
	g.mu.Unlock()

	return resp, nil
}

// Stats returns current server stats.
func (g *GRPCIngestionEngine) Stats() GRPCIngestionStats {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.stats
}

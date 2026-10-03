package chprotocol

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	chronicle "github.com/chronicle-db/chronicle"
)

func setupTestDB(t *testing.T) *chronicle.DB {
	t.Helper()
	dir := t.TempDir()
	cfg := chronicle.DefaultConfig(dir + "/test.db")
	db, err := chronicle.Open(cfg.Path, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestCHSession_WriteReadVarUInt(t *testing.T) {
	server, _ := NewCHNativeServer(setupTestDB(t), DefaultClickHouseProtocolConfig())

	tests := []uint64{0, 1, 127, 128, 16383, 16384, 1<<21 - 1, 1 << 21}
	for _, val := range tests {
		connBuf := &bytes.Buffer{}
		conn := &mockConn{buf: connBuf}
		sess := newCHSession(server, conn)

		sess.writer.Reset()
		sess.writeVarUInt(val)

		// Put written bytes into conn for reading
		connBuf.Write(sess.writer.Bytes())

		got, err := sess.readVarUInt()
		if err != nil {
			t.Errorf("readVarUInt after write(%d): %v", val, err)
			continue
		}
		if got != val {
			t.Errorf("roundtrip: got %d, want %d", got, val)
		}
	}
}

func TestCHSession_WriteReadString(t *testing.T) {
	// Both readVarUInt and readString read from s.conn
	server, _ := NewCHNativeServer(setupTestDB(t), DefaultClickHouseProtocolConfig())

	tests := []string{"", "hello", "ClickHouse Native Protocol"}
	for _, val := range tests {
		connBuf := &bytes.Buffer{}
		conn := &mockConn{buf: connBuf}
		sess := newCHSession(server, conn)

		sess.writer.Reset()
		sess.writeString(val)

		// Put all written bytes into conn for reading
		connBuf.Write(sess.writer.Bytes())

		got, err := sess.readString()
		if err != nil {
			t.Errorf("readString(%q): %v", val, err)
			continue
		}
		if got != val {
			t.Errorf("roundtrip: got %q, want %q", got, val)
		}
	}
}

func TestCHSession_WriteNumericTypes(t *testing.T) {
	conn := &mockConn{buf: &bytes.Buffer{}}
	server, _ := NewCHNativeServer(setupTestDB(t), DefaultClickHouseProtocolConfig())
	sess := newCHSession(server, conn)

	// Just verify writes don't panic
	sess.writeByte(0xFF)
	sess.writeUInt16(0xFFFF)
	sess.writeUInt32(0xFFFFFFFF)
	sess.writeUInt64(0xFFFFFFFFFFFFFFFF)
	sess.writeInt16(-32768)
	sess.writeInt32(-2147483648)
	sess.writeInt64(-9223372036854775808)
	sess.writeFloat32(3.14)
	sess.writeFloat64(2.718281828)

	if sess.writer.Len() == 0 {
		t.Error("expected non-empty buffer after writes")
	}
}

func TestCHQueryTranslator_SelectMin(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UnixNano()
	db.Write(chronicle.Point{Metric: "cpu", Timestamp: now - 3000000000, Value: 10.0})
	db.Write(chronicle.Point{Metric: "cpu", Timestamp: now - 2000000000, Value: 5.0})
	db.Write(chronicle.Point{Metric: "cpu", Timestamp: now - 1000000000, Value: 15.0})
	db.Flush()

	translator := &CHQueryTranslator{db: db}
	result, err := translator.Execute(context.Background(), "SELECT min(value) FROM cpu")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", result.RowCount)
	}
}

func TestCHQueryTranslator_SelectMax(t *testing.T) {
	db := setupTestDB(t)
	now := time.Now().UnixNano()
	db.Write(chronicle.Point{Metric: "cpu", Timestamp: now - 3000000000, Value: 10.0})
	db.Write(chronicle.Point{Metric: "cpu", Timestamp: now - 2000000000, Value: 5.0})
	db.Write(chronicle.Point{Metric: "cpu", Timestamp: now - 1000000000, Value: 15.0})
	db.Flush()

	translator := &CHQueryTranslator{db: db}
	result, err := translator.Execute(context.Background(), "SELECT max(value) FROM cpu")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.RowCount != 1 {
		t.Errorf("RowCount = %d, want 1", result.RowCount)
	}
}

func TestCHQueryTranslator_EmptyTable(t *testing.T) {
	db := setupTestDB(t)
	translator := &CHQueryTranslator{db: db}

	result, err := translator.Execute(context.Background(), "SELECT * FROM nonexistent LIMIT 10")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.RowCount != 0 {
		t.Errorf("RowCount = %d, want 0 for empty table", result.RowCount)
	}
}

func TestCHQueryTranslator_InvalidQuery(t *testing.T) {
	db := setupTestDB(t)
	translator := &CHQueryTranslator{db: db}

	_, err := translator.Execute(context.Background(), "DELETE FROM cpu")
	if err == nil {
		t.Error("expected error for DELETE query")
	}
}

func TestCHQueryTranslator_SelectWithWhere(t *testing.T) {
	db := setupTestDB(t)
	db.Write(chronicle.Point{Metric: "cpu", Timestamp: 1000, Value: 10.0, Tags: map[string]string{"host": "a"}})
	db.Write(chronicle.Point{Metric: "cpu", Timestamp: 2000, Value: 20.0, Tags: map[string]string{"host": "b"}})
	db.Flush()

	translator := &CHQueryTranslator{db: db}

	parsed, err := translator.parseSelect("SELECT * FROM cpu WHERE host = 'a'")
	if err != nil {
		t.Fatalf("parseSelect: %v", err)
	}
	if parsed.table != "cpu" {
		t.Errorf("table = %q, want cpu", parsed.table)
	}
}

func TestCHNativeServer_Stats(t *testing.T) {
	db := setupTestDB(t)
	config := DefaultClickHouseProtocolConfig()
	server, _ := NewCHNativeServer(db, config)

	stats := server.Stats()
	if stats.TotalConnections != 0 {
		t.Errorf("TotalConnections = %d, want 0", stats.TotalConnections)
	}
	if stats.ActiveConnections != 0 {
		t.Errorf("ActiveConnections = %d, want 0", stats.ActiveConnections)
	}
	if stats.TotalQueries != 0 {
		t.Errorf("TotalQueries = %d, want 0", stats.TotalQueries)
	}
	if stats.QueryErrors != 0 {
		t.Errorf("QueryErrors = %d, want 0", stats.QueryErrors)
	}
}

func TestCHNativeServer_ConnectionStats(t *testing.T) {
	const clients = 8
	config := DefaultClickHouseProtocolConfig()
	config.Address = "127.0.0.1:0"
	config.MaxConnections = clients
	server, err := NewCHNativeServer(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	connections := make([]net.Conn, 0, clients)
	for range clients {
		conn, err := net.DialTimeout("tcp", server.listener.Addr().String(), 5*time.Second)
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		connections = append(connections, conn)
	}

	// Observe snapshots while the accept loop and session goroutines update
	// their counters. Keeping the clients open makes both states deterministic.
	waitForStats := func(want CHServerStats) CHServerStats {
		t.Helper()
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			got := server.Stats()
			if got == want {
				return got
			}
			select {
			case <-deadline.C:
				t.Fatalf("Stats() = %+v, want %+v", got, want)
			case <-ticker.C:
			}
		}
	}
	connected := waitForStats(CHServerStats{TotalConnections: clients, ActiveConnections: clients})
	for _, conn := range connections {
		if err := conn.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
	waitForStats(CHServerStats{TotalConnections: clients})
	if connected.ActiveConnections != clients {
		t.Fatalf("previous snapshot changed after connections closed: %+v", connected)
	}
}

// mockConn implements net.Conn for testing
type mockConn struct {
	buf    *bytes.Buffer
	closed bool
}

func (m *mockConn) Read(b []byte) (int, error)  { return m.buf.Read(b) }
func (m *mockConn) Write(b []byte) (int, error) { return m.buf.Write(b) }
func (m *mockConn) Close() error                { m.closed = true; return nil }
func (m *mockConn) LocalAddr() net.Addr         { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9000} }
func (m *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
}
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

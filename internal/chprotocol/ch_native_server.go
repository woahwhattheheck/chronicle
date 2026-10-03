package chprotocol

import (
	"fmt"
	"net"
)

// Start begins listening for connections
func (s *CHNativeServer) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}

	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.listener = listener
	s.running = true
	s.mu.Unlock()

	go s.acceptLoop()

	return nil
}

// Stop gracefully shuts down the server
func (s *CHNativeServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	close(s.shutdown)
	s.running = false

	if s.listener != nil {
		s.listener.Close()
	}

	s.sessions.Range(func(key, value any) bool {
		if session, ok := value.(*CHSession); ok {
			session.Close()
		}
		return true
	})

	return nil
}

func (s *CHNativeServer) acceptLoop() {
	for {
		select {
		case <-s.shutdown:
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				return
			default:
				continue
			}
		}

		if s.activeConns.Load() >= int64(s.config.MaxConnections) {
			conn.Close()
			continue
		}

		s.totalConnections.Add(1)
		s.activeConns.Add(1)

		session := newCHSession(s, conn)
		s.sessions.Store(session.id, session)

		go s.handleSession(session)
	}
}

func (s *CHNativeServer) handleSession(session *CHSession) {
	defer func() {
		session.Close()
		s.sessions.Delete(session.id)
		s.activeConns.Add(-1)
	}()

	if err := session.handleHandshake(); err != nil {
		return
	}

	for {
		select {
		case <-s.shutdown:
			return
		default:
		}

		packetType, err := session.readPacketType()
		if err != nil {
			return
		}

		switch packetType {
		case CHClientPing:
			if err := session.handlePing(); err != nil {
				return
			}

		case CHClientQuery:
			s.totalQueries.Add(1)
			if err := session.handleQuery(); err != nil {
				s.queryErrors.Add(1)
			}

		case CHClientData:
			if err := session.handleData(); err != nil {
				return
			}

		case CHClientCancel:
			session.cancelQuery()

		default:
			return
		}
	}
}

// Stats returns server statistics
func (s *CHNativeServer) Stats() CHServerStats {
	return CHServerStats{
		TotalConnections:  s.totalConnections.Load(),
		ActiveConnections: s.activeConns.Load(),
		TotalQueries:      s.totalQueries.Load(),
		QueryErrors:       s.queryErrors.Load(),
	}
}

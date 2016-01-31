package rpc

import "net"

// Handler is an interface for implementing the backends used by RPC Server
type Handler interface {
	net.Listener
	Handoff(net.Conn)
}

// Server handles routing from an an incoming conneciton to a specified backend.
// These backends are determined based on the 1st byte read from the incoming connection
type Server struct {
	net.Listener
	handlers map[byte]Handler
}

// Request is the structure used for making RPC requests
type Request struct {
	Method string
	Args   []string
}

// Response is what's returned from RPC requests
type Response struct {
	Err string
}

// NewServer creates a new RPC server
func NewServer(l net.Listener, handlers map[byte]Handler) *Server {
	s := &Server{l, handlers}
	go s.listen()
	return s
}

// listen tells the RPC server to start handling incoming connections
func (s *Server) listen() {
	for {
		conn, err := s.Accept()
		if err != nil {
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		conn.Close()
		return
	}

	handler, ok := s.handlers[buf[0]]
	if !ok {
		conn.Close()
		return
	}
	handler.Handoff(conn)
}

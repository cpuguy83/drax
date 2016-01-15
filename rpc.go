package raftkv

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/docker/docker/pkg/raftkv/api"
)

const (
	reapKeys   = "reapKeys"
	addNode    = "addNode"
	removeNode = "removeNode"
	raftApply  = "raftApply"

	raftMessage api.MessageType = iota
)

type rpcHandlerFunc func(request *rpcRequest) error

var errClosedConn = errors.New("use of closed network connection")

// rpcHandler is an interface for implementing the backends used by RPCServer
type rpcHandler interface {
	net.Listener
	Handoff(net.Conn)
}

// RPCServer handles routing from an an incoming conneciton to a specified backend.
// These backends are determined based on the 1st byte read from the incoming connection
type RPCServer struct {
	net.Listener
	handlers map[api.MessageType]rpcHandler
}

type rpcRequest struct {
	Method string
	Args   []string
}

type rpcResponse struct {
	Err string
}

func newRPCServer(l net.Listener, handlers map[api.MessageType]rpcHandler) *RPCServer {
	s := &RPCServer{l, handlers}
	go s.listen()
	return s
}

func (s *RPCServer) listen() {
	for {
		conn, err := s.Accept()
		if err != nil {
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *RPCServer) handleConn(conn net.Conn) {
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		conn.Close()
		return
	}

	handler, ok := s.handlers[api.MessageType(buf[0])]
	if !ok {
		conn.Close()
		return
	}
	handler.Handoff(conn)
}

// streamLayer is used both by raft and by our routing layer
// it implements raft.StreamLayer and net.Listener
type streamLayer struct {
	addr      net.Addr
	chConn    chan net.Conn
	chClose   chan struct{}
	closeLock sync.Mutex
	tlsConfig *tls.Config
	rpcType   api.MessageType
}

func newStreamLayer(addr net.Addr, tlsConfig *tls.Config, rpcType api.MessageType) (*streamLayer, error) {
	return &streamLayer{
		chConn:  make(chan net.Conn),
		chClose: make(chan struct{}),
		addr:    addr,
		rpcType: rpcType,
	}, nil
}

func (l *streamLayer) Handoff(conn net.Conn) {
	l.chConn <- conn
}

func (l *streamLayer) Accept() (net.Conn, error) {
	select {
	case conn := <-l.chConn:
		return conn, nil
	case <-l.chClose:
		return nil, errClosedConn
	}
}

// Dial is used by Raft for RPC
func (l *streamLayer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	return api.Dial(address, l.rpcType, timeout, l.tlsConfig, false)
}

// Close shuts down the stream layer
func (l *streamLayer) Close() error {
	l.closeLock.Lock()
	defer l.closeLock.Unlock()

	select {
	case <-l.chClose:
		return &net.OpError{Op: "close", Net: l.addr.Network(), Addr: l.Addr(), Err: errClosedConn}
	default:
	}

	close(l.chClose)
	return nil
}

// Addr returns the address the stremLayer is listening on
// This is used to satisfy the net.Listener interface
func (l *streamLayer) Addr() net.Addr {
	return l.addr
}

// rpc is a helper function for performing RPC requests between nodes
func rpc(addr string, msg *rpcRequest, tlsConfig *tls.Config) (*rpcResponse, error) {
	conn, err := api.Dial(addr, api.RPCMessage, defaultTimeout, tlsConfig, true)
	if err != nil {
		return nil, err
	}

	if err := api.Encode(msg, conn); err != nil {
		return nil, err
	}

	var res rpcResponse
	if err := api.Decode(&res, conn); err != nil {
		return nil, err
	}
	return &res, nil
}

// rpcProxyRequest is a helper function to proxy an rpc request to another node
func rpcProxyRequest(addr string, msg *rpcRequest, from io.ReadWriter, tlsConfig *tls.Config) error {
	conn, err := api.Dial(addr, api.RPCMessage, defaultTimeout, tlsConfig, true)
	if err != nil {
		return err
	}
	if err := api.Encode(msg, conn); err != nil {
		return err
	}

	chErr := make(chan error, 2)
	go func() {
		_, err := io.Copy(from, conn)
		chErr <- err
	}()
	go func() {
		_, err := io.Copy(conn, from)
		chErr <- err
	}()

	if err := <-chErr; err != nil {
		return err
	}
	return <-chErr
}

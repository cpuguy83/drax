package rpc

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cpuguy83/drax/api"
)

var (
	retryTimeout = 30
	// ErrClosedConn is the error returned when the underlying listener has been closed
	ErrClosedConn = errors.New("use of closed network connection")
)

// DialerFn is the function the StreamLayer should use to Dial out to another node
type DialerFn func(address string, timeout time.Duration) (net.Conn, error)

// StreamLayer is used for handling rpc requests between nodes (be it raft, or other custom RPC)
// When a request comes into an RPC Server, the Server will route it to the appropriate StreamLayer
// StreamLayer implements raft.StreamLayer and net.Listener
type StreamLayer struct {
	addr      net.Addr
	chConn    chan net.Conn
	chClose   chan struct{}
	closeLock sync.Mutex
	rpcType   byte
	rpcDialer DialerFn
}

// NewStreamLayer creates a new StreamLayer
// `addr` is used so this can implement the net.Listener interface
// `rpcType` is the type of Message this StreamLayer will handle
func NewStreamLayer(addr net.Addr, rpcType byte, dialer DialerFn) *StreamLayer {
	return &StreamLayer{
		chConn:    make(chan net.Conn),
		chClose:   make(chan struct{}),
		addr:      addr,
		rpcType:   rpcType,
		rpcDialer: dialer,
	}
}

// Handoff is used by the RPC server hand-off a conn to this StreamLayer
func (l *StreamLayer) Handoff(conn net.Conn) {
	l.chConn <- conn
}

// Accept waits for and returns the next connection
func (l *StreamLayer) Accept() (net.Conn, error) {
	select {
	case conn := <-l.chConn:
		return conn, nil
	case <-l.chClose:
		return nil, ErrClosedConn
	}
}

// Dial is used by Raft for RPC
func (l *StreamLayer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	return l.DialWithRetry(address, timeout, false)
}

// DialWithRetry is like `Dial` but uses a retry mechanism
func (l *StreamLayer) DialWithRetry(address string, timeout time.Duration, retry bool) (net.Conn, error) {
	if address == "" {
		return nil, fmt.Errorf("empty address")
	}
	var retries int
	start := time.Now()

	for {
		conn, err := l.rpcDialer(address, timeout)
		if err != nil {
			if !retry {
				return nil, err
			}
			timeOff := backoff(retries)
			if abort(start, timeOff, timeout) {
				return nil, err
			}

			retries++
			time.Sleep(timeOff)
			continue
		}

		if _, err := conn.Write([]byte{byte(l.rpcType)}); err != nil {
			return nil, err
		}
		return conn, nil
	}
}

// Close shuts down the stream layer
func (l *StreamLayer) Close() error {
	l.closeLock.Lock()
	defer l.closeLock.Unlock()

	select {
	case <-l.chClose:
		return &net.OpError{Op: "close", Net: l.addr.Network(), Addr: l.Addr(), Err: ErrClosedConn}
	default:
	}

	close(l.chClose)
	return nil
}

// Addr returns the address the stremLayer is listening on
// This is used to satisfy the net.Listener interface
func (l *StreamLayer) Addr() net.Addr {
	return l.addr
}

// RPC is a helper function for performing RPC requests between nodes
func (l *StreamLayer) RPC(addr string, msg *Request) (*Response, error) {
	conn, err := l.DialWithRetry(addr, time.Duration(retryTimeout)*time.Second, true)
	if err != nil {
		return nil, err
	}
	if err := api.Encode(msg, conn); err != nil {
		return nil, err
	}

	var res Response
	if err := api.Decode(&res, conn); err != nil {
		return nil, err
	}
	return &res, nil
}

// ProxyRequest is a helper function to proxy an rpc request to another node
func (l *StreamLayer) ProxyRequest(addr string, msg *Request, from io.ReadWriter) error {
	conn, err := l.DialWithRetry(addr, time.Duration(retryTimeout)*time.Second, true)
	if err != nil {
		return err
	}
	defer conn.Close()
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

func backoff(retries int) time.Duration {
	b, max := 1, retryTimeout
	for b < max && retries > 0 {
		b *= 2
		retries--
	}
	if b > max {
		b = max
	}
	return time.Duration(b) * time.Second
}

func abort(start time.Time, timeOff, max time.Duration) bool {
	return timeOff+time.Since(start) >= time.Duration(max)*time.Second
}

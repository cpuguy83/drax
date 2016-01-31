package rpc

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/docker/go-connections/sockets"
)

var testDialers = make(map[string]func(network, addr string) (net.Conn, error))

type testHandler struct {
	*StreamLayer
	msgType byte
}

func (h *testHandler) handleConns() {
	for {
		conn, err := h.Accept()
		if err != nil {
			return
		}
		conn.Write([]byte{h.msgType})
	}
}

func TestServer(t *testing.T) {
	l := sockets.NewInmemSocket("test", 1)
	testDialers[l.Addr().String()] = l.Dial
	handlers := map[byte]Handler{
		'1': newTestHandler(l.Addr(), '1'),
		'2': newTestHandler(l.Addr(), '2'),
	}
	srv := NewServer(l, handlers)
	defer srv.Close()

	for msgType := range handlers {
		conn, err := l.Dial("test", "test")
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := conn.Write([]byte{byte(msgType)}); err != nil {
			t.Fatal(err)
		}

		// the test handler just writes out the message type it uses, so let's read and compare to what we're expecting
		buf := make([]byte, 1)
		if _, err := conn.Read(buf); err != nil {
			t.Fatal(err)
		}
		if buf[0] != byte(msgType) {
			t.Fatalf("expected conn from %q, got %q", msgType, buf[0])
		}
	}
}

func newTestHandler(addr net.Addr, msgType byte) Handler {
	dialer := func(addr string, timeout time.Duration) (net.Conn, error) {
		h, ok := testDialers[addr]
		if !ok {
			return nil, fmt.Errorf("unreachable")
		}
		return h("inmem", addr)
	}
	h := &testHandler{NewStreamLayer(addr, msgType, dialer), msgType}
	go h.handleConns()
	return h
}

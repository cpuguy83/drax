package integration

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/cpuguy83/drax"
	"github.com/cpuguy83/drax/api/errors"
	"github.com/docker/distribution/registry/api/errcode"
	"github.com/docker/go-connections/sockets"
)

var clusterDialers = make(map[string]func(network, addr string) (net.Conn, error))

func TestStoreGetPutDelete(t *testing.T) {
	nodes, err := newTestCluster(3, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupNodes(nodes)

	if len(nodes) != 3 {
		t.Fatal("expected 3 nodes, got %d", len(nodes))
	}

	// Test key doesn't exist
	for _, n := range nodes {
		_, err := n.KVStore().Get("hello")
		if err == nil {
			t.Fatalf("expected error, got none")
		}

		switch e := err.(type) {
		case errcode.Error:
			if e.Code != errors.StoreKeyNotFound {
				t.Fatalf("expected %q, got %q", errors.StoreKeyNotFound.Error(), err)
			}
		default:
			if err != drax.ErrKeyNotFound {
				t.Fatalf("exepcted %v, got %v", drax.ErrKeyNotFound, err)
			}
		}
	}

	if err := nodes[0].KVStore().Put("hello", []byte("world"), nil); err != nil {
		t.Fatal(err)
	}

	for i, n := range nodes {
		kv, err := n.KVStore().Get("hello")
		if err != nil {
			t.Fatalf("fail on node %d: %v", i+1, err)
		}

		if string(kv.Value) != "world" {
			t.Fatalf("expected world, got %s", kv.Value)
		}
	}

	if err := nodes[0].KVStore().Delete("hello"); err != nil {
		t.Fatal(err)
	}

	// Test key doesn't exist
	for _, n := range nodes {
		_, err := n.KVStore().Get("hello")
		if err == nil {
			t.Fatalf("expected error, got none")
		}

		switch e := err.(type) {
		case errcode.Error:
			if e.Code != errors.StoreKeyNotFound {
				t.Fatalf("expected %q, got %q", errors.StoreKeyNotFound.Error(), err)
			}
		default:
			if err != drax.ErrKeyNotFound {
				t.Fatalf("exepcted %v, got %v", drax.ErrKeyNotFound, err)
			}
		}
	}
}

func newTestCluster(size int, prefixAddr string) ([]*drax.Cluster, error) {
	var nodes []*drax.Cluster
	for i := 0; i < size; i++ {
		addr := prefixAddr + strconv.Itoa(i)
		home, err := ioutil.TempDir("", addr)
		if err != nil {
			return nil, err
		}
		l := sockets.NewInmemSocket(addr, 1)
		clusterDialers[addr] = l.Dial
		dialer := func(addr string, timeout time.Duration) (net.Conn, error) {
			h, ok := clusterDialers[addr]
			if !ok {
				return nil, fmt.Errorf("unreachable")
			}
			return h("inmem", addr)
		}
		var peer string
		if i > 0 {
			peer = nodes[i-1].Addr()
		}
		c, err := drax.New(l, dialer, home, addr, peer)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, c)
	}
	return nodes, nil
}

func cleanupNodes(nodes []*drax.Cluster) {
	for _, n := range nodes {
		os.RemoveAll(n.Home())
		n.Shutdown()
		delete(clusterDialers, n.Addr())
	}
}

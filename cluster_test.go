package drax

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/docker/go-connections/sockets"
)

var testHome string

func init() {
	testHome = os.Getenv("DRAX_TEST_HOME")
	if testHome != "" {
		os.MkdirAll(testHome, 0755)
	}
}

var clusterDialers = make(map[string]func(network, addr string) (net.Conn, error))

func TestNewCluster(t *testing.T) {
	nodes, err := newTestCluster(3, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupNodes(nodes)

	timeout := time.After(10 * time.Second)
	for _, n := range nodes {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for peers")
		default:
		}
		peers, err := n.peers.Peers()
		if err != nil {
			t.Fatal(err)
		}
		if len(peers) == 2 {
			break
		}
	}
}

func TestKVSingleNode(t *testing.T) {
	nodes, err := newTestCluster(1, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupNodes(nodes)
	n := nodes[0]

	kv := n.KVStore()
	err = kv.Put("hello", []byte("world"), nil)
	if err != nil {
		t.Fatal(err)
	}

	kvPair, err := kv.Get("hello")
	if string(kvPair.Value) != "world" {
		t.Fatalf("exepcted value to be `world`, got %s", string(kvPair.Value))
	}
}

func TestKVMultiNode(t *testing.T) {
	nodes, err := newTestCluster(3, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupNodes(nodes)

	if err := nodes[0].KVStore().Put("hello", []byte("world"), nil); err != nil {
		t.Fatal(err)
	}

	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	for _, n := range nodes {
		for _ = range ticker.C {
			select {
			case <-timeout:
				t.Fatalf("timeout waiting for k/v pair on node %s", n.addr)
			default:
			}
			kv, err := n.KVStore().Get("hello")
			if err != nil {
				continue
			}

			if string(kv.Value) == "world" {
				break
			}
		}
	}
}

func newTestCluster(size int, prefixAddr string) ([]*Cluster, error) {
	var nodes []*Cluster
	for i := 0; i < size; i++ {
		addr := prefixAddr + strconv.Itoa(i)
		home, err := ioutil.TempDir(testHome, addr)
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
			peer = nodes[i-1].addr
		}
		c, err := New(l, dialer, home, addr, peer)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, c)
	}
	return nodes, nil
}

func cleanupNodes(nodes []*Cluster) {
	for _, n := range nodes {
		os.RemoveAll(n.home)
		n.Shutdown()
		delete(clusterDialers, n.addr)
	}
}

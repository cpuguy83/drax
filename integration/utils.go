package integration

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/cpuguy83/drax"
	"github.com/docker/go-connections/sockets"
)

var clusterDialers = make(map[string]func(network, addr string) (net.Conn, error))

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
		c, err := drax.New(l, dialer, home, addr, peer, nil)
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

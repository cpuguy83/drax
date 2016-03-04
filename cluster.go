package drax

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/cpuguy83/drax/api"
	"github.com/cpuguy83/drax/rpc"
	libkvstore "github.com/docker/libkv/store"
	"github.com/hashicorp/raft"
)

var (
	defaultTimeout = 5 * time.Second
)

// Cluster is used to manage all the cluster state for a given node
type Cluster struct {
	home, addr, peerAddr string
	store                *store
	chShutdown           chan struct{}
	chErrors             chan error
	server               *rpc.Server
	l                    net.Listener
	r                    *Raft
	mu                   sync.Mutex
	rpcDialer            rpc.DialerFn
	peers                raft.PeerStore
	logger               io.Writer
}

// New creates a new Cluster and starts it
func New(l net.Listener, rpcDialer rpc.DialerFn, home, addr, peer string, logger io.Writer) (*Cluster, error) {
	if err := os.MkdirAll(home, 0600); err != nil {
		return nil, fmt.Errorf("error creating home dir: %v", err)
	}

	c := &Cluster{
		home:       home,
		addr:       addr,
		peerAddr:   peer,
		chShutdown: make(chan struct{}),
		chErrors:   make(chan error, 1),
		l:          l,
		logger:     logger,
		rpcDialer:  rpcDialer,
	}
	return c, c.start()
}

func (c *Cluster) start() error {
	c.store = newStore()

	cfg := raft.DefaultConfig()
	cfg.ShutdownOnRemove = false
	if c.logger != nil {
		cfg.LogOutput = c.logger
	}

	raftStream := rpc.NewStreamLayer(c.l.Addr(), byte(raftMessage), c.rpcDialer)
	raftTransport := raft.NewNetworkTransport(raftStream, 3, defaultTimeout, os.Stdout)
	peerStore := newPeerStore(c.home, raftTransport)
	c.peers = peerStore

	peers, err := peerStore.Peers()
	if err != nil {
		return err
	}
	nPeers := len(peers)
	if nPeers <= 1 && c.peerAddr == "" {
		cfg.EnableSingleNode = true
	}

	kvRaft, err := newRaft(filepath.Join(c.home, "raft"), c.addr, peerStore, (*storeFSM)(c.store), raftTransport, cfg)
	if err != nil {
		return err
	}
	c.store.r = kvRaft
	c.store.dialer = c.rpcDialer
	kvRaft.store = c.store
	kvRaft.stream = raftStream

	nodeRPCStream := rpc.NewStreamLayer(c.l.Addr(), byte(api.RPCMessage), c.rpcDialer)
	nodeRPC := &nodeRPC{nodeRPCStream, kvRaft}
	go nodeRPC.handleConns()

	clientRPCStream := rpc.NewStreamLayer(c.l.Addr(), byte(api.ClientMessage), c.rpcDialer)
	clientRPC := &clientRPC{clientRPCStream, c.store}
	go clientRPC.handleConns()

	handlers := map[byte]rpc.Handler{
		byte(raftMessage):       raftStream,
		byte(api.RPCMessage):    nodeRPCStream,
		byte(api.ClientMessage): clientRPCStream,
	}

	c.server = rpc.NewServer(c.l, handlers)
	c.r = kvRaft

	go c.store.waitLeader()
	go c.waitLeader()

	if c.peerAddr != "" && nPeers <= 1 {
		res, err := nodeRPCStream.RPC(c.peerAddr, &rpc.Request{
			Method: addNode,
			Args:   []string{c.addr},
		})
		if err != nil {
			return err
		}
		if res.Err != "" && !strings.Contains(res.Err, "peer already known") {
			return fmt.Errorf(res.Err)
		}
	}

	return nil
}

// Shutdown stops the local cluster node
func (c *Cluster) Shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.chShutdown:
		c.chErrors <- fmt.Errorf("already shutdown")
		return
	default:
	}

	c.r.Close()
	close(c.chShutdown)
}

// KVStore provides access to the underlying KV store
func (c *Cluster) KVStore() libkvstore.Store {
	return c.store
}

// ShutdownCh returns a channel that can be watched to determine if the cluster has been shut down
func (c *Cluster) ShutdownCh() <-chan struct{} {
	return c.chShutdown
}

// Errors returns a channel receiver that callers can use to listen for cluster errors
func (c *Cluster) Errors() <-chan error {
	return c.chErrors
}

func (c *Cluster) waitLeader() {
	leaderCh := c.r.LeaderCh()
	for {
		logrus.Info("cluster: waiting for leader")
		select {
		case <-c.chShutdown:
			return
		case state := <-leaderCh:
			if state == raft.Leader {
				c.handleLeader(leaderCh)
			}
		}
	}
}

func (c *Cluster) handleLeader(leaderCh <-chan interface{}) {
	for {
		select {
		case <-c.chShutdown:
			return
		case state := <-leaderCh:
			if state != raft.Leader {
				return
			}
		default:
		}

		// Handle cluster-level leadership resonsibilities
		time.Sleep(5 * time.Second)
	}
}

// Addr cluster is available on
func (c *Cluster) Addr() string {
	return c.addr
}

// Home is the home path where the cluster state is stored
func (c *Cluster) Home() string {
	return c.home
}

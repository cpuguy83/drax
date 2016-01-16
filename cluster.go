package drax

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/cpuguy83/drax/api"
	libkvstore "github.com/docker/libkv/store"
	"github.com/hashicorp/raft"
)

var (
	DefaultRaftPort = "2380"
	defaultTimeout  = 5 * time.Second
)

type Cluster struct {
	home, addr, peerAddr string
	store                *store
	chShutdown           chan struct{}
	chErrors             chan error
	server               *RPCServer
	tlsConfig            *tls.Config
	l                    net.Listener
	r                    *Raft
	mu                   sync.Mutex
}

func New(l net.Listener, home, addr, peer string, tlsConfig *tls.Config) (*Cluster, error) {
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
	}
	return c, c.start()
}

func (c *Cluster) start() error {
	c.store = newStore()

	cfg := raft.DefaultConfig()
	cfg.ShutdownOnRemove = false

	// setup K/V store
	raftStream, err := newStreamLayer(c.l.Addr(), c.tlsConfig, raftMessage)
	if err != nil {
		return err
	}
	raftTransport := raft.NewNetworkTransport(raftStream, 3, defaultTimeout, os.Stdout)
	peerStore := newPeerStore(c.home, raftTransport)

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
	kvRaft.store = c.store

	nodeRPCStream, err := newStreamLayer(c.l.Addr(), c.tlsConfig, api.RPCMessage)
	if err != nil {
		return err
	}
	nodeRPC := &nodeRPC{nodeRPCStream, kvRaft}
	go nodeRPC.handleConns()

	clientRPCStream, err := newStreamLayer(c.l.Addr(), c.tlsConfig, api.ClientMessage)
	if err != nil {
		return err
	}
	clientRPC := &clientRPC{clientRPCStream, c.store}
	go clientRPC.handleConns()

	handlers := map[api.MessageType]rpcHandler{
		raftMessage:       raftStream,
		api.RPCMessage:    nodeRPCStream,
		api.ClientMessage: clientRPCStream,
	}

	c.server = newRPCServer(c.l, handlers)
	c.r = kvRaft

	go c.store.waitLeader()
	go c.waitLeader()

	if c.peerAddr != "" && nPeers <= 1 {
		res, err := rpc(c.peerAddr, &rpcRequest{
			Method: addNode,
			Args:   []string{c.addr},
		}, c.tlsConfig)
		if err != nil {
			return err
		}
		if res.Err != "" && !strings.Contains(res.Err, "peer already known") {
			return fmt.Errorf(res.Err)
		}
	}

	return nil
}

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

func (c *Cluster) KVStore() libkvstore.Store {
	return c.store
}

func (c *Cluster) ShutdownCh() <-chan struct{} {
	return c.chShutdown
}

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
		logrus.Debug("cluster: handling leadership")
		time.Sleep(5 * time.Second)
	}
}

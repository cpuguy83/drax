package drax

import (
	"io"
	"net"

	"github.com/Sirupsen/logrus"
	"github.com/cpuguy83/drax/api"
	"github.com/cpuguy83/drax/rpc"
	libkvstore "github.com/docker/libkv/store"
)

// nodeRPC handles communcations for node-level actions(e.g., addNode, removeNode)
type nodeRPC struct {
	*rpc.StreamLayer
	r *Raft
}

// clientRPC handles communications with k/v store clients
type clientRPC struct {
	*rpc.StreamLayer
	s *store
}

func (r *nodeRPC) addNode(req *rpc.Request) error {
	return r.r.AddPeer(req.Args[0])
}

func (r *nodeRPC) removeNode(req *rpc.Request) error {
	return r.r.RemovePeer(req.Args[0])
}

func (r *nodeRPC) handleConns() {
	for {
		conn, err := r.Accept()
		if err != nil {
			if err == rpc.ErrClosedConn {
				return
			}
			continue
		}

		go r.handleConn(conn)
	}
}

func (r *nodeRPC) handleConn(conn net.Conn) {
	defer conn.Close()
	var req rpc.Request
	if err := api.Decode(&req, conn); err != nil {
		logrus.Errorf("error handling K/V RPC connection: %v", err)
		return
	}

	logrus.Debugf("Got: %s(%s)", req.Method, req.Args)

	var h rpcHandlerFunc

	switch req.Method {
	case addNode:
		h = r.addNode
	case removeNode:
		h = r.removeNode
	}

	if !r.r.IsLeader() {
		r.ProxyRequest(r.r.GetLeader(), &req, conn)
		return
	}

	var res rpc.Response
	if err := h(&req); err != nil {
		res.Err = err.Error()
	}
	api.Encode(&res, conn)
}

func (r *clientRPC) Get(conn io.Writer, req *clientRequest) {
	var res api.Response
	kv, err := r.s.Get(req.Key)
	if err != nil {
		res.Err = err.Error()
		api.Encode(&res, conn)
		return
	}
	res.KV = libkvToKV(kv)
	api.Encode(&res, conn)
}

func (r *clientRPC) Put(conn io.Writer, req *clientRequest) {
	var res api.Response
	err := r.s.Put(req.Key, req.Value, &libkvstore.WriteOptions{TTL: req.TTL})
	if err != nil {
		res.Err = err.Error()
	}
	api.Encode(res, conn)
}

func waitClose(conn io.Reader, chStop chan struct{}) {
	buf := make([]byte, 1)
	for {
		_, err := conn.Read(buf)
		if err == io.EOF {
			close(chStop)
		}
		continue
	}
}

func (r *clientRPC) Watch(conn io.Writer, req *clientRequest) {
	chStop := make(chan struct{})
	go waitClose(req.body, chStop)
	chKv, err := r.s.Watch(req.Key, chStop)
	if err != nil {
		return
	}
	for kv := range chKv {
		api.Encode(libkvToKV(kv), conn)
	}
}

func (r *clientRPC) WatchTree(conn io.Writer, req *clientRequest) {
	chStop := make(chan struct{})
	go waitClose(req.body, chStop)
	chKv, err := r.s.WatchTree(req.Key, chStop)
	if err != nil {
		return
	}
	var apiKVList []*api.KVPair
	for kvList := range chKv {
		for _, kv := range kvList {
			apiKVList = append(apiKVList, libkvToKV(kv))
		}
		api.Encode(apiKVList, conn)
		apiKVList = nil
	}
}

func (r *clientRPC) handleConns() {
	for {
		conn, err := r.Accept()
		if err != nil {
			if err == rpc.ErrClosedConn {
				return
			}
			continue
		}
		go r.handleConn(conn)
	}
}

type clientRPCHandlerFn func(io.Writer, *clientRequest)
type clientRequest struct {
	*api.Request
	body io.Reader
}

func (r *clientRPC) handleConn(conn net.Conn) {
	defer conn.Close()
	var req api.Request
	if err := api.Decode(&req, conn); err != nil {
		logrus.Errorf("error handling K/V RPC connection: %v", err)
		return
	}

	logrus.Debugf("Got: %s(%s)", req.Action, req.Key)

	var h clientRPCHandlerFn

	switch req.Action {
	case api.Get:
		h = r.Get
	case api.Put:
		h = r.Put
	case api.Watch:
		h = r.Watch
	case api.WatchTree:
		h = r.WatchTree
	}

	h(conn, &clientRequest{&req, conn})
}

func libkvToKV(kv *libkvstore.KVPair) *api.KVPair {
	return &api.KVPair{
		Key:       kv.Key,
		Value:     kv.Value,
		LastIndex: kv.LastIndex,
	}
}

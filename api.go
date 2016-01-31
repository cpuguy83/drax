package drax

import (
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
		r.ProxyRequest(r.r.Leader(), &req, conn)
		return
	}

	var res rpc.Response
	if err := h(&req); err != nil {
		res.Err = err.Error()
	}
	api.Encode(&res, conn)
}

func (r *clientRPC) Get(req *api.Request) *api.Response {
	var res api.Response
	kv, err := r.s.Get(req.Key)
	if err != nil {
		res.Err = err.Error()
		return &res
	}
	res.KV = libkvToKV(kv)
	return &res
}

func (r *clientRPC) Put(req *api.Request) *api.Response {
	var res api.Response
	err := r.s.Put(req.Key, req.Value, &libkvstore.WriteOptions{TTL: req.TTL})
	if err != nil {
		res.Err = err.Error()
	}
	return &res
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

type clientRPCHandlerFn func(*api.Request) *api.Response

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
	}

	api.Encode(h(&req), conn)
}

func libkvToKV(kv *libkvstore.KVPair) *api.KVPair {
	return &api.KVPair{
		Key:       kv.Key,
		Value:     kv.Value,
		LastIndex: kv.LastIndex,
	}
}

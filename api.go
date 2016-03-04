package drax

import (
	"io"
	"net"

	"github.com/Sirupsen/logrus"
	"github.com/cpuguy83/drax/api"
	"github.com/cpuguy83/drax/api/errors"
	"github.com/cpuguy83/drax/rpc"
	"github.com/docker/distribution/registry/api/errcode"
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
		setError(&res, err)
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
		setError(&res, err)
	}
	api.Encode(res, conn)
}

func waitClose(conn io.Reader, chStop chan struct{}) {
	buf := make([]byte, 1)
	for {
		_, err := conn.Read(buf)
		if err == io.EOF {
			close(chStop)
			return
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

func (r *clientRPC) List(conn io.Writer, req *clientRequest) {
	var res api.Response
	ls, err := r.s.List(req.Key)
	if err != nil {
		setError(&res, err)
	}

	var apiLs []*api.KVPair
	for _, kv := range ls {
		apiLs = append(apiLs, libkvToKV(kv))
	}
	res.List = apiLs
	api.NewEncoder(conn).Encode(&res)
}

func (r *clientRPC) DeleteTree(conn io.Writer, req *clientRequest) {
	var res api.Response
	if err := r.s.DeleteTree(req.Key); err != nil {
		setError(&res, err)
	}
	api.NewEncoder(conn).Encode(&res)
}

func (r *clientRPC) Delete(conn io.Writer, req *clientRequest) {
	var res api.Response
	if err := r.s.Delete(req.Key); err != nil {
		setError(&res, err)
	}
	api.NewEncoder(conn).Encode(&res)
}

func (r *clientRPC) Exists(conn io.Writer, req *clientRequest) {
	var res api.Response
	exists, err := r.s.Exists(req.Key)
	if err != nil {
		setError(&res, err)
	}
	res.Exists = exists
	api.NewEncoder(conn).Encode(&res)
}

func (r *clientRPC) AtomicPut(conn io.Writer, req *clientRequest) {
	var res api.Response
	ok, kv, err := r.s.AtomicPut(req.Key, req.Value, kvToLibKV(req.Previous), &libkvstore.WriteOptions{TTL: req.TTL})
	if err != nil {
		setError(&res, err)
	} else {
		res.Completed = ok
		res.KV = libkvToKV(kv)
	}
	api.NewEncoder(conn).Encode(&res)
}

func (r *clientRPC) AtomicDelete(conn io.Writer, req *clientRequest) {
	var res api.Response
	ok, err := r.s.AtomicDelete(req.Key, kvToLibKV(req.Previous))
	if err != nil {
		setError(&res, err)
	}
	res.Completed = ok
	api.NewEncoder(conn).Encode(&res)
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
	case api.List:
		h = r.List
	case api.Delete:
		h = r.Delete
	case api.DeleteTree:
		h = r.DeleteTree
	case api.AtomicPut:
		h = r.AtomicPut
	case api.AtomicDelete:
		h = r.AtomicDelete
	case api.Exists:
		h = r.Exists
	default:
		logrus.Errorf("can't handle action: %s", req.Action)
		return
	}

	h(conn, &clientRequest{&req, conn})
}

func libkvToKV(kv *libkvstore.KVPair) *api.KVPair {
	if kv == nil {
		return nil
	}
	return &api.KVPair{
		Key:       kv.Key,
		Value:     kv.Value,
		LastIndex: kv.LastIndex,
	}
}

func kvToLibKV(kv *api.KVPair) *libkvstore.KVPair {
	if kv == nil {
		return nil
	}
	return &libkvstore.KVPair{
		Key:       kv.Key,
		Value:     kv.Value,
		LastIndex: kv.LastIndex,
	}
}

func setError(res *api.Response, err error) {
	if err == nil {
		return
	}
	switch err {
	case ErrKeyNotFound:
		e := errors.StoreKeyNotFound.WithMessage(err.Error())
		res.Err = &e
	case ErrKeyModified:
		e := errors.StoreKeyModified.WithMessage(err.Error())
		res.Err = &e
	case ErrCallNotSupported:
		e := errcode.ErrorCodeUnsupported.WithMessage(err.Error())
		res.Err = &e
	default:
		e := errcode.ErrorCodeUnknown.WithMessage(err.Error())
		res.Err = &e
	}
}

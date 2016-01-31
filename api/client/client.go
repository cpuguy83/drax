package client

import (
	"errors"
	"time"

	"github.com/cpuguy83/drax/api"
	"github.com/cpuguy83/drax/rpc"
	"github.com/docker/libkv/store"
)

var errNotImplemented = errors.New("Call not implemented in current backend")

type Client struct {
	addr        string
	dialTimeout time.Duration
	streamLayer *rpc.StreamLayer
}

func New(addr string, dialTimeout time.Duration, dialer rpc.DialerFn) *Client {
	sl := rpc.NewStreamLayer(nil, byte(api.ClientMessage), dialer)
	return &Client{addr, dialTimeout, sl}
}

func (c *Client) Get(key string) (*store.KVPair, error) {
	req := &api.Request{
		Action: api.Get,
		Key:    key,
	}

	res, err := c.do(req)
	if err != nil {
		return nil, err
	}

	return kvToLibKV(res.KV), nil
}

func (c *Client) Put(key string, value []byte, options *store.WriteOptions) error {
	req := &api.Request{
		Action: api.Put,
		Key:    key,
		Value:  value,
	}
	if options != nil {
		req.TTL = options.TTL
	}

	_, err := c.do(req)
	return err
}

func (c *Client) Delete(key string) error {
	req := &api.Request{
		Action: api.Delete,
		Key:    key,
	}

	_, err := c.do(req)
	return err
}

func (c *Client) Exists(key string) (bool, error) {
	req := &api.Request{
		Action: api.Exists,
		Key:    key,
	}

	res, err := c.do(req)
	return res.Exists, err
}

func (c *Client) List(prefix string) ([]*store.KVPair, error) {
	req := &api.Request{
		Action: api.List,
		Key:    prefix,
	}

	res, err := c.do(req)
	if err != nil {
		return nil, err
	}

	var ls []*store.KVPair
	for _, kv := range res.List {
		ls = append(ls, kvToLibKV(kv))
	}
	if len(ls) == 0 {
		return nil, store.ErrKeyNotFound
	}

	return ls, nil
}

func (c *Client) DeleteTree(dir string) error {
	req := &api.Request{
		Action: api.DeleteTree,
		Key:    dir,
	}

	_, err := c.do(req)
	return err
}

func (c *Client) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	return nil, errNotImplemented
}

func (c *Client) WatchTree(dir string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, errNotImplemented
}

func (c *Client) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, errNotImplemented
}

func (c *Client) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	req := api.Request{
		Action:   api.AtomicPut,
		Key:      key,
		Value:    value,
		Previous: libkvToKV(previous),
		TTL:      options.TTL,
	}

	res, err := c.do(&req)
	if err != nil {
		return false, nil, err
	}
	return res.Completed, kvToLibKV(res.KV), nil
}

func (c *Client) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	req := api.Request{
		Action:   api.AtomicDelete,
		Key:      key,
		Previous: libkvToKV(previous),
	}

	res, err := c.do(&req)
	if err != nil {
		return false, err
	}
	return res.Completed, nil
}

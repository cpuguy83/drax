package client

import (
	"crypto/tls"
	"errors"
	"time"

	"github.com/docker/docker/pkg/raftkv/api"
	"github.com/docker/libkv/store"
)

type Client interface {
	Get(key string) (*store.KVPair, error)
	Put(key string, value []byte, options *store.WriteOptions) error
	Delete(key string) error
	Exists(key string) (bool, error)
	List(prefix string) ([]*store.KVPair, error)
	DeleteTree(dir string) error
	Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error)
	WatchTree(dir string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error)
	NewLock(key string, options *store.LockOptions) (store.Locker, error)
	AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error)
	AtomicDelete(key string, previous *store.KVPair) (bool, error)
}

var errNotImplemented = errors.New("Call not implemented in current backend")

type client struct {
	addr        string
	tlsConfig   *tls.Config
	dialTimeout time.Duration
}

func New(addr string, tlsConfig *tls.Config, dialTimeout time.Duration) Client {
	return &client{addr, tlsConfig, dialTimeout}
}

func (c *client) Get(key string) (*store.KVPair, error) {
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

func (c *client) Put(key string, value []byte, options *store.WriteOptions) error {
	req := &api.Request{
		Action: api.Put,
		Key:    key,
		Value:  value,
		TTL:    options.TTL,
	}

	_, err := c.do(req)
	return err
}

func (c *client) Delete(key string) error {
	req := &api.Request{
		Action: api.Delete,
		Key:    key,
	}

	_, err := c.do(req)
	return err
}

func (c *client) Exists(key string) (bool, error) {
	req := &api.Request{
		Action: api.Exists,
		Key:    key,
	}

	res, err := c.do(req)
	return res.Exists, err
}

func (c *client) List(prefix string) ([]*store.KVPair, error) {
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

func (c *client) DeleteTree(dir string) error {
	req := &api.Request{
		Action: api.DeleteTree,
		Key:    dir,
	}

	_, err := c.do(req)
	return err
}

func (c *client) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	return nil, errNotImplemented
}

func (c *client) WatchTree(dir string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	return nil, errNotImplemented
}

func (c *client) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, errNotImplemented
}

func (c *client) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
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

func (c *client) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
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

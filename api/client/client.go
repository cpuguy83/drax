package client

import (
	"errors"
	"io"
	"time"

	"github.com/cpuguy83/drax/api"
	"github.com/cpuguy83/drax/rpc"
	"github.com/docker/libkv/store"
)

var errNotImplemented = errors.New("Call not implemented in current backend")

// Client is used to facilitate communications with a drax cluster
type Client struct {
	addr         string
	streamLayer  *rpc.StreamLayer
	retryTimeout time.Duration
}

// New creates a new drax Client
func New(addr string, retryTimeout time.Duration, dialer rpc.DialerFn) *Client {
	sl := rpc.NewStreamLayer(nil, byte(api.ClientMessage), dialer)
	return &Client{addr, sl, retryTimeout}
}

// Get gets the k/v pair for the specified key
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

// Put sets the key to the given value
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

// Delete deletes the key
func (c *Client) Delete(key string) error {
	req := &api.Request{
		Action: api.Delete,
		Key:    key,
	}

	_, err := c.do(req)
	return err
}

// Exists checks for the existence of a key
func (c *Client) Exists(key string) (bool, error) {
	req := &api.Request{
		Action: api.Exists,
		Key:    key,
	}

	res, err := c.do(req)
	if err != nil {
		return false, err
	}
	return res.Exists, nil
}

// List lists the key/value pairs that have the provided key prefix
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

// DeleteTree deletes the specified dir and all subdirs
func (c *Client) DeleteTree(dir string) error {
	req := &api.Request{
		Action: api.DeleteTree,
		Key:    dir,
	}

	_, err := c.do(req)
	return err
}

// Watch watches a key for changes and notifies the caller
// Watch is not implemented
func (c *Client) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	req := &api.Request{
		Action: api.Watch,
		Key:    key,
	}
	conn, err := c.stream(req)
	if err != nil {
		return nil, err
	}

	var kv api.KVPair
	dec := api.NewDecoder(conn)
	chKV := make(chan *store.KVPair)
	go func() {
		defer conn.Close()
		for {
			select {
			case <-stopCh:
				return
			default:
				if err := dec.Decode(&kv); err != nil {
					if err != io.EOF {
						dec = api.NewDecoder(conn)
						continue
					}
					return
				}
				select {
				case chKV <- kvToLibKV(&kv):
				case <-stopCh:
					return
				}
			}
		}
	}()

	return chKV, nil
}

// WatchTree watches a dir and all subdirs for changes and notifies the caller
func (c *Client) WatchTree(dir string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	req := &api.Request{
		Action: api.WatchTree,
		Key:    dir,
	}
	conn, err := c.stream(req)
	if err != nil {
		return nil, err
	}

	var kv []*api.KVPair
	dec := api.NewDecoder(conn)
	chKV := make(chan []*store.KVPair)
	go func() {
		defer conn.Close()
		for {
			select {
			case <-stopCh:
				return
			default:
				if err := dec.Decode(&kv); err != nil {
					if err != io.EOF {
						dec = api.NewDecoder(conn)
						continue
					}
					return
				}
				var kvList []*store.KVPair
				for _, apiKv := range kv {
					kvList = append(kvList, kvToLibKV(apiKv))
				}
				select {
				case chKV <- kvList:
				case <-stopCh:
					return
				}
			}
		}
	}()
	return chKV, err
}

// NewLock creates a new lock that can be used to lock access to the given key, like a sync.Mutex
func (c *Client) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	return nil, errNotImplemented
}

// AtomicPut is like `Put`, but ensures there are no changes to the key while the action is performed
// If the key is changed, this returns an error and does not perorm the requested change
func (c *Client) AtomicPut(key string, value []byte, previous *store.KVPair, options *store.WriteOptions) (bool, *store.KVPair, error) {
	req := api.Request{
		Action:   api.AtomicPut,
		Key:      key,
		Value:    value,
		Previous: libkvToKV(previous),
	}
	if options != nil {
		req.TTL = options.TTL
	}

	res, err := c.do(&req)
	if err != nil {
		return false, nil, err
	}
	return res.Completed, kvToLibKV(res.KV), nil
}

// AtomicDelete is like `Delete`, but makes sure the key is not changed while performing the action.
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

// Close closes the underlying stream layer
func (c *Client) Close() { c.streamLayer.Close() }

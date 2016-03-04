package client

import (
	"fmt"
	"io"
	"net"

	"github.com/cpuguy83/drax/api"
	"github.com/cpuguy83/drax/api/errors"
	"github.com/docker/distribution/registry/api/errcode"
	"github.com/docker/libkv/store"
)

var errcodeToErr = map[errcode.ErrorCode]error{
	errors.StoreKeyNotFound: store.ErrKeyNotFound,
	errors.StoreKeyModified: store.ErrKeyModified,
}

func (c *Client) dial() (net.Conn, error) {
	return c.streamLayer.DialWithRetry(c.addr, c.retryTimeout, true)
}

func (c *Client) do(req *api.Request) (*api.Response, error) {
	conn, err := c.dial()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := api.Encode(&req, conn); err != nil {
		return nil, err
	}

	var res api.Response
	if err := api.Decode(&res, conn); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}
	if res.Err != nil {
		return nil, getErr(res.Err)
	}
	return &res, nil
}

func (c *Client) stream(req *api.Request) (io.ReadCloser, error) {
	conn, err := c.dial()
	if err != nil {
		return nil, err
	}
	if err := api.Encode(&req, conn); err != nil {
		return nil, err
	}
	return conn, nil
}

func kvToLibKV(kv *api.KVPair) *store.KVPair {
	return &store.KVPair{
		Key:       kv.Key,
		Value:     kv.Value,
		LastIndex: kv.LastIndex,
	}
}

func libkvToKV(kv *store.KVPair) *api.KVPair {
	if kv == nil {
		return nil
	}
	return &api.KVPair{
		Key:       kv.Key,
		Value:     kv.Value,
		LastIndex: kv.LastIndex,
	}
}

func getErr(e *errcode.Error) error {
	err, exists := errcodeToErr[e.Code]
	if !exists {
		return e
	}
	return err
}

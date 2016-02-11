package client

import (
	"fmt"
	"io"
	"net"

	"github.com/cpuguy83/drax/api"
	"github.com/docker/libkv/store"
)

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
		return nil, *res.Err
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
	return &api.KVPair{
		Key:       kv.Key,
		Value:     kv.Value,
		LastIndex: kv.LastIndex,
	}
}

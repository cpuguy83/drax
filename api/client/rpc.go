package client

import (
	"fmt"
	"net"

	"github.com/cpuguy83/drax/api"
	"github.com/docker/libkv/store"
)

func (c *client) dial() (net.Conn, error) {
	return api.Dial(c.addr, api.ClientMessage, c.dialTimeout, c.tlsConfig, true)
}

func (c *client) do(req *api.Request) (*api.Response, error) {
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
	if res.Err != "" {
		return nil, fmt.Errorf("store error: %s", res.Err)
	}
	return &res, nil
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

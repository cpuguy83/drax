# Drax
An embeddable distributed k/v store for Go

Drax is a distributed k/v store based on raft (specifically https://github.com/hashicorp/raft)
The intent is to be relatively light-weight and embeddable, while implementing the API from https://github.com/docker/libkv

Drax is NOT intended for production use. Use at your own risk.
This should be considered ALPHA quality.

Boltdb is used to persist raft logs (and as such the K/V data)

### Usage
For running the server, see `cmd/kv.go` as an example to get going.

**Server Side**
```go
  // on node 1
  listener, _ := net.Listen("tcp", "10.0.0.1:2380")
  peerAddr := ""
  cluster, _ := drax.New(listener, "/var/lib/drax", listener.Addr().String(), peerAddr, &tls.Config{})


  // on node 2
  listener, _ := net.Listen("tcp", "10.0.0.2:2380")
  peerAddr := "10.0.0.1:2380"
  cluster, _ := drax.New(listener, "/var/lib/drax", listener.Addr().String(), peerAddr, &tls.Config{})
```

A node that does not specify a peer, and that does not already contain a peer in it's peer store
will be setup in single node mode and made the leader. Once it is the leader it will exit single-node mode.

You can join a cluster by specifying **any** active peer's address, it does not need to be the leader.

**Client side**

```go
  dialTimeout := 5*time.Second
  client := client.New("10.0.0.1:2380", &tls.Config{}, dialTimeout)
  kvPair, err := client.Get("/foo")
```

Requests to the K/V store can be sent to any peer and it will be forwarded to the leader.

###TODO:
- Add tests
- Add support for (libkv) watches
- Imrpove RPC semantics
- Look at using something other than JSON for encoding/decoding K/V messages, and RPC messages
- Add more documentation (both in code and examples)
- Implement cluster management, adding/removing nodes to the store cluster as needed

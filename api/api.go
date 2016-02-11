package api

import (
	"time"

	"github.com/docker/distribution/registry/api/errcode"
)

// MessageType is used for routing connections to the appropriate handler
type MessageType byte

// ClientAction is used to route client requests to the appropriate handler
type ClientAction string

const (
	// RPCMessage is used the MessageType used for none-to-node communication
	RPCMessage MessageType = 10 + iota
	// ClientMessage is the MessageType used for communication from KV clients
	ClientMessage
)

// ClientActions used to handle requests from clients
const (
	Get          ClientAction = "get"
	Put          ClientAction = "put"
	Delete       ClientAction = "delete"
	Exists       ClientAction = "exists"
	List         ClientAction = "list"
	DeleteTree   ClientAction = "deleteTree"
	Watch        ClientAction = "watch"
	WatchTree    ClientAction = "watchTree"
	NewLock      ClientAction = "newLock"
	AtomicPut    ClientAction = "atomicPut"
	AtomicDelete ClientAction = "atomicDelete"
)

// Request is used by kv clients when making requests
type Request struct {
	Action   ClientAction
	Key      string
	Value    []byte
	TTL      time.Duration
	Previous *KVPair
	Args     []string
}

// KVPair stores the key,value data for use in the Request/Response
type KVPair struct {
	Key       string
	Value     []byte
	LastIndex uint64
}

// Response is the response sent to client requests
type Response struct {
	Err       *errcode.Error
	KV        *KVPair
	List      []*KVPair
	Exists    bool
	Completed bool
}

package api

import "time"

type MessageType byte
type ClientAction string

const (
	RPCMessage MessageType = 10 + iota
	ClientMessage

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

type Request struct {
	Action   ClientAction
	Key      string
	Value    []byte
	TTL      time.Duration
	Previous *KVPair
	Args     []string
}

type KVPair struct {
	Key       string
	Value     []byte
	LastIndex uint64
}

type Response struct {
	Err       string
	KV        *KVPair
	List      []*KVPair
	Exists    bool
	Completed bool
}

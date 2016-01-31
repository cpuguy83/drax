package drax

import (
	"github.com/cpuguy83/drax/api"
	"github.com/cpuguy83/drax/rpc"
)

const (
	reapKeys   = "reapKeys"
	addNode    = "addNode"
	removeNode = "removeNode"
	raftApply  = "raftApply"

	raftMessage api.MessageType = iota
)

type rpcHandlerFunc func(request *rpc.Request) error

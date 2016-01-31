package drax

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cpuguy83/drax/rpc"
	"github.com/docker/docker/pkg/pubsub"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

type Raft struct {
	r      *raft.Raft
	trans  *raft.NetworkTransport
	peers  *peerStoreWrapper
	stream *rpc.StreamLayer
	db     interface {
		Close() error
	}
	shutdownCh chan struct{}
	store      *store
	pub        *pubsub.Publisher
}

var raftStateTopic = func(v interface{}) bool {
	_, ok := v.(raft.RaftState)
	return ok
}

func newRaft(home, addr string, peerStore *peerStoreWrapper, fsm raft.FSM, trans *raft.NetworkTransport, cfg *raft.Config) (*Raft, error) {
	if err := os.MkdirAll(home, 0600); err != nil {
		return nil, err
	}
	db, err := raftboltdb.NewBoltStore(filepath.Join(home, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("error initializing raft db: %v", err)
	}

	snapStore, err := raft.NewFileSnapshotStore(filepath.Join(home, "snapshots"), 5, nil)
	if err != nil {
		return nil, fmt.Errorf("error intializing raft snap store: %v", err)
	}

	r, err := raft.NewRaft(cfg, fsm, db, db, snapStore, peerStore, trans)
	if err != nil {
		return nil, err
	}

	raft := &Raft{
		r:          r,
		peers:      peerStore,
		trans:      trans,
		db:         db,
		shutdownCh: make(chan struct{}),
		pub:        pubsub.NewPublisher(defaultTimeout, 1),
	}
	go raft.waitLeader()
	return raft, nil
}

func (r *Raft) getLeader() string {
	leader := r.Leader()
	if leader == "" {
		// best effort to wait for a leader
		ticker := time.NewTicker(250 * time.Millisecond)
		for range ticker.C {
			leader = r.Leader()
			if leader != "" {
				break
			}
		}
		ticker.Stop()
	}
	return leader
}

func (r *Raft) Close() error {
	if err := r.r.Shutdown().Error(); err != nil {
		return err
	}
	r.db.Close()
	r.trans.Close()
	r.store.Close()
	return nil
}

func (r *Raft) Apply(b []byte) error {
	leader := r.r.Leader()
	if leader == "" {
		<-r.pub.SubscribeTopic(raftStateTopic)
	}

	if r.IsLeader() {
		return r.r.Apply(b, defaultTimeout).Error()
	}

	res, err := r.stream.RPC(leader, &rpc.Request{
		Method: raftApply,
		Args:   []string{string(b)},
	})
	if err != nil {
		return err
	}
	if res.Err != "" {
		return fmt.Errorf(res.Err)
	}
	return nil
}

func (r *Raft) IsLeader() bool {
	return r.r.State() == raft.Leader
}

func (r *Raft) AddPeer(peer string) error {
	return r.r.AddPeer(peer).Error()
}

func (r *Raft) RemovePeer(peer string) error {
	return r.r.RemovePeer(peer).Error()
}

func (r *Raft) Peers() ([]string, error) {
	return r.peers.Peers()
}

func (r *Raft) SetPeers(peers []string) error {
	return r.r.SetPeers(peers).Error()
}

func (r *Raft) LeaderCh() <-chan interface{} {
	return r.pub.SubscribeTopic(raftStateTopic)
}

func (r *Raft) ShutdownCh() <-chan struct{} {
	return r.shutdownCh
}

func (r *Raft) Leader() string {
	return r.r.Leader()
}

func (r *Raft) waitLeader() {
	for {
		select {
		case <-r.r.LeaderCh():
			r.pub.Publish(r.r.State())
		case <-r.shutdownCh:
			return
		}
	}
}

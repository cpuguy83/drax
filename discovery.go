package raftkv

import (
	"sync"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
)

type peerStoreWrapper struct {
	peerStore raft.PeerStore
}

func (p *peerStoreWrapper) SetPeers(peers []string) error {
	if err := p.peerStore.SetPeers(peers); err != nil {
		return err
	}
	return nil
}

func (p *peerStoreWrapper) Peers() ([]string, error) {
	return p.peerStore.Peers()
}

func newPeerStore(home string, transport raft.Transport) *peerStoreWrapper {
	return &peerStoreWrapper{
		raft.NewJSONPeers(home, transport),
	}
}

type peers struct {
	sync.Mutex
	peers  map[string]struct{}
	notify chan string
	db     *bolt.DB
}

func (s *peers) Peers() ([]string, error) {
	s.Lock()
	var peers []string
	for p := range s.peers {
		peers = append(peers, p)
	}
	s.Unlock()
	return peers, nil
}

func (s *peers) SetPeers(peers []string) error {
	s.Lock()
	s.peers = make(map[string]struct{})
	for _, p := range peers {
		s.peers[p] = struct{}{}
	}
	s.Unlock()
	return nil
}

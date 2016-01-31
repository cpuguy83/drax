package drax

import "github.com/hashicorp/raft"

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

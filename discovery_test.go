package drax

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/hashicorp/raft"
)

func TestSetPeers(t *testing.T) {
	home, err := ioutil.TempDir("", "test-peers")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(home)
	_, trans := raft.NewInmemTransport()
	p := newPeerStore(home, trans)
	peers, err := p.Peers()
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 0 {
		t.Fatalf("expected peers to be empty but got %v", peers)
	}

	peer := "1.2.3.4:1234"
	err = p.SetPeers([]string{peer})
	if err != nil {
		t.Fatal(err)
	}

	peers, err = p.Peers()
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %v", peers)
	}
	if peers[0] != peer {
		t.Fatalf("expected peer %s, got %s", peer, peer[0])
	}
}

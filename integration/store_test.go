package integration

import (
	"testing"

	"github.com/cpuguy83/drax"
	"github.com/cpuguy83/drax/api/errors"
	"github.com/docker/distribution/registry/api/errcode"
)

func TestStoreGetPutDelete(t *testing.T) {
	nodes, err := newTestCluster(3, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupNodes(nodes)

	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	// Test key doesn't exist
	for _, n := range nodes {
		_, err := n.KVStore().Get("hello")
		if err == nil {
			t.Fatalf("expected error, got none")
		}

		switch e := err.(type) {
		case errcode.Error:
			if e.Code != errors.StoreKeyNotFound {
				t.Fatalf("expected %q, got %q", errors.StoreKeyNotFound.Error(), err)
			}
		default:
			if err != drax.ErrKeyNotFound {
				t.Fatalf("exepcted %v, got %v", drax.ErrKeyNotFound, err)
			}
		}
	}

	if err := nodes[0].KVStore().Put("hello", []byte("world"), nil); err != nil {
		t.Fatal(err)
	}

	for i, n := range nodes {
		kv, err := n.KVStore().Get("hello")
		if err != nil {
			t.Fatalf("fail on node %d: %v", i+1, err)
		}

		if string(kv.Value) != "world" {
			t.Fatalf("expected world, got %s", kv.Value)
		}
	}

	if err := nodes[0].KVStore().Delete("hello"); err != nil {
		t.Fatal(err)
	}

	// Test key doesn't exist
	for _, n := range nodes {
		_, err := n.KVStore().Get("hello")
		if err == nil {
			t.Fatalf("expected error, got none")
		}

		switch e := err.(type) {
		case errcode.Error:
			if e.Code != errors.StoreKeyNotFound {
				t.Fatalf("expected %q, got %q", errors.StoreKeyNotFound.Error(), err)
			}
		default:
			if err != drax.ErrKeyNotFound {
				t.Fatalf("exepcted %v, got %v", drax.ErrKeyNotFound, err)
			}
		}
	}
}

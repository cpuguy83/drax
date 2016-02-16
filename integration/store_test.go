package integration

import (
	"testing"
	"time"

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

func TestStoreWatch(t *testing.T) {
	nodes, err := newTestCluster(3, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupNodes(nodes)

	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	chStop := make(chan struct{})
	chKV, err := nodes[0].KVStore().Watch("hello", chStop)
	if err != nil {
		t.Fatal(err)
	}

	if err := nodes[1].KVStore().Put("hello", []byte("world"), nil); err != nil {
		t.Fatal(err)
	}

	select {
	case kv, open := <-chKV:
		if !open {
			t.Fatal("watch chan should be open")
		}
		if kv.Key != "hello" || string(kv.Value) != "world" {
			t.Fatalf("got wrong k/v pair: %s=%s", kv.Key, string(kv.Value))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for watch key")
	}

	if err := nodes[2].KVStore().Put("hello", []byte("world2"), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case kv, open := <-chKV:
		if !open {
			t.Fatal("watch chan should be open")
		}
		if kv.Key != "hello" || string(kv.Value) != "world2" {
			t.Fatalf("got wrong k/v pair: %s=%s", kv.Key, string(kv.Value))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for watch key")
	}

	close(chStop)
	if err := nodes[2].KVStore().Put("hello", []byte("world3"), nil); err != nil {
		t.Fatal(err)
	}

	select {
	case _, open := <-chKV:
		if open {
			t.Fatalf("exepected watch chan to be closeD")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for watch to close")
	}
}

func TestStoreWatchDelete(t *testing.T) {
	nodes, err := newTestCluster(3, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupNodes(nodes)

	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	chKV, err := nodes[0].KVStore().Watch("hello", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := nodes[1].KVStore().Put("hello", []byte("world"), nil); err != nil {
		t.Fatal(err)
	}

	select {
	case kv, open := <-chKV:
		if !open {
			t.Fatal("watch chan should be open")
		}
		if kv.Key != "hello" || string(kv.Value) != "world" {
			t.Fatalf("got wrong k/v pair: %s=%s", kv.Key, string(kv.Value))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for watch key")
	}

	if err := nodes[2].KVStore().Delete("hello"); err != nil {
		t.Fatal(err)
	}

	select {
	case _, open := <-chKV:
		if open {
			t.Fatalf("exepected watch chan to be closeD")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for watch to close")
	}
}

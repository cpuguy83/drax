package drax

import (
	"testing"
	"time"

	libkvstore "github.com/docker/libkv/store"
	"github.com/hashicorp/raft"
)

type testRaft struct {
	leader bool
	s      *store
	index  uint64
}

func (r *testRaft) IsLeader() bool { return r.leader }

func (r *testRaft) LeaderCh() <-chan interface{} {
	ch := make(chan interface{}, 1)
	if r.leader {
		ch <- raft.Leader
	}
	return ch
}

func (r *testRaft) GetLeader() string {
	return "test"
}

func (r *testRaft) ShutdownCh() <-chan struct{} {
	return make(chan struct{})
}

func (r *testRaft) Apply(b []byte) error {
	l := &raft.Log{
		Index: r.index,
		Term:  uint64(0),
		Type:  raft.LogCommand,
		Data:  b,
	}
	if e := (*storeFSM)(r.s).Apply(l); e != nil {
		return e.(error)
	}
	l.Index++
	return nil
}

func TestStoreGet(t *testing.T) {
	s := newStore()
	s.r = &testRaft{true, s, 0}

	if _, err := s.Get("hello"); err == nil {
		t.Fatal("expected error getting non-existent key")
	}

	if err := s.Put("hello", []byte("world"), nil); err != nil {
		t.Fatal(err)
	}

	kv, err := s.Get("hello")
	if err != nil {
		t.Fatal(err)
	}
	if kv.Key != "hello" {
		t.Fatalf("exepcted key `hello`, got %s", kv.Key)
	}
	if string(kv.Value) != "world" {
		t.Fatalf("expected value `world`, got %s", string(kv.Value))
	}
}

func TestStorePut(t *testing.T) {
	s := newStore()
	s.r = &testRaft{true, s, 0}

	if err := s.Put("hello", []byte("world"), nil); err != nil {
		t.Fatal(err)
	}
	kv, err := s.Get("hello")
	if err != nil {
		t.Fatal(err)
	}
	if kv.Key != "hello" {
		t.Fatalf("exepcted key `hello`, got %s", kv.Key)
	}
	if string(kv.Value) != "world" {
		t.Fatalf("expected value `world`, got %s", string(kv.Value))
	}
}

func TestStorePutWithTTL(t *testing.T) {
	s := newStore()
	s.r = &testRaft{true, s, 0}
	go s.waitLeader()

	// Currently a 1 second resolution on TTL's
	if err := s.Put("hello", []byte("world"), &libkvstore.WriteOptions{TTL: 1 * time.Second}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Get("hello"); err != nil {
		t.Fatal(err)
	}

	ticker := time.Tick(50 * time.Millisecond)
	timeout := time.After(2 * time.Second)
	for range ticker {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for TTL'd key to be deleted")
		default:
		}
		if _, err := s.Get("hello"); err != nil {
			break
		}
	}
}

func TestStoreDelete(t *testing.T) {
	s := newStore()
	s.r = &testRaft{true, s, 0}

	if err := s.Put("hello", []byte("world"), nil); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Get("hello"); err != nil {
		t.Fatal(err)
	}

	if err := s.Delete("hello"); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Get("hello"); err == nil {
		t.Fatal("expected error looking up deleted key")
	}
}

func TestStoreExists(t *testing.T) {
	s := newStore()
	s.r = &testRaft{true, s, 0}

	exists, err := s.Exists("hello")
	if err != nil {
		t.Fatal(err)
	}

	if exists {
		t.Fatal("expected key to not exist")
	}

	if err := s.Put("hello", []byte("world"), nil); err != nil {
		t.Fatal(err)
	}

	exists, err = s.Exists("hello")
	if err != nil {
		t.Fatal(err)
	}

	if !exists {
		t.Fatal("expected key to exist")
	}
}

func TestStoreList(t *testing.T) {
	s := newStore()
	s.r = &testRaft{true, s, 0}

	if _, err := s.List("hello"); err == nil {
		t.Fatal("expected error looking up non-existent key")
	}

	if err := s.Put("hello", []byte("world"), nil); err != nil {
		t.Fatal(err)
	}

	if err := s.Put("hello/world", []byte("bam"), nil); err != nil {
		t.Fatal(err)
	}

	if err := s.Put("hello/batman", []byte("pow"), nil); err != nil {
		t.Fatal(err)
	}

	ls, err := s.List("hello")
	if err != nil {
		t.Fatal(err)
	}

	if len(ls) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(ls))
	}
}

func TestStoreDeleteTree(t *testing.T) {
	s := newStore()
	s.r = &testRaft{true, s, 0}

	if err := s.Put("hello", []byte("world"), nil); err != nil {
		t.Fatal(err)
	}

	if err := s.Put("hello/world", []byte("bam"), nil); err != nil {
		t.Fatal(err)
	}

	if err := s.Put("hello/batman", []byte("pow"), nil); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteTree("hello"); err != nil {
		t.Fatal(err)
	}

	if _, err := s.List("hello"); err == nil {
		t.Fatal("expected error on listing non-existig tree")
	}
}

package drax

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cpuguy83/drax/api"
	libkvstore "github.com/docker/libkv/store"
)

type testRaft struct {
	leader bool
	s      *store
}

func (r *testRaft) IsLeader() bool { return r.leader }

func (r *testRaft) LeaderCh() <-chan interface{} {
	ch := make(chan interface{})
	if r.leader {
		close(ch)
	}
	return ch
}

func (r *testRaft) GetLeader() string {
	return "test"
}

func (r *testRaft) ShutdownCh() <-chan struct{} {
	return make(chan struct{})
}

// TODO: decouple raft handling from the store itself
func (r *testRaft) Apply(b []byte) error {
	buf := bytes.NewBuffer(b)
	var req api.Request
	if err := api.Decode(&req, buf); err != nil {
		return err
	}

	switch req.Action {
	case api.Put:
		r.s.data.KV[req.Key] = &libkvstore.KVPair{
			Key:   req.Key,
			Value: req.Value,
		}
	case api.Delete:
		delete(r.s.data.KV, req.Key)
	case api.DeleteTree:
		for k := range r.s.data.KV {
			if !strings.HasPrefix(k, req.Key) {
				continue
			}
			delete(r.s.data.KV, k)
			delete(r.s.data.TTLs, k)
		}

	default:
		return fmt.Errorf("unknown action")
	}
	return nil
}

func TestStoreGet(t *testing.T) {
	s := newStore()
	s.r = &testRaft{true, s}

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
		t.Fatal("expected value `world`, got %s", string(kv.Value))
	}
}

func TestStorePut(t *testing.T) {
	s := newStore()
	s.r = &testRaft{true, s}

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
		t.Fatal("expected value `world`, got %s", string(kv.Value))
	}
}

func TestStoreDelete(t *testing.T) {
	s := newStore()
	s.r = &testRaft{true, s}

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
	s.r = &testRaft{true, s}

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
	s.r = &testRaft{true, s}

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
	s.r = &testRaft{true, s}

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

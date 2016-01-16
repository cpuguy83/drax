package drax

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/cpuguy83/drax/api"
	"github.com/cpuguy83/drax/api/client"
	libkvstore "github.com/docker/libkv/store"
	"github.com/hashicorp/raft"
)

type store struct {
	mu      sync.RWMutex
	ttlLock sync.Mutex
	data    *db
	r       *Raft
}

type ttl struct {
	TTL         time.Duration
	CreateTime  time.Time
	CreateIndex uint64
}

type db struct {
	KV   map[string]*libkvstore.KVPair
	TTLs map[string]*ttl
}

func newDB() *db {
	return &db{KV: make(map[string]*libkvstore.KVPair), TTLs: make(map[string]*ttl)}
}

func (s *store) newClient() client.Client {
	leader := s.r.getLeader()
	return client.New(leader, s.r.tlsConfig, defaultTimeout)
}

func newStore() *store {
	return &store{data: newDB()}
}

func (s *store) Get(key string) (*libkvstore.KVPair, error) {
	if s.r.IsLeader() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.get(key)
	}
	return s.newClient().Get(key)
}

func (s *store) get(key string) (*libkvstore.KVPair, error) {
	kv, ok := s.data.KV[key]
	if !ok {
		return nil, libkvstore.ErrKeyNotFound
	}
	return kv, nil
}

// TODO: support TTL from libkvstore.WriteOptions
func (s *store) Put(key string, value []byte, options *libkvstore.WriteOptions) error {
	if !s.r.IsLeader() {
		return s.newClient().Put(key, value, options)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	req := &api.Request{
		Action: api.Put,
		Key:    key,
		Value:  value,
	}
	if options != nil {
		req.TTL = options.TTL
	}
	return s.apply(req)
}

func (s *store) Delete(key string) error {
	if !s.r.IsLeader() {
		return s.newClient().Delete(key)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.apply(&api.Request{
		Action: api.Delete,
		Key:    key,
	})
}

func (s *store) Exists(key string) (bool, error) {
	if !s.r.IsLeader() {
		return s.newClient().Exists(key)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.data.KV[key]
	return ok, nil
}

func (s *store) List(prefix string) ([]*libkvstore.KVPair, error) {
	if !s.r.IsLeader() {
		return s.newClient().List(prefix)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []*libkvstore.KVPair

	for k, v := range s.data.KV {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		var kv libkvstore.KVPair
		kv = *v
		out = append(out, &kv)
	}

	if len(out) == 0 {
		return nil, libkvstore.ErrKeyNotFound
	}
	return out, nil
}

func (s *store) DeleteTree(dir string) error {
	if !s.r.IsLeader() {
		return s.newClient().DeleteTree(dir)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.apply(&api.Request{
		Action: api.DeleteTree,
		Key:    dir,
	})
}

func (s *store) Watch(key string, stopCh <-chan struct{}) (<-chan *libkvstore.KVPair, error) {
	if !s.r.IsLeader() {
		return s.newClient().Watch(key, stopCh)
	}
	return nil, libkvstore.ErrCallNotSupported
}

func (s *store) WatchTree(dir string, stopCh <-chan struct{}) (<-chan []*libkvstore.KVPair, error) {
	if !s.r.IsLeader() {
		return s.newClient().WatchTree(dir, stopCh)
	}
	return nil, libkvstore.ErrCallNotSupported
}

func (s *store) NewLock(key string, options *libkvstore.LockOptions) (libkvstore.Locker, error) {
	if !s.r.IsLeader() {
		return s.newClient().NewLock(key, options)
	}
	return nil, libkvstore.ErrCallNotSupported
}

func (s *store) AtomicPut(key string, value []byte, previous *libkvstore.KVPair, options *libkvstore.WriteOptions) (bool, *libkvstore.KVPair, error) {
	if !s.r.IsLeader() {
		return s.newClient().AtomicPut(key, value, previous, options)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	kv, err := s.get(key)
	if err != nil {
		if previous != nil && err == libkvstore.ErrKeyNotFound {
			return false, nil, libkvstore.ErrKeyModified
		}
		return false, nil, err
	}

	if previous != nil && kv.LastIndex != previous.LastIndex {
		return false, nil, libkvstore.ErrKeyModified
	}

	req = &api.Request{
		Action: api.Put,
		Key:    key,
		Value:  value,
	}
	if options != nil {
		req.TTL = options.TTL
	}
	if err := s.apply(req); err != nil {
		return false, nil, err
	}

	kv, err = s.get(key)
	if err != nil {
		return false, nil, libkvstore.ErrKeyNotFound
	}
	return true, kv, nil
}

func (s *store) AtomicDelete(key string, previous *libkvstore.KVPair) (bool, error) {
	if !s.r.IsLeader() {
		return s.newClient().AtomicDelete(key, previous)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if previous == nil {
		return false, libkvstore.ErrPreviousNotSpecified
	}

	kv, err := s.get(key)
	if err != nil {
		if err == libkvstore.ErrKeyModified {
			return false, err
		}
	}
	if kv.LastIndex != previous.LastIndex {
		return false, libkvstore.ErrKeyModified
	}
	if err := s.apply(&api.Request{
		Action: api.Delete,
		Key:    key,
	}); err != nil {
		return false, err
	}
	return true, nil
}

func (s *store) Close() {
	return
}

func (s *store) forwardToLeader(ax *api.Request) error {
	leader := s.r.Leader()
	if leader == "" {
		return raft.ErrNotLeader
	}

	return raft.ErrNotLeader
}

func (s *store) apply(ax *api.Request) error {
	if !s.r.IsLeader() {
		return s.forwardToLeader(ax)
	}
	buf := bytes.NewBuffer(nil)
	if err := api.Encode(ax, buf); err != nil {
		return err
	}
	return s.r.Apply(buf.Bytes())
}

func (s *store) waitLeader() {
	leaderCh := s.r.LeaderCh()
	logrus.Debug("store: waiting for leader")
	var state raft.RaftState
	for {
		select {
		case si := <-leaderCh:
			state = si.(raft.RaftState)
		case <-s.r.ShutdownCh():
			return
		}

		if state != raft.Leader {
			continue
		}
		logrus.Debug("store: handling leader")
		s.handleLeader(leaderCh)
		logrus.Debugf("store: waiting for leader")
	}
}

func (s *store) handleLeader(leaderCh <-chan interface{}) {
	for {
		select {
		case state := <-leaderCh:
			if state != raft.Leader {
				return
			}
		case <-s.r.ShutdownCh():
			return
		default:
		}
		time.Sleep(1 * time.Second)

		s.ttlLock.Lock()
		var keys []string
		for k, ttl := range s.data.TTLs {
			if ttlDue(ttl) {
				keys = append(keys, k)
			}
		}
		if len(keys) > 0 {
			logrus.Debugf("reaping TTL's for %v", keys)
			s.reapKeys(keys)
		}
		s.ttlLock.Unlock()
	}
}

func (s *store) reapKeys(keys []string) {
	if err := s.apply(&api.Request{
		Action: reapKeys,
		Args:   keys,
	}); err != nil {
		logrus.Debugf("error reaping keys: %v", err)
	}
}

func ttlDue(t *ttl) bool {
	now := time.Now()
	return now.After(t.CreateTime.Add(t.TTL))
}

type storeFSM store

// TODO: handle watches
func (s *storeFSM) Apply(l *raft.Log) interface{} {
	var ax api.Request
	if err := api.Decode(&ax, bytes.NewBuffer(l.Data)); err != nil {
		return err
	}

	switch ax.Action {
	case api.Delete:
		delete(s.data.KV, ax.Key)
		delete(s.data.TTLs, ax.Key)
	case api.Put:
		s.data.KV[ax.Key] = &libkvstore.KVPair{Key: ax.Key, Value: ax.Value, LastIndex: l.Index}
		if ax.TTL != 0 {
			s.ttlLock.Lock()
			s.data.TTLs[ax.Key] = &ttl{CreateTime: time.Now(), TTL: ax.TTL, CreateIndex: l.Index}
			s.ttlLock.Unlock()
		}
	case api.DeleteTree:
		for k := range s.data.KV {
			if !strings.HasPrefix(k, ax.Key) {
				continue
			}
			delete(s.data.KV, k)
			delete(s.data.TTLs, k)
		}
	case reapKeys:
		s.mu.Lock()
		for _, k := range ax.Args {
			delete(s.data.KV, k)
			delete(s.data.TTLs, k)
		}
		s.mu.Unlock()
	default:
		return fmt.Errorf("unknown api.Request")
	}
	return nil
}

func (s *storeFSM) Snapshot() (raft.FSMSnapshot, error) {
	return s, nil
}

func (s *storeFSM) Restore(r io.ReadCloser) error {
	defer r.Close()
	s.data = newDB()
	return api.Decode(s.data, r)
}

func (s *storeFSM) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	return api.Encode(s.data, sink)
}

func (*storeFSM) Release() {}

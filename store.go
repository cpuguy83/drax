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
	"github.com/cpuguy83/drax/rpc"
	libkvstore "github.com/docker/libkv/store"
	"github.com/hashicorp/raft"
)

var (
	// ErrKeyNotFound - Key does not exist in the store
	ErrKeyNotFound = libkvstore.ErrKeyNotFound
	// ErrKeyModified - Key was modified during atomic operation
	ErrKeyModified = libkvstore.ErrKeyModified
	// ErrCallNotSupported - call is not supported
	ErrCallNotSupported = libkvstore.ErrCallNotSupported

	wgPool = sync.Pool{New: func() interface{} { return new(sync.WaitGroup) }}
)

type raftCluster interface {
	IsLeader() bool
	LeaderCh() <-chan interface{}
	GetLeader() string
	ShutdownCh() <-chan struct{}
	Apply([]byte) error
}

type store struct {
	mu          sync.RWMutex
	watchLock   sync.RWMutex
	ttlLock     sync.Mutex
	data        *db
	r           raftCluster
	dialer      rpc.DialerFn
	watches     map[string]*watch
	treeWatches map[string]*treeWatch
}

type watch struct {
	watchers map[chan *libkvstore.KVPair]struct{}
	sync.RWMutex
	closed  bool
	timeout time.Duration
}

type treeWatch struct {
	watchers map[chan []*libkvstore.KVPair]struct{}
	sync.RWMutex
	closed  bool
	timeout time.Duration
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

func (s *store) newClient() *client.Client {
	leader := s.r.GetLeader()
	return client.New(leader, defaultTimeout, s.dialer)
}

func newStore() *store {
	return &store{data: newDB(), watches: make(map[string]*watch), treeWatches: make(map[string]*treeWatch)}
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
		return nil, ErrKeyNotFound
	}
	return kv, nil
}

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
		return nil, ErrKeyNotFound
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

	s.watchLock.Lock()
	defer s.watchLock.Unlock()

	waitStop := func(w *watch, stopCh <-chan struct{}, ch chan *libkvstore.KVPair) {
		<-stopCh
		w.Evict(ch)
	}

	if w, exists := s.watches[key]; exists {
		ch := w.Subscribe()
		go waitStop(w, stopCh, ch)
		return ch, nil
	}
	w := &watch{watchers: make(map[chan *libkvstore.KVPair]struct{})}
	ch := w.Subscribe()
	s.watches[key] = w
	go waitStop(w, stopCh, ch)
	return ch, nil
}

func (s *store) WatchTree(dir string, stopCh <-chan struct{}) (<-chan []*libkvstore.KVPair, error) {
	if !s.r.IsLeader() {
		return s.newClient().WatchTree(dir, stopCh)
	}
	s.watchLock.Lock()
	defer s.watchLock.Unlock()

	waitStop := func(w *treeWatch, stopCh <-chan struct{}, ch chan []*libkvstore.KVPair) {
		<-stopCh
		w.Evict(ch)
	}

	if w, exists := s.treeWatches[dir]; exists {
		ch := w.Subscribe()
		go waitStop(w, stopCh, ch)
		return ch, nil
	}

	w := &treeWatch{watchers: make(map[chan []*libkvstore.KVPair]struct{})}
	s.treeWatches[dir] = w
	ch := w.Subscribe()
	go waitStop(w, stopCh, ch)
	return ch, nil
}

func (s *store) NewLock(key string, options *libkvstore.LockOptions) (libkvstore.Locker, error) {
	if !s.r.IsLeader() {
		return s.newClient().NewLock(key, options)
	}
	return nil, ErrCallNotSupported
}

func (s *store) AtomicPut(key string, value []byte, previous *libkvstore.KVPair, options *libkvstore.WriteOptions) (bool, *libkvstore.KVPair, error) {
	if !s.r.IsLeader() {
		return s.newClient().AtomicPut(key, value, previous, options)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	kv, err := s.get(key)
	if err != nil {
		if previous != nil && err == ErrKeyNotFound {
			return false, nil, ErrKeyModified
		}
	}

	if previous != nil && kv.LastIndex != previous.LastIndex {
		return false, nil, ErrKeyModified
	}

	req := &api.Request{
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
		if err == ErrKeyModified {
			return false, err
		}
	}
	if kv.LastIndex != previous.LastIndex {
		return false, ErrKeyModified
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

func (s *store) apply(ax *api.Request) error {
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

func (s *storeFSM) Apply(l *raft.Log) interface{} {
	var ax api.Request
	if err := api.Decode(&ax, bytes.NewBuffer(l.Data)); err != nil {
		return err
	}

	switch ax.Action {
	case api.Delete:
		delete(s.data.KV, ax.Key)
		delete(s.data.TTLs, ax.Key)
		s.closeWatches(ax.Key)
	case api.Put:
		kv := &libkvstore.KVPair{Key: ax.Key, Value: ax.Value, LastIndex: l.Index}
		s.data.KV[ax.Key] = kv
		if ax.TTL != 0 {
			s.ttlLock.Lock()
			s.data.TTLs[ax.Key] = &ttl{CreateTime: time.Now(), TTL: ax.TTL, CreateIndex: l.Index}
			s.ttlLock.Unlock()
		}
		s.checkWatches(ax.Key, kv)
		s.checkTreeWatches(ax.Key, []*libkvstore.KVPair{kv})
	case api.DeleteTree:
		for k := range s.data.KV {
			if !strings.HasPrefix(k, ax.Key) {
				continue
			}
			delete(s.data.KV, k)
			delete(s.data.TTLs, k)
			s.closeWatches(ax.Key)
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

func (s *storeFSM) checkWatches(key string, kv *libkvstore.KVPair) {
	s.watchLock.Lock()
	w, exists := s.watches[key]
	s.watchLock.Unlock()
	if !exists {
		return
	}
	if kv == nil {
		w.Close()
		return
	}
	w.Publish(kv)
}

func (s *storeFSM) checkTreeWatches(key string, kv []*libkvstore.KVPair) {
	s.watchLock.Lock()
	defer s.watchLock.Unlock()

	for dir, w := range s.treeWatches {
		if !strings.HasPrefix(key, dir) {
			continue
		}
		go w.Publish(kv)
	}
}

func (s *storeFSM) closeWatches(key string) {
	s.watchLock.Lock()
	if w, exists := s.watches[key]; exists {
		w.Close()
		delete(s.watches, key)
	}

	for dir, w := range s.treeWatches {
		// key is dir
		if dir == key {
			delete(s.treeWatches, key)
			w.Close()
			continue
		}

		// dir is underneath key, recurrsive delete
		if strings.HasPrefix(dir, key) {
			delete(s.treeWatches, key)
			w.Close()
		}
	}
	s.watchLock.Unlock()
}

func (w *treeWatch) Subscribe() chan []*libkvstore.KVPair {
	ch := make(chan []*libkvstore.KVPair, 1000)
	w.Lock()
	w.watchers[ch] = struct{}{}
	w.Unlock()
	return ch
}

func (w *treeWatch) Publish(kv []*libkvstore.KVPair) {
	w.RLock()
	defer w.RUnlock()

	if len(w.watchers) == 0 {
		return
	}

	wg := wgPool.Get().(*sync.WaitGroup)
	wg.Add(len(w.watchers))
	for s := range w.watchers {
		go func(s chan []*libkvstore.KVPair) {
			defer wg.Done()
			select {
			case <-time.After(w.timeout):
				return
			case s <- kv:
			}
		}(s)
	}

	wg.Wait()
}

func (w *treeWatch) Evict(s chan []*libkvstore.KVPair) {
	w.Lock()
	delete(w.watchers, s)
	close(s)
	w.Unlock()
}

func (w *watch) Subscribe() chan *libkvstore.KVPair {
	ch := make(chan *libkvstore.KVPair, 1000)
	w.Lock()
	w.watchers[ch] = struct{}{}
	w.Unlock()
	return ch
}

func (w *watch) Publish(kv *libkvstore.KVPair) {
	w.RLock()
	defer w.RUnlock()

	if len(w.watchers) == 0 {
		return
	}

	wg := wgPool.Get().(*sync.WaitGroup)
	wg.Add(len(w.watchers))
	for s := range w.watchers {
		go func(s chan *libkvstore.KVPair) {
			defer wg.Done()
			select {
			case <-time.After(w.timeout):
				return
			case s <- kv:
			}
		}(s)
	}

	wg.Wait()
}

func (w *watch) Evict(s chan *libkvstore.KVPair) {
	w.Lock()
	delete(w.watchers, s)
	close(s)
	w.Unlock()
}

func (w *watch) Close() {
	w.Lock()
	defer w.Unlock()

	for s := range w.watchers {
		close(s)
		delete(w.watchers, s)
	}
}

func (w *treeWatch) Close() {
	w.Lock()
	defer w.Unlock()

	for s := range w.watchers {
		close(s)
		delete(w.watchers, s)
	}
}

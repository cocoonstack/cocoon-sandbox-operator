// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package queue

import (
	"container/list"
	"sync"
)

// SandboxKey uniquely identifies a sandbox in the queue.
type SandboxKey struct {
	Namespace string
	Name      string
	NodeName  string
}

// Strategy picks one key from a warm pool's queued sandboxes; false leaves the queue untouched.
type Strategy func([]SandboxKey) (SandboxKey, bool)

// SimpleSandboxQueue is a thread-safe queue of adoptable warm pool sandboxes.
type SimpleSandboxQueue struct {
	// queues is a thread-safe dictionary from warm pool name to a synchronizedQueue
	queues sync.Map
}

// NewSimpleSandboxQueue initializes a new SimpleSandboxQueue.
func NewSimpleSandboxQueue() *SimpleSandboxQueue {
	return &SimpleSandboxQueue{}
}

// Add pushes an item to the specific warm pool's queue.
func (s *SimpleSandboxQueue) Add(namespacedWarmPoolName string, item SandboxKey) {
	q, _ := s.queues.LoadOrStore(namespacedWarmPoolName, newSynchronizedQueue())
	q.(*synchronizedQueue).Push(item)
}

// GetWithStrategy pops an item from the specific warm pool's queue using a custom strategy.
func (s *SimpleSandboxQueue) GetWithStrategy(namespacedWarmPoolName string, pick Strategy) (SandboxKey, bool) {
	q, ok := s.queues.Load(namespacedWarmPoolName)
	if !ok {
		return SandboxKey{}, false
	}
	return q.(*synchronizedQueue).PopWithStrategy(pick)
}

// Len reports how many sandboxes a warm pool's queue holds.
func (s *SimpleSandboxQueue) Len(namespacedWarmPoolName string) int {
	q, ok := s.queues.Load(namespacedWarmPoolName)
	if !ok {
		return 0
	}
	return q.(*synchronizedQueue).Len()
}

// RemoveItem deletes a specific sandbox from a warm pool's queue.
func (s *SimpleSandboxQueue) RemoveItem(namespacedWarmPoolName string, item SandboxKey) {
	if q, ok := s.queues.Load(namespacedWarmPoolName); ok {
		q.(*synchronizedQueue).Remove(item)
	}
}

// RemoveQueue completely deletes a warm pool's queue from the sync.Map
// to prevent memory leaks when SandboxTemplates or WarmPools are deleted.
func (s *SimpleSandboxQueue) RemoveQueue(namespacedWarmPoolName string) {
	s.queues.Delete(namespacedWarmPoolName)
}

// synchronizedQueue is one warm pool's FIFO of adoptable sandbox keys, indexed
// by key so push and remove touch one element instead of scanning the pool;
// RemoveQueue drops it when its SandboxWarmPool goes away.
type synchronizedQueue struct {
	mu    sync.Mutex
	order *list.List
	items map[string]*list.Element
}

func newSynchronizedQueue() *synchronizedQueue {
	return &synchronizedQueue{order: list.New(), items: map[string]*list.Element{}}
}

// Push appends an item, refreshing NodeName on a key already queued: placement
// may have settled since, and its arrival position must not change.
func (q *synchronizedQueue) Push(key SandboxKey) {
	q.mu.Lock()
	defer q.mu.Unlock()
	id := uniqueID(key)
	if el, exists := q.items[id]; exists {
		el.Value = key
		return
	}
	q.items[id] = q.order.PushBack(key)
}

// PopWithStrategy removes and returns the key pick selects, in arrival order.
// pick is a pure in-memory function, so it runs under the lock: no snapshot to
// race against, and no re-verify retry.
func (q *synchronizedQueue) PopWithStrategy(pick Strategy) (SandboxKey, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.order.Len() == 0 {
		return SandboxKey{}, false
	}
	keys := make([]SandboxKey, 0, q.order.Len())
	for el := q.order.Front(); el != nil; el = el.Next() {
		keys = append(keys, el.Value.(SandboxKey))
	}
	key, ok := pick(keys)
	if !ok {
		return SandboxKey{}, false
	}
	q.removeLocked(uniqueID(key))
	return key, true
}

func (q *synchronizedQueue) Remove(key SandboxKey) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.removeLocked(uniqueID(key))
}

func (q *synchronizedQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.order.Len()
}

// removeLocked drops one key's row and index entry. Callers hold mu.
func (q *synchronizedQueue) removeLocked(id string) {
	el, exists := q.items[id]
	if !exists {
		return
	}
	q.order.Remove(el)
	delete(q.items, id)
}

// GetNamespacedWarmPoolName forms the namespace-aware index value to use as a key to a SimpleSandboxQueue type.
func GetNamespacedWarmPoolName(namespace, warmPoolName string) string {
	return namespace + "/" + warmPoolName
}

// uniqueID identifies a queued sandbox independently of its placement, so a
// re-Add with a settled NodeName updates the row instead of duplicating it.
func uniqueID(key SandboxKey) string {
	return key.Namespace + "/" + key.Name
}

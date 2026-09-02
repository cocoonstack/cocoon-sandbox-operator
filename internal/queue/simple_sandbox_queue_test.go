// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package queue

import (
	"testing"
)

func TestSimpleSandboxQueue_BasicOperations(t *testing.T) {
	q := NewSimpleSandboxQueue()
	hash := "template-hash-1"

	key1 := SandboxKey{Namespace: "default", Name: "sb-1"}
	key2 := SandboxKey{Namespace: "default", Name: "sb-2"}

	q.Add(hash, key1)
	q.Add(hash, key2)

	got1, ok1 := pop(q, hash)
	if !ok1 || got1 != key1 {
		t.Errorf("Expected %v, got %v (ok: %v)", key1, got1, ok1)
	}

	got2, ok2 := pop(q, hash)
	if !ok2 || got2 != key2 {
		t.Errorf("Expected %v, got %v (ok: %v)", key2, got2, ok2)
	}

	if _, ok3 := pop(q, hash); ok3 {
		t.Errorf("Expected queue to be empty, but got an item")
	}
}

func TestSimpleSandboxQueue_RemoveItem_GhostPodFix(t *testing.T) {
	q := NewSimpleSandboxQueue()
	hash := "template-hash-1"

	key1 := SandboxKey{Namespace: "default", Name: "sb-1"}
	key2 := SandboxKey{Namespace: "default", Name: "sb-2"}
	key3 := SandboxKey{Namespace: "default", Name: "sb-3"}

	q.Add(hash, key1)
	q.Add(hash, key2)
	q.Add(hash, key3)

	q.RemoveItem(hash, key2)

	if got := q.Len(hash); got != 2 {
		t.Errorf("Expected 2 items after removing the middle key, got %d", got)
	}

	got1, _ := pop(q, hash)
	if got1 != key1 {
		t.Errorf("Expected %v, got %v", key1, got1)
	}

	got3, _ := pop(q, hash)
	if got3 != key3 {
		t.Errorf("Expected %v to skip deleted item and return %v, but got %v", hash, key3, got3)
	}

	if _, hasItem := pop(q, hash); hasItem {
		t.Errorf("Expected queue to be empty after Ghost Pod removal")
	}
}

func TestSynchronizedQueue_Deduplication(t *testing.T) {
	q := newSynchronizedQueue()
	key := SandboxKey{Namespace: "default", Name: "duplicate-sb"}

	q.Push(key)
	q.Push(key)
	q.Push(key)

	if got := q.Len(); got != 1 {
		t.Errorf("Expected length 1 due to O(1) deduplication, got %d", got)
	}
	if got := len(q.items); got != 1 {
		t.Errorf("Expected index length 1, got %d", got)
	}
}

func TestSynchronizedQueue_PushKeepsArrivalPositionAndRefreshesNode(t *testing.T) {
	q := newSynchronizedQueue()
	first := SandboxKey{Namespace: "default", Name: "sb-1"}
	second := SandboxKey{Namespace: "default", Name: "sb-2"}

	q.Push(first)
	q.Push(second)
	q.Push(SandboxKey{Namespace: "default", Name: "sb-1", NodeName: "node-a"})

	got, ok := q.PopWithStrategy(func(keys []SandboxKey) (SandboxKey, bool) { return keys[0], true })
	if !ok || got.Name != "sb-1" || got.NodeName != "node-a" {
		t.Errorf("Expected sb-1 first with a refreshed node, got %+v (ok: %v)", got, ok)
	}
}

func TestSimpleSandboxQueue_RemoveQueue_MemoryLeakFix(t *testing.T) {
	q := NewSimpleSandboxQueue()
	hash := "template-hash-to-delete"
	key1 := SandboxKey{Namespace: "default", Name: "sb-1"}

	q.Add(hash, key1)
	q.RemoveQueue(hash)

	if _, ok := pop(q, hash); ok {
		t.Errorf("Expected queue to be completely removed, but it still existed")
	}
}

func TestSimpleSandboxQueue_GetWithStrategy(t *testing.T) {
	q := NewSimpleSandboxQueue()
	hash := "template-hash-1"

	key1 := SandboxKey{Namespace: "default", Name: "sb-1"}
	key2 := SandboxKey{Namespace: "default", Name: "sb-2"}
	key3 := SandboxKey{Namespace: "default", Name: "sb-3"}

	q.Add(hash, key1)
	q.Add(hash, key2)
	q.Add(hash, key3)

	pickKey2 := func(items []SandboxKey) (SandboxKey, bool) {
		for _, item := range items {
			if item.Name == "sb-2" {
				return item, true
			}
		}
		return SandboxKey{}, false
	}

	got, ok := q.GetWithStrategy(hash, pickKey2)
	if !ok || got != key2 {
		t.Errorf("Expected to pick %v, got %v (ok: %v)", key2, got, ok)
	}

	got1, _ := pop(q, hash)
	if got1 != key1 {
		t.Errorf("Expected first remaining item to be %v, got %v", key1, got1)
	}

	got3, _ := pop(q, hash)
	if got3 != key3 {
		t.Errorf("Expected second remaining item to be %v, got %v", key3, got3)
	}

	if _, ok3 := pop(q, hash); ok3 {
		t.Errorf("Expected queue to be empty, but got an item")
	}
}

func TestGetNamespacedWarmPoolName(t *testing.T) {
	namespace := "my-ns"
	wp := "my-wp"
	expected := "my-ns/my-wp"
	got := GetNamespacedWarmPoolName(namespace, wp)
	if got != expected {
		t.Errorf("Expected %q, got %q", expected, got)
	}
}

func TestSimpleSandboxQueue_NoLegacyFallback(t *testing.T) {
	q := NewSimpleSandboxQueue()
	namespace := "my-ns"
	wpName := "my-wp"
	namespacedName := GetNamespacedWarmPoolName(namespace, wpName)

	key1 := SandboxKey{Namespace: namespace, Name: "sb-1"}
	q.Add(namespacedName, key1)

	if _, ok := pop(q, wpName); ok {
		t.Errorf("Expected pop with namespace-agnostic name to fail")
	}

	if _, ok := q.GetWithStrategy(wpName, func(items []SandboxKey) (SandboxKey, bool) { return items[0], true }); ok {
		t.Errorf("Expected GetWithStrategy with namespace-agnostic name to fail")
	}

	q.RemoveItem(wpName, key1)
	if got := q.Len(namespacedName); got != 1 {
		t.Errorf("Expected item to still be queued after RemoveItem with namespace-agnostic name, got %d", got)
	}

	q.RemoveQueue(wpName)
	if got := q.Len(namespacedName); got != 1 {
		t.Errorf("Expected queue to still exist after RemoveQueue with namespace-agnostic name, got %d", got)
	}
}

func pop(q *SimpleSandboxQueue, namespacedWarmPoolName string) (SandboxKey, bool) {
	return q.GetWithStrategy(namespacedWarmPoolName, func(keys []SandboxKey) (SandboxKey, bool) {
		return keys[0], true
	})
}

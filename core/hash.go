package core

import "sync"

//For in-memory storage (not on disc) hashmap should be faster thar b-tree
type HashEngine struct {
	mu sync.RWMutex

	data map[string]*Item
}

// NewHashEngine constructs new  HashEngine instance
func NewHashEngine() *HashEngine {
	return &HashEngine{data: make(map[string]*Item)}
}

// Get returns reference to Item by key. If Item not exists, return nil
func (e *HashEngine) Get(key string) (item *Item) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.data[key]
}

// Get returns *Items mapped to provided keys.
func (e *HashEngine) GetSubmap(keys []string) (submap map[string]*Item) {
	submap = make(map[string]*Item, len(keys))

	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, key := range keys {
		if item, ok := e.data[key]; ok {
			submap[key] = item
		}
	}

	return submap
}

// Keys returns all keys existing in the Engine
func (e *HashEngine) Keys() (keys []string) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	keys = make([]string, 0, len(e.data))
	for k := range e.data {
		keys = append(keys, k)
	}

	return keys
}

// AddOrReplace adds new or replaces existing Items in the engine
func (e *HashEngine) AddOrReplace(items map[string]*Item) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for k, item := range items {
		if item == nil {
			panic("Program Logic error: trying to add nil *Item into Engine")
		}
		e.data[k] = item
	}
}

// Del removes values from engine and returns count of actually removed values
// if key not found in the engine, just skip it
func (e *HashEngine) Del(keys []string) (count int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, k := range keys {
		if _, ok := e.data[k]; ok {
			count++
		}

		delete(e.data, k)
	}

	return count
}

// DelSubmap removes Items only if existing *Item equals to provided submap[key]
// if key not found in the engine, just skip it and returns count of actually deleted items
func (e *HashEngine) DelSubmap(submap map[string]*Item) (count int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for key, item := range submap {
		if existingItem, ok := e.data[key]; ok && existingItem == item {
			count++
			delete(e.data, key)
		}
	}

	return count
}

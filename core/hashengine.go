package core

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/mshaverdo/assert"
	"io"
	"sync"
)

// register HashEngine as Engine implementation for GOB
func init() {
	gob.Register(&HashEngine{})
}

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
		assert.True(item != nil, "trying to add nil *Item into Engine")
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

// Persist dumps storage engine data into provided Writer
func (e *HashEngine) Persist(w io.Writer, lastMessageId int64) error {
	e.fullLock()
	defer e.fullUnlock()

	encoder := gob.NewEncoder(w)

	if err := encoder.Encode(lastMessageId); err != nil {
		return fmt.Errorf("HashEngine.Persist(): can't encode messageId: %s", err)
	}

	exp := &gobExportItem{}
	for k, v := range e.data {
		exp.Key = k
		exp.ExpireAt = v.expireAt
		exp.Kind = v.kind
		exp.Bytes = v.bytes
		exp.List = v.list
		exp.Dict = v.dict

		if err := encoder.Encode(exp); err != nil {
			return fmt.Errorf("HashEngine.Persist(): can't encode messageId: %s", err)
			return err
		}
	}

	return nil
}

// Load loads storage engine data from Reader
func (e *HashEngine) Load(r io.Reader) (lastMessageId int64, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.data) != 0 {
		return 0, errors.New("HashEngine.Load(): restore enabled only on empty engine")
	}

	decoder := gob.NewDecoder(r)

	if err := decoder.Decode(&lastMessageId); err != nil {
		return 0, fmt.Errorf("HashEngine.Load(): can't decode messageId: %s", err)
	}

	e.data = make(map[string]*Item)
	exp := new(gobExportItem)
	for err := decoder.Decode(exp); err != io.EOF; err = decoder.Decode(exp) {
		if err != nil {
			return 0, fmt.Errorf("HashEngine.Load(): can't decode item: %s", err)
		}

		e.data[exp.Key] = new(Item)
		e.data[exp.Key].expireAt = exp.ExpireAt
		e.data[exp.Key].kind = exp.Kind
		e.data[exp.Key].bytes = exp.Bytes
		e.data[exp.Key].list = exp.List
		e.data[exp.Key].dict = exp.Dict

		exp = new(gobExportItem)
	}

	return lastMessageId, nil
}

// FullLock locks engine and all items to ensure exclusive access to its content
func (e *HashEngine) fullLock() {
	e.mu.Lock()

	for _, v := range e.data {
		v.Lock()
	}
}

// FullUnlock unlocks engine and all items
func (e *HashEngine) fullUnlock() {
	for _, v := range e.data {
		v.Unlock()
	}

	e.mu.Unlock()
}

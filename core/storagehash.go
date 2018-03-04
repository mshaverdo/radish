package core

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"sync"
)

//For in-memory storage (not on disc) hashmap should be faster thar b-tree
type StorageHash struct {
	mu sync.RWMutex

	data map[string]*Item
}

// NewStorageHash constructs new  StorageHash instance
func NewStorageHash() *StorageHash {
	return &StorageHash{data: make(map[string]*Item)}
}

// Get returns reference to Item by key. If Item not exists, return nil
func (e *StorageHash) Get(key string) (item *Item) {
	e.mu.RLock()
	item = e.data[key]
	e.mu.RUnlock()
	return item
}

// Get returns *Items mapped to provided keys.
func (e *StorageHash) GetSubmap(keys []string) (submap map[string]*Item) {
	submap = make(map[string]*Item, len(keys))

	e.mu.RLock()
	for _, key := range keys {
		if item, ok := e.data[key]; ok {
			submap[key] = item
		}
	}
	e.mu.RUnlock()

	return submap
}

// Keys returns all keys existing in the Storage
func (e *StorageHash) Keys() (keys []string) {
	e.mu.RLock()
	keys = make([]string, 0, len(e.data))
	for k := range e.data {
		keys = append(keys, k)
	}
	e.mu.RUnlock()

	return keys
}

// AddOrReplaceOne adds new or replaces one existing Item in the storage. It much faster than AddOrReplace with single items
func (e *StorageHash) AddOrReplaceOne(key string, item *Item) {
	e.mu.Lock()
	e.data[key] = item
	e.mu.Unlock()
}

// Del removes values from storage and returns count of actually removed values
// if key not found in the storage, just skip it
func (e *StorageHash) Del(keys []string) (count int) {
	e.mu.Lock()
	for _, k := range keys {
		if _, ok := e.data[k]; ok {
			count++
		}

		delete(e.data, k)
	}
	e.mu.Unlock()

	return count
}

// DelSubmap removes Items only if existing *Item equals to provided submap[key]
// if key not found in the storage, just skip it and returns count of actually deleted items
func (e *StorageHash) DelSubmap(submap map[string]*Item) (count int) {
	e.mu.Lock()
	for key, item := range submap {
		if existingItem, ok := e.data[key]; ok && existingItem == item {
			count++
			delete(e.data, key)
		}
	}
	e.mu.Unlock()

	return count
}

// Persist dumps storage storage data into provided Writer
func (e *StorageHash) Persist(w io.Writer, lastMessageId int64) error {
	e.fullLock()
	defer e.fullUnlock()

	encoder := gob.NewEncoder(w)

	if err := encoder.Encode(lastMessageId); err != nil {
		return fmt.Errorf("StorageHash.Persist(): can't encode messageId: %s", err)
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
			return fmt.Errorf("StorageHash.Persist(): can't encode item: %s", err)
			return err
		}
	}

	return nil
}

// Load loads storage storage data from Reader
func (e *StorageHash) Load(r io.Reader) (lastMessageId int64, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.data) != 0 {
		return 0, errors.New("StorageHash.Load(): restore enabled only on empty storage")
	}

	decoder := gob.NewDecoder(r)

	if err := decoder.Decode(&lastMessageId); err != nil {
		return 0, fmt.Errorf("StorageHash.Load(): can't decode messageId: %s", err)
	}

	e.data = make(map[string]*Item)
	exp := new(gobExportItem)
	for err := decoder.Decode(exp); err != io.EOF; err = decoder.Decode(exp) {
		if err != nil {
			return 0, fmt.Errorf("StorageHash.Load(): can't decode item: %s", err)
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

// FullLock locks storage and all items to ensure exclusive access to its content
func (e *StorageHash) fullLock() {
	e.mu.Lock()

	for _, v := range e.data {
		v.Lock()
	}
}

// FullUnlock unlocks storage and all items
func (e *StorageHash) fullUnlock() {
	for _, v := range e.data {
		v.Unlock()
	}

	e.mu.Unlock()
}

package core

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/OneOfOne/xxhash"
	"io"
	"sync"
)

const (
	bucketsCount = 1024
)

//For in-memory storage (not on disc) hashmap should be faster thar b-tree
// hashmap sharding gives significant performance boost on wide keyspace
// (up to 10x SET on 1M keys & 1k concurrent connects at octa-core cpu)
type StorageHash struct {
	mu [bucketsCount]sync.RWMutex

	data [bucketsCount]map[string]*Item
}

// NewStorageHash constructs new  StorageHash instance
func NewStorageHash() *StorageHash {
	s := &StorageHash{}
	for i := range s.data {
		s.data[i] = make(map[string]*Item)
	}
	return s
}

// Get returns reference to Item by key. If Item not exists, return nil
func (e *StorageHash) Get(key string) (item *Item) {
	b := getBucket(key)
	e.mu[b].RLock()
	item = e.data[b][key]
	e.mu[b].RUnlock()
	return item
}

// Get returns *Items mapped to provided keys.
func (e *StorageHash) GetSubmap(keys []string) (submap map[string]*Item) {
	var keysByBucket [bucketsCount][]string
	for _, key := range keys {
		b := getBucket(key)
		keysByBucket[b] = append(keysByBucket[b], key)
	}

	submap = make(map[string]*Item, len(keys))
	for b, bucketKeys := range keysByBucket {
		if len(bucketKeys) == 0 {
			continue
		}

		e.mu[b].RLock()
		for _, key := range bucketKeys {
			if item, ok := e.data[b][key]; ok {
				submap[key] = item
			}
		}
		e.mu[b].RUnlock()
	}

	return submap
}

// Keys returns all keys existing in the Storage
func (e *StorageHash) Keys() (keys []string) {
	totalLen := 0
	for b := range e.data {
		e.mu[b].RLock()
		totalLen += len(e.data[b])
		e.mu[b].RUnlock()
	}

	//add 1% to avoid whole keys slice reallocation when couple of items added
	keys = make([]string, 0, totalLen+totalLen/100)
	for b := range e.data {
		e.mu[b].RLock()
		for k := range e.data[b] {
			keys = append(keys, k)
		}
		e.mu[b].RUnlock()
	}

	return keys
}

// AddOrReplaceOne adds new or replaces one existing Item in the storage. It much faster than AddOrReplace with single items
func (e *StorageHash) AddOrReplaceOne(key string, item *Item) {
	b := getBucket(key)
	e.mu[b].Lock()
	e.data[b][key] = item
	e.mu[b].Unlock()
}

// Del removes values from storage and returns count of actually removed values
// if key not found in the storage, just skip it
func (e *StorageHash) Del(keys []string) (count int) {
	var keysByBucket [bucketsCount][]string
	for _, key := range keys {
		b := getBucket(key)
		keysByBucket[b] = append(keysByBucket[b], key)
	}

	for b, bucketKeys := range keysByBucket {
		if len(bucketKeys) == 0 {
			continue
		}

		e.mu[b].Lock()
		for _, key := range bucketKeys {
			if _, ok := e.data[b][key]; ok {
				count++
				delete(e.data[b], key)
			}
		}
		e.mu[b].Unlock()
	}

	return count
}

// DelSubmap removes Items only if existing *Item equals to provided submap[key]
// if key not found in the storage, just skip it and returns count of actually deleted items
func (e *StorageHash) DelSubmap(submap map[string]*Item) (count int) {
	var keysByBucket [bucketsCount][]string
	for key := range submap {
		b := getBucket(key)
		keysByBucket[b] = append(keysByBucket[b], key)
	}

	for b, bucketKeys := range keysByBucket {
		if len(bucketKeys) == 0 {
			continue
		}

		e.mu[b].Lock()
		for _, key := range bucketKeys {
			if existingItem, ok := e.data[b][key]; ok && existingItem == submap[key] {
				count++
				delete(e.data[b], key)
			}
		}
		e.mu[b].Unlock()
	}

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
	for _, bucketData := range e.data {
		for k, v := range bucketData {
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
	}

	return nil
}

// Load loads storage storage data from Reader
func (e *StorageHash) Load(r io.Reader) (lastMessageId int64, err error) {
	for b := range e.data {
		e.mu[b].Lock()
		defer e.mu[b].Unlock()

		if len(e.data[b]) != 0 {
			return 0, errors.New("StorageHash.Load(): restore enabled only on empty storage")
		}

		e.data[b] = make(map[string]*Item)
	}

	decoder := gob.NewDecoder(r)

	if err := decoder.Decode(&lastMessageId); err != nil {
		return 0, fmt.Errorf("StorageHash.Load(): can't decode messageId: %s", err)
	}

	exp := new(gobExportItem)
	for err := decoder.Decode(exp); err != io.EOF; err = decoder.Decode(exp) {
		if err != nil {
			return 0, fmt.Errorf("StorageHash.Load(): can't decode item: %s", err)
		}

		bucket := e.data[getBucket(exp.Key)]
		bucket[exp.Key] = new(Item)
		bucket[exp.Key].expireAt = exp.ExpireAt
		bucket[exp.Key].kind = exp.Kind
		bucket[exp.Key].bytes = exp.Bytes
		bucket[exp.Key].list = exp.List
		bucket[exp.Key].dict = exp.Dict

		exp = new(gobExportItem)
	}

	return lastMessageId, nil
}

// FullLock locks storage and all items to ensure exclusive access to its content
func (e *StorageHash) fullLock() {
	for b := range e.data {
		e.mu[b].Lock()
		for _, v := range e.data[b] {
			v.Lock()
		}
	}
}

// FullUnlock unlocks storage and all items
func (e *StorageHash) fullUnlock() {
	for b := range e.data {
		for _, v := range e.data[b] {
			v.Unlock()
		}
		e.mu[b].Unlock()
	}
}

func getBucket(key string) int {
	return int(xxhash.ChecksumString64(key) % bucketsCount)
}

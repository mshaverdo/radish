package core

import (
	"errors"
	"github.com/ryanuber/go-glob"
	"math"
)

//TODO: check performance! check Locks waiting!
//TODO: if Engine.Lock() will be a bottleneck, try to use sharding by engines

// configuration
var (
	// CollectExpiredBatchSize items processed by CollectExpired()  at once, in single mutex lock to reduce mutex lock overhead
	CollectExpiredBatchSize = 100

	// If true, Core.Keys() will check every element to isExpire() end exlude expired keys from return
	KeysCheckTtl = true
)

var (
	// ErrNotFound returned by Core API methods when requested key not found
	ErrNotFound      = errors.New("core: item not found")
	ErrWrongType     = errors.New("core: operation against a key holding the wrong kind of value")
	ErrInvalidParams = errors.New("core: Invalid command arguments")
)

// Engine encapsulates concrete concurrency-safe storage engine  -- Btree, hashmap, etc
type Engine interface {
	// Get returns reference to Item by key. If Item not exists, return nil
	Get(key string) (item *Item)

	// Get returns *Items mapped to provided keys.
	GetSubmap(keys []string) (submap map[string]*Item)

	// AddOrReplace adds new or replaces existing Items in the engine
	AddOrReplace(items map[string]*Item)

	// Del removes Items from engine and returns count of actually removed values
	// if key not found in the engine, just skip it
	Del(keys []string) (count int)

	// DelSubmap removes Items only if existing *Item equals to provided submap[key]
	// if key not found in the engine, just skip it and returns count of actually deleted items
	DelSubmap(submap map[string]*Item) (count int)

	// Keys returns all keys existing in the
	//TODO: check performance in Keys() & CollectExpired(): maybe, it's better to return map[string]*Item to avoid extra Get() in those methods
	Keys() (keys []string)
}

// Core provides domain operations on the storage -- get, set, keys, hset, hdel, etc
type Core struct {
	engine Engine
}

// NewCore constructs new core instance
func NewCore(engine Engine) *Core {
	return &Core{engine: engine}
}

// CollectExpired checks all keys from engine and removes items with expired TTL and return count of actually removed items
func (c *Core) CollectExpired() (count int) {
	//TODO: check performance, it could freeze writing operations for a long time!
	allKeys := c.engine.Keys()

	for len(allKeys) > 0 {
		batchLen := int(math.Min(float64(CollectExpiredBatchSize), float64(len(allKeys))))
		batch := allKeys[:batchLen]
		allKeys = allKeys[batchLen:]

		items := c.engine.GetSubmap(batch)
		expiredItems := map[string]*Item{}
		for key, item := range items {
			item.RLock()
			if item.IsExpired() {
				expiredItems[key] = item
			}
			item.RUnlock()
		}

		count += c.engine.DelSubmap(expiredItems)
	}

	return count
}

// Keys returns all keys matching glob pattern
// Warning: consider KEYS as a command that should only be used in production environments with extreme care.
// It may ruin performance when it is executed against large databases.
func (c *Core) Keys(pattern string) (result []string) {
	allKeys := c.engine.Keys()

	isFresh := func(key string) bool {
		if !KeysCheckTtl {
			return true
		}

		i := c.engine.Get(key)

		if i == nil {
			return false
		}

		i.RLock()
		defer i.RUnlock()
		return !i.IsExpired()
	}

	// pre-allocate slice to avoid reallocation
	filteredKeys := make([]string, 0, len(allKeys))
	for _, key := range allKeys {
		if glob.Glob(pattern, key) && isFresh(key) {
			filteredKeys = append(filteredKeys, key)
		}
	}

	return filteredKeys
}

// Get the value of key. If the key does not exist the special value nil is returned.
// An error is returned if the value stored at key is not a string, because GET only handles string values.
func (c *Core) Get(key string) (result []byte, err error) {
	item := c.getItem(key)
	if item == nil {
		return nil, nil //ErrNotFound
	}

	item.RLock()
	defer item.RUnlock()

	if item.kind != Bytes {
		return nil, ErrWrongType
	}

	bytes := item.Bytes()
	result = make([]byte, len(bytes))
	copy(result, bytes)

	return result, nil
}

// Set key to hold the string value.
// If key already holds a value, it is overwritten, regardless of its type.
// Any previous time to live associated with the key is discarded on successful SET operation.
func (c *Core) Set(key string, value []byte) {
	if value == nil {
		panic("Program Logic error: trying to to insert nil value into the core")
	}
	c.engine.AddOrReplace(map[string]*Item{key: NewItemBytes(value)})
}

// Set key to hold the string value and set key to timeout after a given number of seconds.
// If key already holds a value, it is overwritten, regardless of its type.
// ttl <= 0 leads to deleting record
func (c *Core) SetEx(key string, value []byte, ttl int) {
	if value == nil {
		panic("Program Logic error: trying to to insert nil value into the core")
	}

	if ttl <= 0 {
		//item expired before set, just remove it
		c.Del([]string{key})
		return
	}

	item := NewItemBytes(value)
	item.SetTtl(ttl)
	c.engine.AddOrReplace(map[string]*Item{key: item})
}

// Del Removes the specified keys, ignoring not existing and returns count of actually removed values.
// Due to the system isn't supports replications/slaves,
// we don't need conflict resolution, so we could simplify deletion:
// just remove link to Item from Engine, instead marking 'deleted' and then collect garbage in background, etc
func (c *Core) Del(keys []string) (count int) {
	return c.engine.Del(keys)
}

// DSet Sets field in the hash stored at key to value.
// If key does not exist, a new key holding a hash is created.
// If field already exists in the dict, it is overwritten.
// returns 1 if f field is a new field in the hash and value was set.
// returns 0 if field already exists in the hash and the value was updated.
func (c *Core) DSet(key, field string, value []byte) (count int, err error) {
	if value == nil {
		panic("Program Logic error: trying to to insert nil value into the core")
	}

	item := c.getItem(key)
	if item == nil {
		item = NewItemDict(map[string][]byte{})
		defer func() {
			c.engine.AddOrReplace(map[string]*Item{key: item})
		}()
	}

	item.Lock()
	defer item.Unlock()

	if item.kind != Dict {
		return 0, ErrWrongType
	}

	dict := item.Dict()
	count = 1
	if _, ok := dict[field]; ok {
		count = 0
	}
	dict[field] = value

	return count, nil
}

// DGet Returns the value associated with field in the dict stored at key.
func (c *Core) DGet(key, field string) (result []byte, err error) {
	item := c.getItem(key)
	if item == nil {
		return nil, nil //ErrNotFound
	}

	item.RLock()
	defer item.RUnlock()

	if item.kind != Dict {
		return nil, ErrWrongType
	}

	dict := item.Dict()
	value, ok := dict[field]
	if !ok {
		return nil, nil //ErrNotFound
	}

	result = make([]byte, len(value))
	copy(result, value)

	return result, nil
}

// Returns all field names in the dict stored at key.
func (c *Core) DKeys(key, pattern string) (result []string, err error) {
	item := c.getItem(key)
	if item == nil {
		return nil, nil //ErrNotFound
	}

	item.RLock()
	defer item.RUnlock()

	if item.kind != Dict {
		return nil, ErrWrongType
	}

	dict := item.Dict()

	// pre-allocate slice to avoid reallocation
	filteredKeys := make([]string, 0, len(dict))
	for key := range dict {
		if glob.Glob(pattern, key) {
			filteredKeys = append(filteredKeys, key)
		}
	}

	return filteredKeys, nil
}

// DGetAll Returns all fields and values of the hash stored at key.
// In the returned value, every field name is followed by its value,
// so the length of the reply is twice the size of the hash.
func (c *Core) DGetAll(key string) (result [][]byte, err error) {
	item := c.getItem(key)
	if item == nil {
		return nil, nil //ErrNotFound
	}

	item.RLock()
	defer item.RUnlock()

	if item.kind != Dict {
		return nil, ErrWrongType
	}

	dict := item.Dict()
	result = make([][]byte, 0, 2*len(dict))
	for k, v := range dict {
		keyBytes := []byte(k)
		value := make([]byte, len(v))
		copy(value, v)
		result = append(result, keyBytes, value)
	}

	return result, nil
}

// DDel Removes the specified fields from the hash stored at key.
// Specified fields that do not exist within this hash are ignored.
// If key does not exist, it is treated as an empty hash and this command returns 0.
func (c *Core) DDel(key string, fields []string) (count int, err error) {
	item := c.getItem(key)
	if item == nil {
		return 0, nil
	}

	item.Lock()
	defer item.Unlock()

	if item.kind != Dict {
		return 0, ErrWrongType
	}

	dict := item.Dict()
	for _, field := range fields {
		if _, ok := dict[field]; ok {
			count++
			delete(dict, field)
		}
	}

	return count, nil
}

// Returns the length of the list stored at key.
// If key does not exist, it is interpreted as an empty list and 0 is returned.
// An error is returned when the value stored at key is not a list.
func (c *Core) LLen(key string) (count int, err error) {
	item := c.getItem(key)
	if item == nil {
		return 0, nil //ErrNotFound
	}

	item.RLock()
	defer item.RUnlock()

	if item.kind != List {
		return 0, ErrWrongType
	}

	return len(item.List()), nil
}

// LRange returns the specified elements of the list stored at key.
// The offsets start and stop are zero-based indexes,  with 0 being the first element of the list (the HEAD of the list)
// These offsets can also be negative numbers indicating offsets starting at the end of the list.
// For example, -1 is the last element of the list, -2 the penultimate, and so on.
func (c *Core) LRange(key string, start, stop int) (result [][]byte, err error) {
	item := c.getItem(key)
	if item == nil {
		return nil, nil //ErrNotFound
	}

	item.RLock()
	defer item.RUnlock()

	if item.kind != List {
		return nil, ErrWrongType
	}

	list := item.List()
	lLen := len(list)

	// just return on empty list to avoid further index checks
	if lLen == 0 {
		return [][]byte{}, nil
	}

	if start < 0 {
		start += lLen
	}
	if stop < 0 {
		stop += lLen
	}

	start = int(math.Max(float64(start), 0.0))
	stop = int(math.Min(float64(stop), float64(lLen-1)))

	// after normalizing, next check  also covers start > len(), stop < 0
	if start > stop {
		return [][]byte{}, nil
	}

	//IMPORTANT: by proto, HEAD of the list has index 0, but in the slice storage it is the LAST element of the slice
	startIndex := lLen - 1 - stop
	// don't do -1 due to in GO slicing stops BEFORE stop, and in radish proto range stops AT stop
	stopIndex := lLen - start

	slice := list[startIndex:stopIndex]
	// just return on empty list to avoid further index checks
	if len(slice) == 0 {
		return [][]byte{}, nil
	}

	result = make([][]byte, len(slice))

	// due to in radish HEAD of list has index 0, reverse actual items order in the slice
	for i, v := range slice {
		resultI := len(slice) - 1 - i
		result[resultI] = make([]byte, len(v))
		copy(result[resultI], v)
	}

	return result, nil
}

// LIndex Returns the element at index index in the list stored at key.
// The index is zero-based, 0 points to HEAD of the list.
// Negative indices can be used to designate elements starting at the tail of the list.
// Here, -1 means the last element, -2 means the penultimate and so forth.
// When the value at key is not a list, an error is returned.
func (c *Core) LIndex(key string, index int) (result []byte, err error) {
	item := c.getItem(key)
	if item == nil {
		return nil, nil //ErrNotFound
	}

	item.RLock()
	defer item.RUnlock()

	if item.kind != List {
		return nil, ErrWrongType
	}

	list := item.List()
	lLen := len(list)

	if index < 0 {
		index += lLen
	}

	// it also covers LLen == 0
	if !(0 <= index && index <= lLen-1) {
		return []byte{}, nil
	}

	//IMPORTANT: by proto, HEAD of the list has index 0, but in the slice storage it is the LAST element of the slice
	sliceIndex := lLen - 1 - index

	value := list[sliceIndex]

	result = make([]byte, len(value))
	copy(result, value)

	return result, nil
}

// LSet Sets the list element at index to value.
// The index is zero-based, 0 points to HEAD of the list.
// Negative indices can be used to designate elements starting at the tail of the list.
// Here, -1 means the last element, -2 means the penultimate and so forth.
// An error is returned for out of range indexes.
func (c *Core) LSet(key string, index int, value []byte) (err error) {
	if value == nil {
		panic("Program Logic error: trying to to insert nil value into the core")
	}

	item := c.getItem(key)
	if item == nil {
		// LSet replaces only existing element
		return ErrNotFound
	}

	item.Lock()
	defer item.Unlock()

	if item.kind != List {
		return ErrWrongType
	}

	list := item.List()
	lLen := len(list)

	if index < 0 {
		index += lLen
	}

	// index out of range
	if !(0 <= index && index <= lLen-1) {
		return ErrInvalidParams
	}

	//IMPORTANT: by proto, HEAD of the list has index 0, but in the slice storage it is the LAST element of the slice
	sliceIndex := lLen - 1 - index

	list[sliceIndex] = value

	return nil
}

// LPush Insert all the specified values at the head of the list stored at key.
// If key does not exist, it is created as empty list before performing the push operations.
// When key holds a value that is not a list, an error is returned.
// Multiple Elements are inserted one after the other to the head of the list,
// from the leftmost element to the rightmost element.
// So for instance the command LPush("mylist",  []byte[a b c]) will result into a list containing [c, b, a]
func (c *Core) LPush(key string, values [][]byte) (count int, err error) {
	item := c.getItem(key)
	if item == nil {
		item = NewItemList([][]byte{})
		defer func() {
			c.engine.AddOrReplace(map[string]*Item{key: item})
		}()
	}

	item.Lock()
	defer item.Unlock()

	if item.kind != List {
		return 0, ErrWrongType
	}

	list := item.List()

	for _, v := range values {
		if v == nil {
			panic("Program Logic error: trying to to insert nil value into the core")
		}
	}

	list = append(list, values...)
	item.SetList(list)

	return len(list), nil
}

// LPop Removes and returns the first element of the list stored at key.
func (c *Core) LPop(key string) (result []byte, err error) {
	item := c.getItem(key)
	if item == nil {
		return nil, nil
	}

	item.Lock()
	defer item.Unlock()

	if item.kind != List {
		return nil, ErrWrongType
	}

	list := item.List()

	if len(list) == 0 {
		return nil, nil
	}

	// don't copy result ,due to it will be removed from list
	result = list[len(list)-1]
	list = list[:len(list)-1]
	item.SetList(list)

	return result, nil
}

// Ttl Returns the remaining time to live of a key that has a timeout.
// If key not found, return error, if key found, but has no setted TTL, return -1
func (c *Core) Ttl(key string) (ttl int, err error) {
	item := c.getItem(key)
	if item == nil {
		return 0, ErrNotFound
	}

	item.RLock()
	defer item.RUnlock()

	if !item.HasTtl() {
		return -1, nil
	}

	return item.Ttl(), nil
}

// Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
// Note that calling EXPIRE with a non-positive timeout will result in the key being deleted rather than expired
func (c *Core) Expire(key string, seconds int) (err error) {
	item := c.getItem(key)
	if item == nil {
		return ErrNotFound
	}

	if seconds <= 0 {
		c.Del([]string{key})
		return nil
	}

	item.Lock()
	defer item.Unlock()

	// check IsExpired() one more time inside the critical section, to avoid updating TTL
	// for item, that already prepared to removal by CollectExpired()
	if item.IsExpired() {
		return ErrNotFound
	}

	item.SetTtl(seconds)

	return nil
}

// Persist Removes the existing timeout on key.
func (c *Core) Persist(key string) (err error) {
	item := c.getItem(key)
	if item == nil {
		return ErrNotFound
	}

	item.Lock()
	defer item.Unlock()

	// check IsExpired() one more time inside the critical section, to avoid updating TTL
	// for item, that already prepared to removal by CollectExpired()
	if item.IsExpired() {
		return ErrNotFound
	}

	item.RemoveTtl()

	return nil
}

// warning: it could affect performance due to extra mutex lock.
// if it makes perf. penalty, move  IsExpired() check inside existing Lock() in every API func
func (c *Core) getItem(key string) *Item {
	item := c.engine.Get(key)
	if item == nil {
		return nil
	}

	item.RLock()
	defer item.RUnlock()

	if item.IsExpired() {
		return nil
	}

	return item
}

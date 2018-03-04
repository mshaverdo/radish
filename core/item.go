package core

import (
	"fmt"
	"github.com/mshaverdo/assert"
	"sort"
	"sync"
	"time"
)

//go:generate stringer -type=ItemKind
type ItemKind int

const (
	Bytes ItemKind = iota
	List
	Dict
)

type Item struct {
	sync.RWMutex

	expireAt time.Time

	kind  ItemKind
	bytes []byte
	list  [][]byte
	dict  map[string][]byte
}

func NewItemBytes(value []byte) *Item {
	return &Item{
		kind:  Bytes,
		bytes: value,
		list:  nil,
		dict:  nil,
	}
}

// NewItemString constructs Bytes Item from string argument
func NewItemString(value string) *Item {
	return NewItemBytes([]byte(value))
}

func NewItemList(value [][]byte) *Item {
	return &Item{
		kind:  List,
		bytes: nil,
		list:  value,
		dict:  nil,
	}
}

func NewItemDict(value map[string][]byte) *Item {
	return &Item{
		kind:  Dict,
		bytes: nil,
		list:  nil,
		dict:  value,
	}
}

func (i *Item) Kind() ItemKind {
	return i.kind
}

func (i *Item) Bytes() []byte {
	return i.bytes
}

func (i *Item) SetBytes(v []byte) {
	i.bytes = v
}

func (i *Item) List() [][]byte {
	return i.list
}

func (i *Item) SetList(v [][]byte) {
	i.list = v
}

func (i *Item) Dict() map[string][]byte {
	return i.dict
}

func (i *Item) SetDict(v map[string][]byte) {
	i.dict = v
}

func (i *Item) String() string {
	switch i.kind {
	case Bytes:
		return string(i.bytes)
	case List:
		return fmt.Sprintf("%v", i.list)
	case Dict:
		keys := make([]string, 0, len(i.dict))
		for k := range i.dict {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		result := "["
		delimiter := ""
		for _, k := range keys {
			result += fmt.Sprintf("%s%q: %q", delimiter, k, i.dict[k])
		}
		result += "]"

		return result
	default:
		assert.True(false, "unknown Item.kind: "+i.kind.String())
		return ""
	}
}

func (i *Item) SetTtl(seconds int) {
	i.expireAt = time.Now().Add(time.Duration(seconds) * time.Second)
}

func (i *Item) SetMilliTtl(milliseconds int) {
	i.expireAt = time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
}

func (i *Item) RemoveTtl() {
	i.expireAt = time.Time{}
}

func (i *Item) Ttl() (seconds int) {
	seconds = int(i.expireAt.Sub(time.Now()).Seconds() + 0.5) //round value
	if seconds < 0 {
		seconds = 0
	}

	return seconds
}

func (i *Item) IsExpired() bool {
	return i.HasTtl() && i.expireAt.Before(time.Now())
}

func (i *Item) HasTtl() bool {
	return i.expireAt != time.Time{}
}

type gobExportItem struct {
	Key string

	ExpireAt time.Time
	Kind     ItemKind
	Bytes    []byte
	List     [][]byte
	Dict     map[string][]byte
}

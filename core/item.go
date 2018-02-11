package core

import (
	"fmt"
	"sort"
	"sync"
)

//go:generate stringer -type=ItemKind
type ItemKind int

const (
	Bytes ItemKind = iota
	List
	Dict
)

//TODO: add expireAt field
type Item struct {
	sync.RWMutex

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
	if i.kind != Bytes {
		panic("Program Logic error: trying to get Bytes value on " + i.kind.String())
	}
	return i.bytes
}

func (i *Item) SetBytes(v []byte) {
	if i.kind != Bytes {
		panic("Program Logic error: trying to get Bytes value on " + i.kind.String())
	}
	i.bytes = v
}

func (i *Item) List() [][]byte {
	if i.kind != List {
		panic("Program Logic error: trying to get List value on " + i.kind.String())
	}
	return i.list
}

func (i *Item) SetList(v [][]byte) {
	if i.kind != List {
		panic("Program Logic error: trying to get List value on " + i.kind.String())
	}
	i.list = v
}

func (i *Item) Dict() map[string][]byte {
	if i.kind != Dict {
		panic("Program Logic error: trying to get Dict value on " + i.kind.String())
	}
	return i.dict
}

func (i *Item) SetDict(v map[string][]byte) {
	if i.kind != Dict {
		panic("Program Logic error: trying to get Dict value on " + i.kind.String())
	}
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
		panic("Program Logic error: unknown Item.kind: " + i.kind.String())
	}
}

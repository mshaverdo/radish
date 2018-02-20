package core

import (
	"bytes"
	"encoding/gob"
	"fmt"
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

func (i *Item) SetTtl(seconds int) {
	if seconds <= 0 {
		panic("Program Logic error: Trying to set non-positive TTL. To reset TTL use Item.RemoveTtl()")
	}

	i.expireAt = time.Now().Add(time.Duration(seconds) * time.Second)
}

func (i *Item) SetMilliTtl(milliseconds int) {
	if milliseconds <= 0 {
		panic("Program Logic error: Trying to set non-positive TTL. To reset TTL use Item.RemoveTtl()")
	}

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

//TODO: maybe, it's a good idea to add Version field?
type gobExportItem struct {
	ExpireAt time.Time

	Kind  ItemKind
	Bytes []byte
	List  [][]byte
	Dict  map[string][]byte
}

func (i *Item) GobEncode() ([]byte, error) {
	data := gobExportItem{
		ExpireAt: i.expireAt,

		Kind:  i.kind,
		Bytes: i.bytes,
		List:  i.list,
		Dict:  i.dict,
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(&data)

	return buf.Bytes(), err
}

func (i *Item) GobDecode(gobData []byte) error {
	data := gobExportItem{}

	dec := gob.NewDecoder(bytes.NewReader(gobData))
	err := dec.Decode(&data)

	if err != nil {
		return err
	}

	i.expireAt = data.ExpireAt
	i.kind = data.Kind
	i.bytes = data.Bytes
	i.list = data.List
	i.dict = data.Dict

	return nil
}

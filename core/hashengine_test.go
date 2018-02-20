package core

import (
	"fmt"
	"github.com/go-test/deep"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

func getSampleDataHashEngine() map[string]*Item {
	return map[string]*Item{
		"bytes": NewItemBytes([]byte("Призрак бродит по Европе - призрак коммунизма.")),
		"dict": NewItemDict(map[string][]byte{
			"banana": []byte("mama"),
			"測試":     []byte("別れ、比類のない"),
		}),
		"list": NewItemList([][]byte{
			[]byte("Abba"),
			[]byte("Rammstein"),
			[]byte("KMFDM"),
		}),
		"測": NewItemBytes([]byte("幽霊はヨーロッパを追いかけています - 共産主義の幽霊")),
	}
}

func TestHashEngine_Get(t *testing.T) {
	data := getSampleDataHashEngine()
	e := NewHashEngine()
	e.data = data

	for key, item := range data {
		got := e.Get(key)
		if got != item {
			t.Errorf("Get(%q): got %p want %p (values: %q, %q)", key, got, item, got, item)
		}
	}
}

func TestHashEngine_GetSubmap(t *testing.T) {
	data := getSampleDataHashEngine()

	tests := []struct {
		keys []string
		want map[string]*Item
	}{
		{
			[]string{"bytes", "dict", "測", "404"},
			map[string]*Item{"bytes": data["bytes"], "dict": data["dict"], "測": data["測"]},
		},
	}

	e := NewHashEngine()
	e.data = data

	for _, v := range tests {
		got := e.GetSubmap(v.keys)
		if diff := deep.Equal(got, v.want); diff != nil {
			t.Errorf("GetSubmap(%q): %s\n\ngot:%v\n\nwant:%v", v.keys, diff, got, v.want)
		}
	}
}

func TestHashEngine_AddOrReplace(t *testing.T) {
	tests := []map[string]*Item{
		{"測試": NewItemBytes([]byte("value of 測試")), "list": NewItemBytes([]byte("value of list"))},
	}
	data := getSampleDataHashEngine()
	e := NewHashEngine()
	e.data = data

	for _, v := range tests {
		e.AddOrReplace(v)
		for key, item := range data {
			got := e.Get(key)
			if got != item {
				t.Errorf("Get(%q): got %p want %p (values: %q, %q)", key, got, item, got, item)
			}
		}
	}
}

func TestHashEngine_Keys(t *testing.T) {
	data := getSampleDataHashEngine()
	e := NewHashEngine()
	e.data = data

	want := []string{}
	for key := range data {
		want = append(want, key)
	}

	got := e.Keys()
	sort.Strings(got)
	sort.Strings(want)

	if diff := deep.Equal(got, want); diff != nil {
		t.Errorf("Keys(): %s\n\ngot:%v\n\nwant:%v", diff, got, want)
	}
}

func TestHashEngine_Del(t *testing.T) {
	tests := []struct {
		keys, want []string
	}{
		{[]string{"404", "測"}, []string{"bytes", "dict", "list"}},
		{[]string{"bytes", "dict"}, []string{"list"}},
	}

	data := getSampleDataHashEngine()
	e := NewHashEngine()
	e.data = data

	for _, v := range tests {
		e.Del(v.keys)
		got := e.Keys()

		sort.Strings(got)
		sort.Strings(v.want)
		if diff := deep.Equal(got, v.want); diff != nil {
			t.Errorf("Del(): %s\n\ngot:%v\n\nwant:%v", diff, got, v.want)
		}
	}
}

func TestHashEngine_DelSubmap(t *testing.T) {
	data := getSampleDataHashEngine()

	tests := []struct {
		submap    map[string]*Item
		wantCount int
		wantKeys  []string
	}{
		{
			map[string]*Item{"404": nil, "測": data["bytes"], "list": data["list"]},
			1,
			[]string{"bytes", "dict", "測"},
		},
		{
			map[string]*Item{"測": nil, "dict": data["dict"], "bytes": data["bytes"]},
			2,
			[]string{"測"},
		},
	}

	e := NewHashEngine()
	e.data = data

	for _, v := range tests {
		count := e.DelSubmap(v.submap)
		got := e.Keys()

		sort.Strings(got)
		sort.Strings(v.wantKeys)

		if count != v.wantCount {
			t.Errorf("DelSubmap(%q) count: %d != %d", v.submap, count, v.wantCount)
		}

		if diff := deep.Equal(got, v.wantKeys); diff != nil {
			t.Errorf("DelSubmap(%q): %s\n\ngot:%v\n\nwant:%v", v.submap, diff, got, v.wantKeys)
		}
	}
}

func TestHashEngine_concurrency(t *testing.T) {
	tests := [][]string{
		{"aa", "bb", "cc"},
		{"aa", "bb", "cc", "測", "測試"},
		{"測", "別れ、比類のない", "hhh"},
		{"aa", "bb", "cc", "測", "測試"},
	}

	var keys []string
	for i := 0; i < 100; i++ {
		keys = append(keys, fmt.Sprintf("%d", rand.Uint64()))
	}
	tests = append(tests, keys)

	e := NewHashEngine()
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go hashEngineWorker(&wg, e, tests)
	}

	wg.Wait()

	// Due to last operation of every hashEngineWorker is AddOrReplace() for last keyset
	// after all workers done, only last keyset  should remain in the engine
	got := e.Keys()
	want := tests[len(tests)-1]
	sort.Strings(got)
	sort.Strings(want)
	if diff := deep.Equal(got, want); diff != nil {
		t.Errorf("Keys(): %s\n\ngot:%v\n\nwant:%v", diff, got, want)
	}
}

func hashEngineWorker(wg *sync.WaitGroup, e *HashEngine, tests [][]string) {
	var items map[string]*Item
	for _, v := range tests {
		items = map[string]*Item{}
		for _, key := range v {
			items[key] = NewItemBytes([]byte(time.Now().String()))
			e.Get(key)
		}

		e.AddOrReplace(items)
		e.GetSubmap(v[1:3])
		e.Keys()
		e.DelSubmap(map[string]*Item{"404": nil, v[0]: items[v[0]], v[1]: items[v[1]]})
		e.Del(v)
	}
	e.AddOrReplace(items)

	wg.Done()
}

package core

import (
	"fmt"
	"github.com/go-test/deep"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHashEngine_AddOrReplace_Get(t *testing.T) {
	tests := []map[string]*Item{
		{
			"b": NewItemBytes([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
			"⌘": NewItemBytes([]byte("測試")),
			"l": NewItemList([][]byte{
				{1, 2, 3, 4, 5},
				{1, 2, 3},
				{},
			}),
			"d": NewItemDict(map[string][]byte{
				"測":  {1, 2, 3, 4, 5},
				"":   {1, 2, 3},
				"測試": {},
			}),
		},
		{
			"b":   NewItemBytes([]byte(strings.Repeat("test - 測試", 10000))),
			"⌘測試": NewItemBytes([]byte("")),
			"l": NewItemList([][]byte{
				{10, 20},
				{11, 22, 33},
				{44},
				{},
			}),
			"d": NewItemDict(map[string][]byte{
				"測":  {1},
				"":   {2},
				"測試": {3},
			}),
		},
	}

	e := NewHashEngine()
	for _, test := range tests {
		e.AddOrReplace(test)
		for k, want := range test {
			got := e.Get(k)
			if got.String() != want.String() {
				t.Errorf("Get(%q): got %q want %q", k, got.String(), want.String())
			}
		}
	}
}

func TestHashEngine_Keys_Del(t *testing.T) {
	tests := [][]string{
		{"aa", "bb", "cc", "測", "測試"},
		{"aa", "bb", "測", "別れ、比類のない"},
	}

	e := NewHashEngine()

	existingKeys := map[string]bool{}
	want := []string{}

	// test Keys()
	for _, test := range tests {
		items := map[string]*Item{}
		for _, key := range test {
			items[key] = NewItemBytes([]byte(time.Now().String()))
			if !existingKeys[key] {
				existingKeys[key] = true
				want = append(want, key)
			}
		}

		e.AddOrReplace(items)
		got := e.Keys()

		sort.Strings(got)
		sort.Strings(want)
		if diff := deep.Equal(got, want); diff != nil {
			t.Errorf("Atfer AddOrReplace() Keys() != want: %s\n\ngot:%v\n\nwant:%v", diff, got, want)
		}
	}

	// test Del()
	for _, test := range tests {
		for _, key := range test {
			delete(existingKeys, key)
		}

		want = []string{}
		for key := range existingKeys {
			want = append(want, key)
		}

		e.Del(test)
		got := e.Keys()

		sort.Strings(got)
		sort.Strings(want)
		if diff := deep.Equal(got, want); diff != nil {
			t.Errorf("Atfer Del() Keys() != want: %s\n\ngot:%v\n\nwant:%v", diff, got, want)
		}
	}
}

func TestHashEngine_concurrency(t *testing.T) {
	tests := [][]string{
		{"aa", "bb", "cc"},
		{"aa", "bb", "cc", "測", "測試"},
		{"測", "別れ、比類のない"},
		{"aa", "bb", "cc", "測", "測試"},
	}

	var keys []string
	for i := 0; i < 1000; i++ {
		keys = append(keys, fmt.Sprintf("%d", rand.Uint64()))
	}
	tests = append(tests, keys)

	e := NewHashEngine()
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go worker(&wg, e, tests)
	}

	wg.Wait()

	// Due to last operation of every worker is AddOrReplace() for last keyset
	// after all workers done, only last keyset  should remain in the engine
	got := e.Keys()
	want := tests[len(tests)-1]
	sort.Strings(got)
	sort.Strings(want)
	if diff := deep.Equal(got, want); diff != nil {
		t.Errorf("Keys() got != Keys() want: %s\n\ngot:%v\n\nwant:%v", diff, got, want)
	}
}

func worker(wg *sync.WaitGroup, e *HashEngine, tests [][]string) {
	var items map[string]*Item
	for _, test := range tests {
		items = map[string]*Item{}
		for _, key := range test {
			items[key] = NewItemBytes([]byte(time.Now().String()))
			e.Get(key)
		}

		e.AddOrReplace(items)
		e.Keys()
		e.Del(test)
	}
	e.AddOrReplace(items)

	wg.Done()
}

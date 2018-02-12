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

type MockEngine struct {
	data map[string]*Item
}

func getSampleData() map[string]*Item {
	return map[string]*Item{
		"bytes": NewItemBytes([]byte("Призрак бродит по Европе - призрак коммунизма.")),
		"dict": NewItemDict(map[string][]byte{
			"banana": []byte("mama"),
			"測試":     []byte("別れ、比類のない"),
		}),
		"list": NewItemList([][]byte{
			//IMPORTANT: by proto, HEAD of the list has index 0, but in the slice storage it is the LAST element of the slice
			[]byte("Abba"),
			[]byte("Rammstein"),
			[]byte("KMFDM"),
		}),
		"測": NewItemBytes([]byte("幽霊はヨーロッパを追いかけています - 共産主義の幽霊")),
	}
}

func NewMockEngine() *MockEngine {
	return &MockEngine{data: getSampleData()}
}

func (e *MockEngine) Get(key string) (item *Item) {
	return e.data[key]
}

func (e *MockEngine) Keys() (keys []string) {
	keys = make([]string, 0, len(e.data))
	for k := range e.data {
		keys = append(keys, k)
	}

	return keys
}

func (e *MockEngine) AddOrReplace(items map[string]*Item) {
	for k, item := range items {
		e.data[k] = item
	}
}

func (e *MockEngine) Del(keys []string) (count int) {
	for _, k := range keys {
		if _, ok := e.data[k]; ok {
			count++
		}

		delete(e.data, k)
	}

	return count
}

/////////////////////  Tests  ///////////////////////////

func TestCore_Keys(t *testing.T) {
	tests := []struct {
		pattern string
		want    []string
	}{
		{"*", []string{"bytes", "dict", "list", "測"}},
		{"bytes", []string{"bytes"}},
		{"*i*", []string{"dict", "list"}},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		got := c.Keys(v.pattern)
		sort.Strings(got)
		sort.Strings(v.want)

		if diff := deep.Equal(got, v.want); diff != nil {
			t.Errorf("Keys(%q): %s\n\ngot:%v\n\nwant:%v", v.pattern, diff, got, v.want)
		}
	}
}

func TestCore_Get(t *testing.T) {
	tests := []struct {
		key  string
		err  error
		want string
	}{
		{"bytes", nil, "Призрак бродит по Европе - призрак коммунизма."},
		{"測", nil, "幽霊はヨーロッパを追いかけています - 共産主義の幽霊"},
		{"404", nil, ""},
		{"dict", ErrWrongType, ""},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		got, err := c.Get(v.key)
		if err != v.err {
			t.Errorf("Get(%q) err: %q != %q", v.key, err, v.err)
		}
		if string(got) != v.want {
			t.Errorf("Get(%q) err: %q != %q", v.key, string(got), v.want)
		}
	}
}

func TestCore_Set(t *testing.T) {
	tests := []struct {
		key   string
		value string
	}{
		{"bytes", "Ктулху фхтагн!"},
		{"new 測", "共産主義の幽霊"},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		c.Set(v.key, []byte(v.value))
		got, err := c.Get(v.key)
		if err != nil {
			t.Errorf("Set(%q) err: %q != %q", v.key, err, nil)
		}
		if string(got) != v.value {
			t.Errorf("Set(%q) got: %q != %q", v.key, string(got), v.value)
		}
	}
}

func TestCore_Del(t *testing.T) {
	tests := []struct {
		keys []string
		want []string
	}{
		{[]string{"bytes", "list", "404"}, []string{"dict", "測"}},
		{[]string{"dict", "測"}, []string{}},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		c.Del(v.keys)
		got := c.Keys("*")
		sort.Strings(got)
		sort.Strings(v.want)

		if diff := deep.Equal(got, v.want); diff != nil {
			t.Errorf("Del(%v): %s\n\ngot:%v\n\nwant:%v", v.keys, diff, got, v.want)
		}
	}
}

func TestCore_DGet(t *testing.T) {
	tests := []struct {
		key, field, want string
		err              error
	}{
		{"bytes", "", "", ErrWrongType},
		{"404", "", "", nil},
		{"dict", "banana", "mama", nil},
		{"dict", "測試", "別れ、比類のない", nil},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		got, err := c.DGet(v.key, v.field)
		if err != v.err {
			t.Errorf("DGet(%q, %q) err: %q != %q", v.key, v.field, err, v.err)
		}
		if string(got) != v.want {
			t.Errorf("DGet(%q, %q) got: %q != %q", v.key, v.field, string(got), v.want)
		}
	}
}

func TestCore_DKeys(t *testing.T) {
	tests := []struct {
		key, pattern string
		err          error
		want         []string
	}{
		{"bytes", "", ErrWrongType, nil},
		{"404", "", nil, nil},
		{"dict", "b", nil, []string{}},
		{"dict", "b*", nil, []string{"banana"}},
		{"dict", "*", nil, []string{"banana", "測試"}},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		got, err := c.DKeys(v.key, v.pattern)
		sort.Strings(got)
		sort.Strings(v.want)

		if err != v.err {
			t.Errorf("DKeys(%q, %q) err: %q != %q", v.key, v.pattern, err, v.err)
		}
		if diff := deep.Equal(got, v.want); diff != nil {
			t.Errorf("DKeys(%q, %q): %s\n\ngot:%v\n\nwant:%v", v.key, v.pattern, diff, got, v.want)
		}
	}
}

func TestCore_DSet(t *testing.T) {
	tests := []struct {
		key, field, value string
		err               error
		count             int
	}{
		{"bytes", "", "", ErrWrongType, 0},
		{"new dict 測", "共", "共産主義の幽霊", nil, 1},
		{"dict", "共", "共産主義の幽霊", nil, 1},
		{"dict", "banana", "mango", nil, 0},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		count, err := c.DSet(v.key, v.field, []byte(v.value))
		got, getErr := c.DGet(v.key, v.field)
		if err != v.err {
			t.Errorf("DSet(%q, %q) err: %q != %q", v.key, v.field, err, v.err)
		}
		if err == nil && err != nil {
			t.Errorf("DSet(%q, %q) getErr: %q ", v.key, v.field, getErr)
		}
		if err == nil && count != v.count {
			t.Errorf("DSet(%q, %q) count: %d != %d", v.key, v.field, count, v.count)
		}
		if err == nil && string(got) != v.value {
			t.Errorf("DSet(%q, %q) got: %q != %q", v.key, v.field, string(got), v.value)
		}
	}
}

func TestCore_DGetAll(t *testing.T) {
	tests := []struct {
		key  string
		want map[string]string
		err  error
	}{
		{"bytes", nil, ErrWrongType},
		{"404", map[string]string{}, nil},
		{"dict", map[string]string{"banana": "mama", "測試": "別れ、比類のない"}, nil},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		result, err := c.DGetAll(v.key)
		if err != v.err {
			t.Errorf("DGet(%q) err: %q != %q", v.key, err, v.err)
		}
		got := map[string]string{}
		for i, v := range result {
			if i%2 == 1 {
				// skip values
				continue
			}
			got[string(v)] = string(result[i+1])
		}
		if diff := deep.Equal(got, v.want); err == nil && diff != nil {
			t.Errorf("DGetAll(%q): %s\n\ngot:%v\n\nwant:%v", v.key, diff, got, v.want)
		}
	}
}

func TestCore_DDel(t *testing.T) {
	tests := []struct {
		key       string
		fields    []string
		err       error
		wantKeys  []string
		wantCount int
	}{
		{"bytes", nil, ErrWrongType, nil, 0},
		{"404", []string{"banana", "nothing"}, nil, nil, 0},
		{"dict", []string{"banana", "nothing"}, nil, []string{"測試"}, 1},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		count, err := c.DDel(v.key, v.fields)
		got, _ := c.DKeys(v.key, "*")
		sort.Strings(got)
		sort.Strings(v.wantKeys)

		if err != v.err {
			t.Errorf("DDel(%q, %q) err: %q != %q", v.key, v.fields, err, v.err)
		}
		if count != v.wantCount {
			t.Errorf("DDel(%q, %q) count: %d != %d", v.key, v.fields, count, v.wantCount)
		}
		if diff := deep.Equal(got, v.wantKeys); diff != nil {
			t.Errorf("DKeys(%q, %q): %s\n\ngot:%v\n\nwant:%v", v.key, v.fields, diff, got, v.wantKeys)
		}
	}
}

func TestCore_LLen(t *testing.T) {
	tests := []struct {
		key  string
		err  error
		want int
	}{
		{"bytes", ErrWrongType, 0},
		{"404", nil, 0},
		{"list", nil, 3},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		got, err := c.LLen(v.key)

		if err != v.err {
			t.Errorf("LLen(%q) err: %q != %q", v.key, err, v.err)
		}
		if got != v.want {
			t.Errorf("LLen(%q) count: %d != %d", v.key, got, v.want)
		}
	}
}

func TestCore_LRange(t *testing.T) {
	tests := []struct {
		key         string
		start, stop int
		err         error
		want        []string
	}{
		{"bytes", 0, 0, ErrWrongType, []string{}},
		{"404", 0, 0, nil, []string{}},
		//IMPORTANT: by proto, HEAD of the list has index 0
		{"list", 0, 0, nil, []string{"KMFDM"}},
		{"list", 0, 10, nil, []string{"KMFDM", "Rammstein", "Abba"}},
		{"list", 1, 2, nil, []string{"Rammstein", "Abba"}},
		{"list", 10, 10, nil, []string{}},
		{"list", -2, -1, nil, []string{"Rammstein", "Abba"}},
		{"list", -1, 10, nil, []string{"Abba"}},
		{"list", -3, -3, nil, []string{"KMFDM"}},
		{"list", -1, -2, nil, []string{}},
		{"list", -10, -10, nil, []string{}},
		{"list", -1, -1, nil, []string{"Abba"}},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		result, err := c.LRange(v.key, v.start, v.stop)
		got := make([]string, len(result))
		for i, b := range result {
			got[i] = string(b)
		}

		if err != v.err {
			t.Errorf("LRange(%q, %d, %d) err: %q != %q", v.key, v.start, v.stop, err, v.err)
		}
		if diff := deep.Equal(got, v.want); diff != nil {
			t.Errorf("LRange(%q, %d, %d): %s\n\ngot:%v\n\nwant:%v", v.key, v.start, v.stop, diff, got, v.want)
		}
	}
}

func TestCore_LIndex(t *testing.T) {
	tests := []struct {
		key   string
		index int
		err   error
		want  string
	}{
		{"bytes", 0, ErrWrongType, ""},
		{"404", 0, nil, ""},
		//IMPORTANT: by proto, HEAD of the list has index 0
		{"list", 0, nil, "KMFDM"},
		{"list", 10, nil, ""},
		{"list", 2, nil, "Abba"},
		{"list", -1, nil, "Abba"},
		{"list", -3, nil, "KMFDM"},
		{"list", -10, nil, ""},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		result, err := c.LIndex(v.key, v.index)
		got := string(result)

		if err != v.err {
			t.Errorf("LIndex(%q, %d) err: %q != %q", v.key, v.index, err, v.err)
		}
		if got != v.want {
			t.Errorf("LIndex(%q, %d) got: %q != %q", v.key, v.index, got, v.want)
		}
	}
}

func TestCore_LSet(t *testing.T) {
	tests := []struct {
		key   string
		index int
		err   error
		value string
	}{
		{"bytes", 0, ErrWrongType, ""},
		{"404", 0, ErrNotFound, ""},
		//IMPORTANT: by proto, HEAD of the list has index 0
		{"list", 10, ErrInvalidParams, ""},
		{"list", 0, nil, "AC/DC"},
		{"list", -1, nil, "Оргия праведников"},
		{"list", -10, ErrInvalidParams, ""},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		err := c.LSet(v.key, v.index, []byte(v.value))
		result, _ := c.LIndex(v.key, v.index)
		got := string(result)

		if err != v.err {
			t.Errorf("LSet(%q, %d, %q) err: %q != %q", v.key, v.index, v.value, err, v.err)
		}
		if err == nil && got != v.value {
			t.Errorf("LSet(%q, %d, %q) got: %q != %q", v.key, v.index, v.value, got, v.value)
		}
	}
}

func TestCore_LPush(t *testing.T) {
	tests := []struct {
		key          string
		err          error
		values, want []string
	}{
		{"bytes", ErrWrongType, nil, nil},
		{"list_new", nil, []string{"a", "b", "c"}, []string{"c", "b", "a"}},
		{"list", nil, []string{"a", "b", "c", "d", "e", "AC/DC"}, []string{"AC/DC", "e", "d", "c", "b", "a", "KMFDM", "Rammstein", "Abba"}},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		values := make([][]byte, len(v.values))
		for i, value := range v.values {
			values[i] = []byte(value)
		}

		count, err := c.LPush(v.key, values)
		result, _ := c.LRange(v.key, 0, -1)

		got := make([]string, len(result))
		for i, value := range result {
			got[i] = string(value)
		}

		if err != v.err {
			t.Errorf("LPush(%q, %q) err: %q != %q", v.key, v.values, err, v.err)
		}
		if err == nil && count != len(v.want) {
			t.Errorf("LPush(%q, %q) count: %d != %d", v.key, v.values, count, len(v.want))
		}
		if diff := deep.Equal(got, v.want); err == nil && diff != nil {
			t.Errorf("LPush(%q, %q): %s\n\ngot:%v\n\nwant:%v", v.key, v.values, diff, got, v.want)
		}
	}
}

func TestCore_LPop(t *testing.T) {
	tests := []struct {
		key        string
		err        error
		wantResult string
		wantList   []string
	}{
		{"bytes", ErrWrongType, "", nil},
		{"list_new", nil, "", []string{}},
		{"list", nil, "KMFDM", []string{"Rammstein", "Abba"}},
	}

	c := NewCore(NewMockEngine())

	for _, v := range tests {
		value, err := c.LPop(v.key)
		result, _ := c.LRange(v.key, 0, -1)

		got := make([]string, len(result))
		for i, value := range result {
			got[i] = string(value)
		}

		if err != v.err {
			t.Errorf("LPop(%q) err: %q != %q", v.key, err, v.err)
		}
		if err == nil && string(value) != v.wantResult {
			t.Errorf("LPop(%q) value: %q != %q", v.key, string(value), v.wantResult)
		}
		if diff := deep.Equal(got, v.wantList); err == nil && diff != nil {
			t.Errorf("LPop(%q): %s\n\ngot:%v\n\nwant:%v", v.key, diff, got, v.wantList)
		}
	}
}

//TODO: поставить неправильный тип лока (Rlock) на операцию записи и посмотреть, чтобы это отловилось
type TestCoreConcurrencyTestCase struct {
	bytes      []string
	list       []string
	dict       []string
	dictFields []string
	listLen    int
}

func TestCore_concurrency(t *testing.T) {

	tests := []TestCoreConcurrencyTestCase{
		{
			[]string{"b_a", "b_b", "b_c"},
			[]string{"l_a", "l_b", "l_c"},
			[]string{"d_a", "d_b", "d_c"},
			[]string{"f1", "f2", "f3", "f4"},
			10,
		},
		{
			[]string{"b_1", "b_2", "b_3"},
			[]string{"l_1", "l_2", "l_3"},
			[]string{"d_1", "d_2", "d_3"},
			[]string{"f1", "f2", "f3", "f4"},
			10,
		},
		{
			[]string{"b_a", "b_b", "b_c", "b_d", "b_e"},
			[]string{"l_a", "l_b", "l_c", "l_d", "l_e"},
			[]string{"d_a", "d_b", "d_c", "d_d", "d_e"},
			[]string{"f1", "f2", "f3", "f4"},
			10,
		},
	}

	var longTest TestCoreConcurrencyTestCase
	longTest.dictFields = []string{"f1", "f2", "f3", "f4"}
	longTest.listLen = 10
	for i := 0; i < 1000; i++ {
		longTest.bytes = append(longTest.bytes, fmt.Sprintf("b_%d", rand.Uint64()))
		longTest.list = append(longTest.list, fmt.Sprintf("l_%d", rand.Uint64()))
		longTest.dict = append(longTest.dict, fmt.Sprintf("d_%d", rand.Uint64()))
	}
	tests = append(tests, longTest)

	c := NewCore(NewHashEngine())
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go coreWorker(&wg, c, tests)
	}

	wg.Wait()

	// Due to last operation of every coreWorker is AddOrReplace() for last keyset
	// after all workers done, only last keyset  should remain in the engine
	got := c.Keys("*")
	want := append([]string{}, tests[0].bytes...)
	want = append(want, tests[0].list...)
	want = append(want, tests[0].dict...)
	sort.Strings(got)
	sort.Strings(want)
	if diff := deep.Equal(got, want); diff != nil {
		t.Errorf("Keys() got != Keys() want: %s\n\ngot:%v\n\nwant:%v", diff, got, want)
	}
}

func coreWorker(wg *sync.WaitGroup, c *Core, tests []TestCoreConcurrencyTestCase) {
	for _, t := range tests {
		for _, key := range t.bytes {
			c.Set(key, []byte(time.Now().String()))
			c.Get(key)
		}
		for _, key := range t.dict {
			for _, field := range t.dictFields {
				c.DSet(key, field, []byte(time.Now().String()))
				c.DGet(key, field)
			}
			c.DKeys(key, "**")
			c.DGetAll(key)
			c.DDel(key, t.dictFields)
		}
		for _, key := range t.list {
			values := [][]byte{}
			for i := 0; i < t.listLen; i++ {
				values = append(values, []byte(time.Now().String()))
			}
			c.LPush(key, values)
			for i := 0; i < t.listLen; i++ {
				c.LSet(key, i, []byte(time.Now().String()))
				c.LIndex(key, i)
			}
			c.LLen(key)
			c.LRange(key, 0, -1)
			for i := 0; i < t.listLen; i++ {
				c.LPop(key)
			}
		}

		c.Keys("**")
		c.Del(t.bytes)
		c.Del(t.list)
		c.Del(t.dict)
	}

	// add first test to check that data actually adds to storage
	t := tests[0]
	for _, key := range t.bytes {
		c.Set(key, []byte(time.Now().String()))
	}
	for _, key := range t.dict {
		c.DSet(key, "f", []byte(time.Now().String()))
	}
	for _, key := range t.list {
		c.LPush(key, [][]byte{[]byte("val")})
	}

	wg.Done()
}

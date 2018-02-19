package radish

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/mshaverdo/radish/controller"
	"github.com/mshaverdo/radish/log"
	"sort"
	"strconv"
	"testing"
	"time"
)

var _ = redis.Nil
var _ = controller.Controller{}
var _ = log.CRITICAL
var _ = time.Millisecond

var notFound interface{}

type TestCase struct {
	a        []string
	want     string
	wantData string
}

func TestClient(t *testing.T) {
	//*
	log.SetLevel(log.CRITICAL)
	go controller.New("", 6381, "").ListenAndServe()
	time.Sleep(500 * time.Millisecond) // wait to ensure, that controller started

	cl := NewClient("localhost", 6381)
	notFound = ErrNotFound
	/*/
	cl := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	//cl.FlushDB() //flush DB to generate expected results
	notFound = redis.Nil
	//*/

	fmt.Printf("Using %T\n", cl)

	var (
		cmd   string
		tests []TestCase
		fnc   func(v TestCase) (interface{}, error)
	)

	getDataKeys := func(v TestCase) (interface{}, error) { return cl.Keys("*").Result() }
	getDataVal := func(v TestCase) (interface{}, error) { return cl.Get(v.a[0]).Result() }
	getDataDict := func(v TestCase) (interface{}, error) { return cl.HGetAll(v.a[0]).Result() }
	getDataList := func(v TestCase) (interface{}, error) { return cl.LRange(v.a[0], 0, -1).Result() }
	getDataTtl := func(v TestCase) (interface{}, error) { return cl.TTL(v.a[0]).Result() }

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"key1", "val1!"}, `Set(["key1" "val1!"]): OK`, `Set(["key1" "val1!"]): val1!`},
		{[]string{"key2", "!!!"}, `Set(["key2" "!!!"]): OK`, `Set(["key2" "!!!"]): !!!`},
		{[]string{"key2", "val2!"}, `Set(["key2" "val2!"]): OK`, `Set(["key2" "val2!"]): val2!`},
	}
	cmd = "Set"
	fnc = func(v TestCase) (interface{}, error) { return cl.Set(v.a[0], v.a[1], 0).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, getDataVal, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"list", "val1!"}, `LPush(["list" "val1!"]): 1`, `LPush(["list" "val1!"]): [val1!]`},
		{[]string{"list", "val2!"}, `LPush(["list" "val2!"]): 2`, `LPush(["list" "val2!"]): [val1! val2!]`},
	}
	cmd = "LPush"
	fnc = func(v TestCase) (interface{}, error) { return cl.LPush(v.a[0], v.a[1]).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, getDataList, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"dict", "f1", "val1"}, `Hset(["dict" "f1" "val1"]): OK`, `Hset(["dict" "f1" "val1"]): [f1: val1]`},
		{[]string{"dict", "f2", "!!!"}, `Hset(["dict" "f2" "!!!"]): OK`, `Hset(["dict" "f2" "!!!"]): [f1: val1 f2: !!!]`},
		{[]string{"dict", "f2", "val2"}, `Hset(["dict" "f2" "val2"]): FAIL`, `Hset(["dict" "f2" "val2"]): [f1: val1 f2: val2]`},
	}
	cmd = "Hset"
	fnc = func(v TestCase) (interface{}, error) { return cl.HSet(v.a[0], v.a[1], v.a[2]).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, getDataDict, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"*"}, `Keys(["*"]): [dict key1 key2 list]`, ``},
	}
	cmd = "Keys"
	fnc = func(v TestCase) (interface{}, error) { return cl.Keys(v.a[0]).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, nil, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"key1"}, `Get(["key1"]): val1!`, ``},
		{[]string{"dict"}, `Error during Get(["dict"]): "radish: ErrTypeMismatch"`, ``},
		{[]string{"404"}, `Get(["404"]) Not Found!`, ``},
	}
	cmd = "Get"
	fnc = func(v TestCase) (interface{}, error) { return cl.Get(v.a[0]).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, nil, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"404", "key2"}, `Del(["404" "key2"]): 1`, `Del(["404" "key2"]): [dict key1 list]`},
	}
	cmd = "Del"
	fnc = func(v TestCase) (interface{}, error) { return cl.Del(v.a[0], v.a[1]).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, getDataKeys, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"dict"}, `HKeys(["dict"]): [f1 f2]`, ``},
		{[]string{"404"}, `HKeys(["404"]): []`, ``},
		{[]string{"key1"}, `Error during HKeys(["key1"]): "radish: ErrTypeMismatch"`, ``},
	}
	cmd = "HKeys"
	fnc = func(v TestCase) (interface{}, error) { return cl.HKeys(v.a[0]).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, nil, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"dict"}, `HGetAll(["dict"]): [f1: val1 f2: val2]`, ``},
		{[]string{"404"}, `HGetAll(["404"]): []`, ``},
		{[]string{"key1"}, `Error during HGetAll(["key1"]): "radish: ErrTypeMismatch"`, ``},
	}
	cmd = "HGetAll"
	fnc = func(v TestCase) (interface{}, error) { return cl.HGetAll(v.a[0]).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, nil, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"dict", "f1"}, `HDel(["dict" "f1"]): 1`, `HDel(["dict" "f1"]): [f2: val2]`},
		{[]string{"404", "f1"}, `HDel(["404" "f1"]): 0`, `HDel(["404" "f1"]): []`},
		{[]string{"key1", "f1"}, `Error during HDel(["key1" "f1"]): "radish: ErrTypeMismatch"`, `Error during HDel(["key1" "f1"]): "radish: ErrTypeMismatch"`},
	}
	cmd = "HDel"
	fnc = func(v TestCase) (interface{}, error) { return cl.HDel(v.a[0], v.a[1]).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, getDataDict, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"dict", "f2"}, `HGet(["dict" "f2"]): val2`, ``},
		{[]string{"key1", "f2"}, `Error during HGet(["key1" "f2"]): "radish: ErrTypeMismatch"`, ``},
		{[]string{"404", "f2"}, `HGet(["404" "f2"]) Not Found!`, ``},
	}
	cmd = "HGet"
	fnc = func(v TestCase) (interface{}, error) { return cl.HGet(v.a[0], v.a[1]).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, nil, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"list"}, `LLen(["list"]): 2`, ``},
		{[]string{"key1"}, `Error during LLen(["key1"]): "radish: ErrTypeMismatch"`, ``},
		{[]string{"404"}, `LLen(["404"]): 0`, ``},
	}
	cmd = "LLen"
	fnc = func(v TestCase) (interface{}, error) { return cl.LLen(v.a[0]).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, nil, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"list"}, `LRange(["list"]): [val1! val2!]`, ``},
		{[]string{"key1"}, `Error during LRange(["key1"]): "radish: ErrTypeMismatch"`, ``},
		{[]string{"404"}, `LRange(["404"]): []`, ``},
	}
	cmd = "LRange"
	fnc = func(v TestCase) (interface{}, error) { return cl.LRange(v.a[0], 0, -1).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, nil, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"list"}, `LIndex(["list"]): val2!`, ``},
		{[]string{"key1"}, `Error during LIndex(["key1"]): "radish: ErrTypeMismatch"`, ``},
		{[]string{"404"}, `LIndex(["404"]) Not Found!`, ``},
	}
	cmd = "LIndex"
	fnc = func(v TestCase) (interface{}, error) {
		return cl.LIndex(v.a[0], 0).Result()
	}
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, nil, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"list", "new val"}, `LSet(["list" "new val"]): OK`, `LSet(["list" "new val"]): [new val val1!]`},
		{[]string{"key1", ""}, `Error during LSet(["key1" ""]): "radish: ErrTypeMismatch"`, `Error during LSet(["key1" ""]): "radish: ErrTypeMismatch"`},
		{[]string{"404", ""}, `LSet(["404" ""]) Not Found!`, `LSet(["404" ""]): []`},
	}
	cmd = "LSet"
	fnc = func(v TestCase) (interface{}, error) {
		return cl.LSet(v.a[0], 0, v.a[1]).Result()
	}
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, getDataList, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"list"}, `LPop(["list"]): new val`, `LPop(["list"]): [val1!]`},
		{[]string{"key1"}, `Error during LPop(["key1"]): "radish: ErrTypeMismatch"`, `Error during LPop(["key1"]): "radish: ErrTypeMismatch"`},
		{[]string{"404"}, `LPop(["404"]) Not Found!`, `LPop(["404"]): []`},
	}
	cmd = "LPop"
	fnc = func(v TestCase) (interface{}, error) { return cl.LPop(v.a[0]).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, getDataList, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"key1"}, `TTL(["key1"]): -1s`, ``},
		{[]string{"404"}, `TTL(["404"]): -2s`, ``},
	}
	cmd = "TTL"
	fnc = func(v TestCase) (interface{}, error) { return cl.TTL(v.a[0]).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, nil, tests) })

	//////////////////////////////////////////////////////
	tests = []TestCase{
		{[]string{"key1", "5"}, `Expire(["key1" "5"]): OK`, `Expire(["key1" "5"]): 5s`},
		{[]string{"dict", "0"}, `Expire(["dict" "0"]): OK`, `Expire(["dict" "0"]): -2s`},
		{[]string{"list", "-1"}, `Expire(["list" "-1"]): OK`, `Expire(["list" "-1"]): -2s`},
		{[]string{"404", "0"}, `Expire(["404" "0"]): FAIL`, `Expire(["404" "0"]): -2s`},
	}
	cmd = "Expire"
	fnc = func(v TestCase) (interface{}, error) {
		seconds, _ := strconv.Atoi(string(v.a[1]))
		return cl.Expire(v.a[0], time.Duration(seconds)*time.Second).Result()
	}
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, getDataTtl, tests) })

	tests = []TestCase{
		{[]string{"key1"}, `Persist(["key1"]): OK`, `Persist(["key1"]): -1s`},
		{[]string{"dict"}, `Persist(["dict"]): FAIL`, `Persist(["dict"]): -2s`},
		{[]string{"404"}, `Persist(["404"]): FAIL`, `Persist(["404"]): -2s`},
	}
	cmd = "Persist"
	fnc = func(v TestCase) (interface{}, error) { return cl.Persist(v.a[0]).Result() }
	t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, getDataTtl, tests) })

	/*
		//////////////////////////////////////////////////////
		tests = []TestCase{
			{[]string{"", ""}, ``, ``},
		}
		cmd = ""
		fnc = func(v TestCase) (interface{}, error) { return cl.(v.a[0], v.a[1]).Result() }
		getData = nil
		t.Run(cmd, func(t *testing.T) { processTest(t, cmd, fnc, getData, tests) })
	*/
}

func processTest(t *testing.T, cmd string, command, getData func(testCase TestCase) (interface{}, error), tests []TestCase) {
	for _, v := range tests {
		val, err := command(v)
		result := getCommandResult(cmd, val, err, v.a)

		if v.want == `` {
			t.Log("want >>>>>", result)
		} else if result != v.want {
			t.Errorf("%s command: %s != %s", cmd, result, v.want)
		}

		if getData != nil {
			data, dataErr := getData(v)
			dataResult := getCommandResult(cmd, data, dataErr, v.a)

			if v.wantData == `` {
				t.Log("wantData >>>>>", dataResult)
			} else if dataResult != v.wantData {
				t.Errorf("%s data: %s != %s", cmd, dataResult, v.wantData)
			}
		}
	}
}

func getCommandResult(cmd, val interface{}, err error, param interface{}) string {
	if err == notFound {
		return fmt.Sprintf("%s(%q) Not Found!", cmd, param)
	} else if err != nil && err.Error() == "WRONGTYPE Operation against a key holding the wrong kind of value" {
		return fmt.Sprintf("Error during %s(%q): %q", cmd, param, ErrTypeMismatch.Error())
	} else if err != nil && err.Error() == "ERR no such key" {
		return fmt.Sprintf("%s(%q) Not Found!", cmd, param)
	} else if err != nil {
		return fmt.Sprintf("Error during %s(%q): %q", cmd, param, err.Error())
	} else {
		switch val := val.(type) {
		case bool:
			if val {
				return fmt.Sprintf("%s(%q): OK", cmd, param)
			} else {
				return fmt.Sprintf("%s(%q): FAIL", cmd, param)
			}
		case []string:
			sort.Strings(val)
			return fmt.Sprintf("%s(%q): %v", cmd, param, val)
		//case float64:
		//	return fmt.Sprintf("%s(%q): %.4f!", cmd, param, val)
		//case time.Duration:
		//	return fmt.Sprintf("%s(%q): %v", cmd, param, val)
		case map[string]string:
			var sortedVals, sortedKeys []string

			for k := range val {
				sortedKeys = append(sortedKeys, k)
			}
			sort.Strings(sortedKeys)
			for _, k := range sortedKeys {
				sortedVals = append(sortedVals, fmt.Sprintf("%s: %s", k, val[k]))
			}
			return fmt.Sprintf("%s(%q): %v", cmd, param, sortedVals)
		default:
			return fmt.Sprintf("%s(%q): %v", cmd, param, val)
		}
	}
}

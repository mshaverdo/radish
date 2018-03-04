// +build integration

package integration_test

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/mshaverdo/radish/controller"
	"github.com/mshaverdo/radish/log"
	"github.com/mshaverdo/radish/radish-client"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"
)

var testers []*ClientTester

type TestCase struct {
	args     []interface{}
	want     string
	wantData string
}

type ClientTester struct {
	name   string
	t      *testing.T
	client interface{}
}

func NewClientTester(name string, client interface{}) *ClientTester {
	return &ClientTester{client: client, name: name}
}

func (ct *ClientTester) Test(cmd string, getData func(TestCase) (interface{}, error), tests []TestCase) {
	for _, tst := range tests {
		val, err := ct.callCommand(cmd, tst.args...)
		result := ct.formatCommandResult(cmd, val, err, tst.args)

		if result != tst.want {
			ct.t.Errorf("%s> %s(%v) \n got: %s \n want: %s", ct.name, cmd, tst.args, result, tst.want)
		}

		if getData == nil {
			continue
		}

		data, dataErr := getData(tst)
		dataResult := ct.formatCommandResult(cmd, data, dataErr, tst.args)

		if dataResult != tst.wantData {
			ct.t.Errorf("%s> %s(%v) \n data got: %s \n data want: %s", ct.name, cmd, tst.args, dataResult, tst.wantData)
		}
	}
}

func (ct *ClientTester) formatCommandResult(cmd, val interface{}, err error, param interface{}) string {
	if err != nil {
		return fmt.Sprintf("ERROR: %s", err)
	}
	switch concreteVal := val.(type) {
	case []string:
		sort.Strings(concreteVal)
		return fmt.Sprintf("%v", concreteVal)
	case map[string]string:
		var sortedVals, sortedKeys []string

		for k := range concreteVal {
			sortedKeys = append(sortedKeys, k)
		}
		sort.Strings(sortedKeys)
		for _, k := range sortedKeys {
			sortedVals = append(sortedVals, fmt.Sprintf("%s: %s", k, concreteVal[k]))
		}
		return fmt.Sprintf("map%v", sortedVals)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func (ct *ClientTester) callCommand(cmd string, args ...interface{}) (value interface{}, err error) {
	argValues := make([]reflect.Value, len(args))
	for i, v := range args {
		argValues[i] = reflect.ValueOf(v)
	}

	method := reflect.ValueOf(ct.client).MethodByName(cmd)
	if !method.IsValid() {
		panic(fmt.Sprintf("Unknown method %T.%s", ct.client, cmd))
	}

	commandResult := method.Call(argValues)

	result := commandResult[0].MethodByName("Result").Call(nil)

	value = result[0].Interface()
	if result[1].IsNil() {
		err = nil
	} else {
		err = result[1].Interface().(error)
	}

	return value, err
}

func (ct *ClientTester) GetDataKeys(tst TestCase) (value interface{}, err error) {
	return ct.callCommand("Keys", "*")
}

func (ct *ClientTester) GetDataVal(tst TestCase) (value interface{}, err error) {
	return ct.callCommand("Get", tst.args[0])
}

func (ct *ClientTester) getDataDict(tst TestCase) (value interface{}, err error) {
	return ct.callCommand("HGetAll", tst.args[0])
}

func (ct *ClientTester) getDataList(tst TestCase) (value interface{}, err error) {
	return ct.callCommand("LRange", tst.args[0], int64(0), int64(-1))
}

func (ct *ClientTester) getDataTtl(tst TestCase) (value interface{}, err error) {
	return ct.callCommand("TTL", tst.args[0])
}

func (ct *ClientTester) Setup(t *testing.T) {
	ct.t = t

	ct.callCommand("Set", "", "0000", 0*time.Second)
	ct.callCommand("Set", "key1", "val1", 0*time.Second)
	ct.callCommand("Set", "key2", "val2", 0*time.Second)
	ct.callCommand("Set", "key3", "val3", 0*time.Second)
	ct.callCommand("Expire", "key3", 1*time.Hour)

	ct.callCommand("LPush", "list", "lv3", "lv2", "lv1", "", "lv0")

	ct.callCommand("HSet", "dict", "f1", "dv1")
	ct.callCommand("HSet", "dict", "f2", "dv2")
	ct.callCommand("HSet", "dict", "f3", "dv3")
	ct.callCommand("HSet", "dict", "f__", "")
	ct.callCommand("HSet", "dict", "", "dv000")
}

func (ct *ClientTester) Teardown() {
	ct.t = nil
	val, _ := ct.callCommand("Keys", "*")
	var keys []interface{}
	for _, v := range val.([]string) {
		keys = append(keys, v)
	}
	ct.callCommand("Del", keys...)
}

func TestMain(m *testing.M) {
	var (
		redisAddr      string
		redisFlush     bool
		radishHttpPort int
		radishRespPort int
	)
	//TODO: во всех get-командах проверить, чтобы TypeMismathc, notfound-значения возвращались ТОЧНО ТАК ЖЕ КАК В ЭТАЛОНЕ

	//TODO: добавить описания ключей и способ запуска интеграционного теста в README
	//go test -tags integration -v -redis localhost:6379 -flush github.com/mshaverdo/radish/integration_test
	flag.IntVar(&radishHttpPort, "resp", 16381, "Free port for testing RESP radish server")
	flag.IntVar(&radishRespPort, "http", 16380, "Free port for testing HTTP radish server")
	flag.StringVar(&redisAddr, "redis", "", "Address of existing REDIS server like 'localhost:6379'. If specified, use redis installation on the port to self-check test data. WARNING! it WILL FLUSH all of your data!")
	flag.BoolVar(&redisFlush, "flush", false, "If true, flush redis DB at specified port before start test WARNING! It WILL corrupt your data!")
	flag.Parse()

	// redis client
	if redisAddr != "" {
		// Run test on real REDIS instance to ensure that test data is correct.
		// if this tests fails, fix test data to pass
		redisClient := redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})

		if redisFlush {
			redisClient.FlushDB()
		}

		if redisClient.RandomKey().Val() != "" {
			panic("Redis DB must be empty before running tests. Suggested to use -flush key carefully")
		}

		testers = append(testers, NewClientTester("Redis", redisClient))
	}

	//Radish HTTP client
	log.SetLevel(log.CRITICAL)
	go func() {
		controllerHttp := controller.New("", radishHttpPort, "", 0, 0, 0, true)
		err := controllerHttp.ListenAndServe()
		if err != nil {
			panic("HTTP controller failed to start:" + err.Error())
		}
	}()
	time.Sleep(100 * time.Millisecond) // wait to ensure, that controller started

	radishHttpClient := radish.NewClient("localhost", radishHttpPort)

	testers = append(testers, NewClientTester("Radish-HTTP", radishHttpClient))

	//Radish RESP client
	go func() {
		controllerResp := controller.New("", radishRespPort, "", 0, 0, 0, false)
		err := controllerResp.ListenAndServe()
		if err != nil {
			panic("HTTP controller failed to start:" + err.Error())
		}
	}()
	time.Sleep(100 * time.Millisecond) // wait to ensure, that controller started

	radishRespClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("localhost:%d", radishRespPort),
	})

	testers = append(testers, NewClientTester("Radish-RESP", radishRespClient))

	os.Exit(m.Run())
}

func Test_Set(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"key/1", "11_/測試\r\n\x00", 0 * time.Second}, `OK`, "11_/測試\r\n\x00"},
		{[]interface{}{"key2", "", 0 * time.Second}, `OK`, ``},
		{[]interface{}{"key3\"\r\n\x00\x00", "測試", 0 * time.Second}, `OK`, `測試`},
		{[]interface{}{"", "dat", 0 * time.Second}, `OK`, `dat`},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("Set", tester.GetDataVal, tests)
		tester.Teardown()
	}
}

func Test_LPush(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"list", "!", "!!", ""}, `8`, `[  ! !! lv0 lv1 lv2 lv3]`},
		{[]interface{}{"404", "val2!"}, `1`, `[val2!]`},
		{[]interface{}{"key1", "val1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
		{[]interface{}{"", "val1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
		{[]interface{}{"dict", "val1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("LPush", tester.getDataList, tests)
		tester.Teardown()
	}
}

func Test_HSet(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"dict", "f1", "val1"}, `false`, `map[: dv000 f1: val1 f2: dv2 f3: dv3 f__: ]`},
		{[]interface{}{"dict", "f5", "!!!"}, `true`, `map[: dv000 f1: val1 f2: dv2 f3: dv3 f5: !!! f__: ]`},
		{[]interface{}{"", "f1", "val11"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
		{[]interface{}{"404", "", "val00"}, `true`, `map[: val00]`},
		{[]interface{}{"404", "f1", ""}, `true`, `map[: val00 f1: ]`},
		{[]interface{}{"404", "f2", "val1"}, `true`, `map[: val00 f1:  f2: val1]`},
		{[]interface{}{"key1", "f1", "val11"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
		{[]interface{}{"list", "f1", "val11"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("HSet", tester.getDataDict, tests)
		tester.Teardown()
	}
}

func Test_Keys(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"*"}, `[ dict key1 key2 key3 list]`, ``},
		{[]interface{}{"key*"}, `[key1 key2 key3]`, ``},
		{[]interface{}{"\x00"}, `[]`, ``},
		{[]interface{}{""}, `[]`, ``},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("Keys", nil, tests)
		tester.Teardown()
	}
}

func Test_Get(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{""}, `0000`, ``},
		{[]interface{}{"key1"}, `val1`, ``},
		{[]interface{}{"404"}, `ERROR: redis: nil`, ``},
		{[]interface{}{"list"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("Get", nil, tests)
		tester.Teardown()
	}
}

func Test_Del(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"404", "key1", ""}, `2`, `[dict key2 key3 list]`},
		{[]interface{}{"list", "dict"}, `2`, `[key2 key3]`},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("Del", tester.GetDataKeys, tests)
		tester.Teardown()
	}
}

func Test_HKeys(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"dict"}, `[ f1 f2 f3 f__]`, ``},
		{[]interface{}{"list"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"key1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{""}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"404"}, `[]`, ``},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("HKeys", nil, tests)
		tester.Teardown()
	}
}

func Test_HGetAll(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"dict"}, `map[: dv000 f1: dv1 f2: dv2 f3: dv3 f__: ]`, ``},
		{[]interface{}{"list"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"key1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{""}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"404"}, `map[]`, ``},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("HGetAll", nil, tests)
		tester.Teardown()
	}
}

func Test_HDel(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"dict", "f1", "f404"}, `1`, `map[: dv000 f2: dv2 f3: dv3 f__: ]`},
		{[]interface{}{"dict", ""}, `1`, `map[f2: dv2 f3: dv3 f__: ]`},
		{[]interface{}{"dict", "f2", "f3"}, `2`, `map[f__: ]`},
		{[]interface{}{"list", "f1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
		{[]interface{}{"key1", "f1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
		{[]interface{}{"404", "f1"}, `0`, `map[]`},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("HDel", tester.getDataDict, tests)
		tester.Teardown()
	}
}

func Test_HGet(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"dict", "f1"}, `dv1`, ``},
		{[]interface{}{"dict", ""}, `dv000`, ``},
		{[]interface{}{"dict", "f404"}, `ERROR: redis: nil`, ``},
		{[]interface{}{"list", "f1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"key1", "f1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"", "f1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"404", "f1"}, `ERROR: redis: nil`, ``},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("HGet", nil, tests)
		tester.Teardown()
	}
}

func Test_LLen(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"list"}, `5`, ``},
		{[]interface{}{"dict"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"key1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"404"}, `0`, ``},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("LLen", nil, tests)
		tester.Teardown()
	}
}

func Test_LRange(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"list", int64(0), int64(-1)}, `[ lv0 lv1 lv2 lv3]`, ``},
		{[]interface{}{"list", int64(10), int64(10)}, `[]`, ``},
		{[]interface{}{"list", int64(1), int64(2)}, `[ lv1]`, ``},
		{[]interface{}{"list", int64(0), int64(0)}, `[lv0]`, ``},
		{[]interface{}{"key1", int64(0), int64(0)}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"", int64(0), int64(0)}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"dict", int64(0), int64(0)}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"404", int64(0), int64(0)}, `[]`, ``},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("LRange", nil, tests)
		tester.Teardown()
	}
}

func Test_LIndex(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"list", int64(0)}, `lv0`, ``},
		{[]interface{}{"list", int64(-1)}, `lv3`, ``},
		{[]interface{}{"list", int64(10)}, `ERROR: redis: nil`, ``},
		{[]interface{}{"dict", int64(10)}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"key1", int64(10)}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"", int64(10)}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, ``},
		{[]interface{}{"404", int64(10)}, `ERROR: redis: nil`, ``},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("LIndex", nil, tests)
		tester.Teardown()
	}
}

func Test_LSet(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"list", int64(0), "val1"}, `OK`, `[ lv1 lv2 lv3 val1]`},
		{[]interface{}{"list", int64(1), ""}, `OK`, `[ lv1 lv2 lv3 val1]`},
		{[]interface{}{"list", int64(-1), "val1"}, `OK`, `[ lv1 lv2 val1 val1]`},
		{[]interface{}{"list", int64(10), "val1"}, `ERROR: ERR index out of range`, `[ lv1 lv2 val1 val1]`},
		{[]interface{}{"dict", int64(0), "val1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
		{[]interface{}{"key1", int64(0), "val1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
		{[]interface{}{"", int64(0), "val1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
		{[]interface{}{"404", int64(0), "val1"}, `ERROR: ERR no such key`, `[]`},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("LSet", tester.getDataList, tests)
		tester.Teardown()
	}
}

func Test_LPop(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"list"}, `lv0`, `[ lv1 lv2 lv3]`},
		{[]interface{}{"list"}, ``, `[lv1 lv2 lv3]`},
		{[]interface{}{"list"}, `lv1`, `[lv2 lv3]`},
		{[]interface{}{"list"}, `lv2`, `[lv3]`},
		{[]interface{}{"list"}, `lv3`, `[]`},
		{[]interface{}{"list"}, `ERROR: redis: nil`, `[]`},
		{[]interface{}{"404"}, `ERROR: redis: nil`, `[]`},
		{[]interface{}{"key1"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
		{[]interface{}{""}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
		{[]interface{}{"dict"}, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`, `ERROR: WRONGTYPE Operation against a key holding the wrong kind of value`},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("LPop", tester.getDataList, tests)
		tester.Teardown()
	}
}

func Test_TTL(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"key1"}, `-1s`, ``},
		{[]interface{}{"key3"}, `1h0m0s`, ``},
		{[]interface{}{""}, `-1s`, ``},
		{[]interface{}{"404"}, `-2s`, ``},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("TTL", nil, tests)
		tester.Teardown()
	}
}

func Test_Expire(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{"", 5 * time.Second}, `true`, `5s`},
		{[]interface{}{"key1", 5 * time.Second}, `true`, `5s`},
		{[]interface{}{"key1", 0 * time.Second}, `true`, `-2s`},
		{[]interface{}{"key2", 0 * time.Second}, `true`, `-2s`},
		{[]interface{}{"key3", -1 * time.Second}, `true`, `-2s`},
		{[]interface{}{"404", 0 * time.Second}, `false`, `-2s`},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("Expire", tester.getDataTtl, tests)
		tester.Teardown()
	}
}

func Test_Persist(t *testing.T) {
	tests := []TestCase{
		{[]interface{}{""}, `false`, `-1s`},
		{[]interface{}{"key1"}, `false`, `-1s`},
		{[]interface{}{"key3"}, `true`, `-1s`},
		{[]interface{}{"404"}, `false`, `-2s`},
	}

	for _, tester := range testers {
		tester.Setup(t)
		tester.Test("Persist", tester.getDataTtl, tests)
		tester.Teardown()
	}
}

package main

import (
	"flag"
	"fmt"
	"github.com/mshaverdo/assert"
	"github.com/mshaverdo/radish/radish-client"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var debug = "1"

func init() {
	assert.Enabled = (debug == "1")
}

type Test struct {
	fnc                                  func(*Test) (bool, error)
	succeeded, failed, remainingRequests Counter
	wg                                   *sync.WaitGroup
	keySpaceLen                          int
	sampleDataLen                        int
	client                               *radish.Client
	verbose                              bool
	cmd                                  string
}

func (t *Test) Run() {
	for t.remainingRequests.Add(-1) >= 0 {
		if ok, err := t.fnc(t); ok {
			t.succeeded.Add(1)
		} else {
			if t.verbose {
				fmt.Printf("FAILED %s: %s\n", t.cmd, err)
			}
			t.failed.Add(1)
		}
	}

	t.wg.Done()
}

func (t *Test) getStringKey() string {
	return fmt.Sprintf("string_%d", rand.Int()%t.keySpaceLen)
}

func (t *Test) getListKey() string {
	return fmt.Sprintf("list_%d", rand.Int()%t.keySpaceLen)
}

func (t *Test) getData() string {
	val := time.Now().Format("04:05.0000")
	repeats := t.sampleDataLen - len(val)
	if repeats < 0 {
		repeats = 0
	}
	return val + strings.Repeat("=", repeats)
}

type Counter struct {
	mu  sync.Mutex
	val int
}

func (c *Counter) Add(d int) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.val += d

	return c.val
}

func (c *Counter) Get() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.val
}

func main() {
	var (
		host          string
		port          int
		clients       int
		testNames     string
		requests      int
		keySpaceLen   int
		sampleDataLen int
		verbose       bool
	)

	flag.StringVar(&host, "h", "localhost", "Server hostname")
	flag.IntVar(&port, "p", 6380, "Server port")
	flag.IntVar(&clients, "c", 20, "Number of parallel connections")
	flag.IntVar(&requests, "n", 100000, "Total number of requests")
	flag.IntVar(&keySpaceLen, "r", 1, "Use random keys  with specified space len")
	flag.IntVar(&sampleDataLen, "d", 10, "Data size of SET/GET value in bytes")
	flag.StringVar(&testNames, "t", "SET,GET,LPUSH,LPOP", "Only run the comma separated list of tests")
	flag.BoolVar(&verbose, "v", false, "Show failed requests")
	flag.Parse()

	// do ulimit -n 65535 for server
	http.DefaultTransport.(*http.Transport).MaxIdleConns = clients
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = clients

	client := radish.NewClient(host, port)
	wg := new(sync.WaitGroup)

	testNames = strings.ToUpper(testNames)
	testList := strings.Split(testNames, ",")
	if len(testList) == 0 {
		return
	}

	test := Test{
		remainingRequests: Counter{val: int(float64(requests)/float64(len(testList)) + 0.5)},
		client:            client,
		wg:                wg,
		sampleDataLen:     sampleDataLen,
		keySpaceLen:       keySpaceLen,
		verbose:           verbose,
	}

	tests := make(map[string]*Test)

	testCopy := new(Test)
	*testCopy = test
	tests["SET"] = testCopy
	tests["SET"].cmd = "SET"
	tests["SET"].fnc = func(t *Test) (bool, error) {
		err := t.client.Set(t.getStringKey(), t.getData(), 0).Err()
		return err == nil, err
	}

	testCopy = new(Test)
	*testCopy = test
	tests["GET"] = testCopy
	tests["GET"].cmd = "GET"
	tests["GET"].fnc = func(t *Test) (bool, error) {
		err := t.client.Get(t.getStringKey()).Err()
		return err == nil || err == radish.ErrNotFound, err
	}

	testCopy = new(Test)
	*testCopy = test
	tests["LPUSH"] = testCopy
	tests["LPUSH"].cmd = "LPUSH"
	tests["LPUSH"].fnc = func(t *Test) (bool, error) {
		err := t.client.LPush(t.getListKey(), t.getData()).Err()
		return err == nil, err
	}

	testCopy = new(Test)
	*testCopy = test
	tests["LPOP"] = testCopy
	tests["LPOP"].cmd = "LPOP"
	tests["LPOP"].fnc = func(t *Test) (bool, error) {
		err := t.client.LPop(t.getListKey()).Err()
		return err == nil || err == radish.ErrNotFound, err
	}

	start := time.Now()
	for n := 0; n < clients; {
		if test, ok := tests[testList[n%len(testList)]]; ok {
			wg.Add(1)
			n++
			go test.Run()
		} else {
			fmt.Printf("Invalid test: %q\n", testList[n%len(testList)])
			os.Exit(2)
		}
	}

	wg.Wait()
	elapsed := time.Since(start)

	var totalOK, totalFailed int
	for _, t := range testList {
		if test, ok := tests[t]; ok {
			totalOK += test.succeeded.Get()
			totalFailed += test.failed.Get()
			fmt.Printf(
				"%s: %d/%d success\n",
				t,
				test.succeeded.Get(),
				test.succeeded.Get()+test.failed.Get(),
			)
		}
	}

	fmt.Printf(
		"Total: %d/%d, %s, %0.f requests per second\n",
		totalOK,
		totalFailed+totalOK,
		elapsed,
		float64(requests)/(elapsed.Seconds()),
	)
}

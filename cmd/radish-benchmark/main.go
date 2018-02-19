package main

import (
	"flag"
	"fmt"
	"github.com/mshaverdo/radish/log"
	"github.com/mshaverdo/radish/radish-client"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	sampleDataLen   = 100
	stringCount     = 1000
	dictCount       = 0
	fieldCount      = 0
	repeatsCOunt    = 100
	setWorkersCount = 10
	getWorkersCount = 10

	successed, failed Counter
)

type Counter struct {
	mu  sync.Mutex
	val int
}

func (c *Counter) Add(d int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.val += d
}

func (c *Counter) Get() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.val
}

func main() {
	//TODO: Refactor it!
	var (
		host string
		port int
	)

	// do ulimit -n 65535 for server
	http.DefaultTransport.(*http.Transport).MaxIdleConns = setWorkersCount + getWorkersCount
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = setWorkersCount + getWorkersCount

	flag.StringVar(&host, "h", "localhost", "The Radish host.")
	flag.IntVar(&port, "p", 6380, "The Radish port.")
	flag.Parse()

	client := radish.NewClient(host, port)

	stringKeys, dictKeys, dictFields := getKeys(stringCount, dictCount, fieldCount)

	//client.Del(stringKeys...)
	//return

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < setWorkersCount; i++ {
		wg.Add(1)
		go workerSet(&wg, client, stringKeys, dictKeys, dictFields, repeatsCOunt)
	}

	wg.Wait()
	setDuration := time.Now().Sub(start)
	totalSets := successed.Get() + failed.Get()
	setsPerSecond := float64(totalSets) / setDuration.Seconds()

	log.Infof("Set: success: %d/%d sets in %s, %d sets/second", successed.Get(), totalSets, setDuration, int(setsPerSecond))

	successed, failed = Counter{}, Counter{}
	start = time.Now()
	for i := 0; i < getWorkersCount; i++ {
		wg.Add(1)
		go workerGet(&wg, client, stringKeys, dictKeys, dictFields, repeatsCOunt)
	}

	wg.Wait()
	getDuration := time.Now().Sub(start)
	totalGets := successed.Get() + failed.Get()
	getsPerSecond := float64(totalGets) / getDuration.Seconds()

	log.Infof("Get: success: %d/%d gets in %s, %d gets/second", successed.Get(), totalGets, getDuration, int(getsPerSecond))

	successed, failed = Counter{}, Counter{}
	start = time.Now()
	for i := 0; i < getWorkersCount; i++ {
		wg.Add(1)
		go workerGet(&wg, client, stringKeys, dictKeys, dictFields, repeatsCOunt)
	}

	for i := 0; i < setWorkersCount; i++ {
		wg.Add(1)
		go workerSet(&wg, client, stringKeys, dictKeys, dictFields, repeatsCOunt)
	}

	wg.Wait()
	mixedDuration := time.Now().Sub(start)
	totalMixed := successed.Get() + failed.Get()
	mixedPerSecond := float64(totalMixed) / mixedDuration.Seconds()

	log.Infof("Get&Set: success: %d/%d Gets&Sets in %s, %d gets&sets/second", successed.Get(), totalMixed, mixedDuration, int(mixedPerSecond))
}

func getKeys(stringCount, dictCount, fieldCount int) (stringKeys, dictKeys, dictFields []string) {
	for i := 0; i < stringCount; i++ {
		stringKeys = append(stringKeys, fmt.Sprintf("string_%d", i))
	}
	for i := 0; i < dictCount; i++ {
		dictKeys = append(dictKeys, fmt.Sprintf("dict_%d", i))
	}
	for i := 0; i < fieldCount; i++ {
		dictFields = append(dictFields, fmt.Sprintf("field_%d", i))
	}

	return stringKeys, dictKeys, dictFields
}

func getData() string {
	val := time.Now().Format("15:04:05.000")
	repeats := sampleDataLen - len(val)
	if repeats < 0 {
		repeats = 0
	}
	return strings.Repeat("=", repeats) + val
}

func workerSet(wg *sync.WaitGroup, client *radish.Client, stringKeys, dictKeys, dictFields []string, repeats int) {
	for step := 0; step < repeats; step++ {
		for _, key := range stringKeys {
			val := getData()
			err := client.Set(key, val, 0).Err()
			if err != nil {
				failed.Add(1)
				log.Errorf("Set(%q, %q): %s", key, val, err.Error())
			} else {
				successed.Add(1)
			}
		}
		for _, key := range dictKeys {
			for _, field := range dictFields {
				val := getData()
				err := client.HSet(key, field, val).Err()
				if err != nil {
					failed.Add(1)
					log.Errorf("HSet(%q, %q, %q): %s", key, field, val, err.Error())
				} else {
					successed.Add(1)
				}
			}
		}
	}

	wg.Done()
}

func workerGet(wg *sync.WaitGroup, client *radish.Client, stringKeys, dictKeys, dictFields []string, repeats int) {
	for step := 0; step < repeats; step++ {
		for _, key := range stringKeys {
			err := client.Get(key).Err()
			if err != nil {
				failed.Add(1)
				log.Errorf("Get(%q): %s", key, err.Error())
			} else {
				successed.Add(1)
			}
		}
		for _, key := range dictKeys {
			for _, field := range dictFields {
				err := client.HGet(key, field).Err()
				if err != nil {
					failed.Add(1)
					log.Errorf("HGet(%q, %q): %s", key, field, err.Error())
				} else {
					successed.Add(1)
				}
			}
		}
	}

	wg.Done()
}

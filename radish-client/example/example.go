package main

import (
	"fmt"
	"github.com/mshaverdo/radish/radish-client"
	"time"
)

func main() {
	client := radish.NewClient("localhost", 6380)

	// Ensure, that keys are not exists before our trip
	err := client.Del("key", "key2").Err()
	if err != nil {
		panic(err)
	}

	// Set only one key
	err = client.Set("key1", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	// Try to get existing and non-existing keys
	printStringResult("key1", client.Get("key1"))
	printStringResult("key2", client.Get("key2"))

	// Define key2 as a dict {f1: 42, f1: true}
	err = client.HSet("key2", "f1", 42).Err()
	err = client.HSet("key2", "f2", true).Err()
	if err != nil {
		panic(err)
	}

	// Try to get string value from key2
	printStringResult("key2", client.Get("key2"))

	// Get field values from key2
	printStringResult("key2", client.HGet("key2", "f1"))
	printStringResult("key2", client.HGet("key2", "f2"))

	// Expire key1 after 1 second
	key := "key1"
	ok, err := client.Expire(key, 1*time.Second).Result()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%q became volatile: %t\n", key, ok)

	// key1 still exists
	printStringResult(key, client.Get(key))
	time.Sleep(1 * time.Second)

	// key1 has gone
	printStringResult(key, client.Get(key))
}

func printStringResult(key string, result *radish.StringResult) {
	if result.Err() == radish.ErrNotFound {
		fmt.Printf("%q: does not exist\n", key)
	} else if result.Err() != nil {
		fmt.Printf("%q: Oops! Something wrong happens: %q\n", key, result.Err().Error())
	} else {
		fmt.Printf("%q: %q\n", key, result.Val())
	}
}

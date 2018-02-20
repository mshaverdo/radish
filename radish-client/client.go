package radish

import (
	"errors"
	"fmt"
	"github.com/mshaverdo/radish/message"
	"io"
	"io/ioutil"
	"net/http"
	netUrl "net/url"
	"strconv"
	"time"
)

const statusHeader = "X-Radish-Status"

const ErrNotFound = RadishError("radish: NotFound")
const ErrTypeMismatch = RadishError("radish: ErrTypeMismatch")

var (
	RequestTimeout = time.Second * 10
)

type RadishError string

func (e RadishError) Error() string { return string(e) }

type Client struct {
	// host:port
	host       string
	httpClient *http.Client
}

func NewClient(host string, port int) *Client {
	return &Client{
		host:       fmt.Sprintf("%s:%d", host, port),
		httpClient: &http.Client{Timeout: RequestTimeout},
	}
}

// Keys returns all keys matching glob pattern
func (c *Client) Keys(pattern string) *StringSliceResult {
	url := c.getUrl("KEYS", pattern)
	payload, err := c.requestSingleMulti(false, url, nil)
	return newStringSliceResult(payload, err)
}

// Get the value of key. If the key does not exist the special value nil is returned.
func (c *Client) Get(key string) *StringResult {
	url := c.getUrl("GET", key)
	payload, err := c.requestSingleSingle(false, url, nil)
	return newStringResult(payload, err)
}

// Set key to hold the string value and set key to timeout after a given number of seconds.
// If key already holds a value, it is overwritten, regardless of its type.
// Zero expiration means the key has no expiration time.
func (c *Client) Set(key string, value interface{}, expiration time.Duration) *StatusResult {
	url := c.getUrl("SET", key)
	if expiration != 0 {
		url = c.getUrl("SETEX", key, strconv.Itoa(int(expiration.Seconds())))
	}

	bytesValue, err := convertToBytes(value)
	if err != nil {
		newStatusResult(err)
	}

	_, err = c.requestSingleSingle(true, url, bytesValue)
	return newStatusResult(err)

}

// Del Removes the specified keys, ignoring not existing and returns count of actually removed values.
func (c *Client) Del(keys ...string) *IntResult {
	url := c.getUrl("DEL", keys...)
	payload, err := c.requestSingleSingle(true, url, nil)
	return newIntResult(payload, err)
}

// HSet Sets field in the hash stored at key to value.
func (c *Client) HSet(key, field string, value interface{}) *BoolResult {
	url := c.getUrl("DSET", key, field)

	bytesValue, err := convertToBytes(value)
	if err != nil {
		newStatusResult(err)
	}

	payload, err := c.requestSingleSingle(true, url, bytesValue)
	return newBoolResult(payload, err)
}

// HGetAll Returns all fields and values of the hash stored at key.
func (c *Client) HGetAll(key string) *StringStringMapResult {
	url := c.getUrl("DGETALL", key)
	payload, err := c.requestSingleMulti(false, url, nil)
	return newStringStringMapResult(payload, err)
}

// HKeys Returns all field names in the dict stored at key.
func (c *Client) HKeys(key string) *StringSliceResult {
	url := c.getUrl("DKEYS", key, "*")
	payload, err := c.requestSingleMulti(false, url, nil)
	return newStringSliceResult(payload, err)
}

// HDel Removes the specified keys, ignoring not existing and returns count of actually removed values.
func (c *Client) HDel(key string, fields ...string) *IntResult {
	args := make([]string, len(fields)+1)
	args[0] = key
	copy(args[1:], fields)
	url := c.getUrl("DDEL", args...)
	payload, err := c.requestSingleSingle(true, url, nil)
	return newIntResult(payload, err)
}

// HGet Returns the value associated with field in the dict stored at key.
func (c *Client) HGet(key, field string) *StringResult {
	url := c.getUrl("DGET", key, field)
	payload, err := c.requestSingleSingle(false, url, nil)
	return newStringResult(payload, err)

}

// LRange returns the specified elements of the list stored at key.
func (c *Client) LRange(key string, start, stop int) *StringSliceResult {
	url := c.getUrl("LRANGE", key, strconv.Itoa(start), strconv.Itoa(stop))
	payload, err := c.requestSingleMulti(false, url, nil)
	return newStringSliceResult(payload, err)
}

// LPush Insert all the specified values at the head of the list stored at key.
func (c *Client) LPush(key string, values ...interface{}) *IntResult {
	url := c.getUrl("LPUSH", key)

	var err error
	bytesValues := make([][]byte, len(values))
	for i, v := range values {
		bytesValues[i], err = convertToBytes(v)
		if err != nil {
			newStatusResult(err)
		}
	}

	payload, err := c.requestMultiSingle(url, bytesValues)
	return newIntResult(payload, err)
}

// LLen Returns the length of the list stored at key.
func (c *Client) LLen(key string) *IntResult {
	url := c.getUrl("LLEN", key)
	payload, err := c.requestSingleSingle(false, url, nil)
	return newIntResult(payload, err)
}

// LIndex Returns the element at index index in the list stored at key.
func (c *Client) LIndex(key string, index int) *StringResult {
	url := c.getUrl("LINDEX", key, strconv.Itoa(index))
	payload, err := c.requestSingleSingle(false, url, nil)
	return newStringResult(payload, err)
}

// LSet Sets the list element at index to value.
func (c *Client) LSet(key string, index int, value interface{}) *StatusResult {
	url := c.getUrl("LSET", key, strconv.Itoa(index))

	bytesValue, err := convertToBytes(value)
	if err != nil {
		newStatusResult(err)
	}

	_, err = c.requestSingleSingle(true, url, bytesValue)
	return newStatusResult(err)
}

// LPop Removes and returns the first element of the list stored at key.
func (c *Client) LPop(key string) *StringResult {
	url := c.getUrl("LPOP", key)
	payload, err := c.requestSingleSingle(true, url, nil)
	return newStringResult(payload, err)
}

// TTL Returns the remaining time to live of a key that has a timeout.
func (c *Client) TTL(key string) *DurationResult {
	url := c.getUrl("TTL", key)
	payload, err := c.requestSingleSingle(false, url, nil)

	return newDurationResult(payload, err)
}

// Expire sets a timeout on key. After the timeout has expired, the key will automatically be deleted.
func (c *Client) Expire(key string, expiration time.Duration) *BoolResult {
	url := c.getUrl("EXPIRE", key, strconv.Itoa(int(expiration.Seconds())))
	val, err := c.requestSingleSingle(true, url, nil)
	return newBoolResult(val, err)
}

// Persist Removes the existing timeout on key.
func (c *Client) Persist(key string) *BoolResult {
	url := c.getUrl("PERSIST", key)
	val, err := c.requestSingleSingle(true, url, nil)
	return newBoolResult(val, err)
}

func (c *Client) getUrl(cmd string, args ...string) string {
	path := fmt.Sprintf("/%s", netUrl.PathEscape(cmd))
	for _, key := range args {
		path += fmt.Sprintf("/%s", netUrl.PathEscape(key))
	}

	u := netUrl.URL{
		Scheme: "http",
		Host:   c.host,
	}

	return u.String() + path
}

// requestSingleSingle send single-part request and waiting for single-part response
func (c *Client) requestSingleSingle(usePost bool, url string, payload []byte) (result []byte, err error) {
	request, err := getRequestSingle(usePost, url, payload)
	if err != nil {
		return nil, err
	}

	response, err := c.doRequest(request)
	if err != nil {
		return nil, err
	}

	return parseResponseSingle(response)
}

// requestSingleMulti send single-part request and waiting for multi-part response
func (c *Client) requestSingleMulti(usePost bool, url string, payload []byte) (result [][]byte, err error) {
	request, err := getRequestSingle(usePost, url, payload)
	if err != nil {
		return nil, err
	}

	response, err := c.doRequest(request)
	if err != nil {
		return nil, err
	}

	return parseResponseMulti(response)
}

// requestMultiSingle send multi-part request and waiting for single-part response
func (c *Client) requestMultiSingle(url string, multiPayloads [][]byte) (result []byte, err error) {
	request, err := getRequestMulti(url, multiPayloads)
	if err != nil {
		return nil, err
	}

	response, err := c.doRequest(request)
	if err != nil {
		return nil, err
	}

	return parseResponseSingle(response)
}

func (c *Client) doRequest(request *http.Request) (*http.Response, error) {
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode == http.StatusOK {
		return response, nil
	}

	defer func() {
		// it isn't enough just to close body
		io.Copy(ioutil.Discard, response.Body)
		response.Body.Close()
	}()

	// Something wrong happens
	errorStatus := response.Header.Get(statusHeader)
	switch errorStatus {
	case message.StatusNotFound.String():
		return nil, ErrNotFound
	case message.StatusTypeMismatch.String():
		return nil, ErrTypeMismatch
	case "":
		body, _ := ioutil.ReadAll(response.Body)
		return nil, fmt.Errorf(
			"Unknown command status. Http status: %s\nBody: %s",
			response.Status,
			body,
		)
	default:
		return nil, errors.New(errorStatus)
	}
}

package radish

import (
	"fmt"
	"strconv"
	"time"
)

// String result representation, inspired by go-redis/redis
type StringResult struct {
	val []byte
	err error
}

func newStringResult(val []byte, err error) *StringResult {
	return &StringResult{val: val, err: err}
}

func (r *StringResult) Val() string {
	return string(r.val)
}

func (r *StringResult) Err() error {
	return r.err
}

func (r *StringResult) Result() (string, error) {
	return r.Val(), r.err
}

func (r *StringResult) Bytes() ([]byte, error) {
	return r.val, r.err
}

func (r *StringResult) Int64() (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	return strconv.ParseInt(r.Val(), 10, 64)
}

func (r *StringResult) Uint64() (uint64, error) {
	if r.err != nil {
		return 0, r.err
	}
	return strconv.ParseUint(r.Val(), 10, 64)
}

func (r *StringResult) Float64() (float64, error) {
	if r.err != nil {
		return 0, r.err
	}
	return strconv.ParseFloat(r.Val(), 64)
}

func (r *StringResult) String() string {
	return r.Val()
}

// Slice of strings result representation, inspired by go-redis/redis
type StringSliceResult struct {
	val [][]byte
	err error
}

func newStringSliceResult(val [][]byte, err error) *StringSliceResult {
	return &StringSliceResult{val: val, err: err}
}

func (r *StringSliceResult) Val() []string {
	result := make([]string, len(r.val))
	for i, v := range r.val {
		result[i] = string(v)
	}
	return result
}

func (r *StringSliceResult) Err() error {
	return r.err
}

func (r *StringSliceResult) Result() ([]string, error) {
	return r.Val(), r.err
}

func (r *StringSliceResult) Bytes() ([][]byte, error) {
	return r.val, r.err
}

func (r *StringSliceResult) String() string {
	return fmt.Sprintf("%v", r.Val())
}

// Int result representation, inspired by go-redis/redis
type IntResult struct {
	val int
	err error
}

func newIntResult(val []byte, err error) *IntResult {
	if err != nil {
		return &IntResult{val: 0, err: err}
	}
	result := &IntResult{}
	result.val, result.err = strconv.Atoi(string(val))
	return result
}

func (r *IntResult) Val() int {
	return r.val
}

func (r *IntResult) Err() error {
	return r.err
}

func (r *IntResult) Result() (int, error) {
	return r.val, r.err
}

func (r *IntResult) String() string {
	return strconv.Itoa(r.val)
}

// Status of command result representation, inspired by go-redis/redis
type StatusResult struct {
	err error
}

func newStatusResult(err error) *StatusResult {
	return &StatusResult{err: err}
}

func (r *StatusResult) Val() string {
	if r.err == nil {
		return "OK"
	} else {
		return r.err.Error()
	}
}

func (r *StatusResult) Err() error {
	return r.err
}

func (r *StatusResult) Result() (string, error) {
	return r.Val(), r.err
}

func (r *StatusResult) String() string {
	return r.Val()
}

// Status of command result representation, inspired by go-redis/redis
type StringStringMapResult struct {
	val map[string][]byte
	err error
}

func newStringStringMapResult(val [][]byte, err error) *StringStringMapResult {
	if err != nil {
		return &StringStringMapResult{val: nil, err: err}
	}

	if len(val)%2 != 0 {
		return &StringStringMapResult{val: nil, err: fmt.Errorf("odd len(val) = %d", len(val))}
	}

	mapVal := make(map[string][]byte)
	for i, v := range val {
		if i%2 == 0 {
			continue
		}

		mapVal[string(val[i-1])] = v
	}

	return &StringStringMapResult{val: mapVal, err: err}
}

func (r *StringStringMapResult) Val() map[string]string {
	result := make(map[string]string, len(r.val))
	for k, v := range r.val {
		result[k] = string(v)
	}
	return result
}

func (r *StringStringMapResult) Err() error {
	return r.err
}

func (r *StringStringMapResult) Result() (map[string]string, error) {
	return r.Val(), r.err
}

func (r *StringStringMapResult) Bytes() (map[string][]byte, error) {
	return r.val, r.err
}

func (r *StringStringMapResult) String() string {
	return fmt.Sprintf("%v", r.Val())
}

// Bool  result representation, inspired by go-redis/redis
// Int result representation, inspired by go-redis/redis
type BoolResult struct {
	val bool
	err error
}

func newBoolResult(val []byte, err error) *BoolResult {
	return &BoolResult{val: string(val) == "1", err: err}
}

func (r *BoolResult) Val() bool {
	return r.val
}

func (r *BoolResult) Err() error {
	return r.err
}

func (r *BoolResult) Result() (bool, error) {
	return r.val, r.err
}

func (r *BoolResult) String() string {
	return fmt.Sprintf("%t", r.Val())
}

// Int result representation, inspired by go-redis/redis
type DurationResult struct {
	val time.Duration
	err error
}

func newDurationResult(val []byte, err error) *DurationResult {
	if err != nil {
		return &DurationResult{val: 0, err: err}
	}
	result := &DurationResult{}
	var seconds int
	seconds, result.err = strconv.Atoi(string(val))

	result.val = time.Duration(seconds) * time.Second
	return result
}

func (r *DurationResult) Val() time.Duration {
	return r.val
}

func (r *DurationResult) Err() error {
	return r.err
}

func (r *DurationResult) Result() (time.Duration, error) {
	return r.val, r.err
}

func (r *DurationResult) String() string {
	return r.Val().String()
}

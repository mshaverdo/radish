package message

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Request is a container, represents a Command, parsed from external API interface
type Request struct {
	//Time is a message creation time
	Time time.Time
	//Unique, monotony growing request ID, initialized on writing to WAL
	Id int64
	// Cmd is a Command type
	Cmd string
	// Args is a list of command positional args
	Args [][]byte
	// Payload carrys value for SET/HSET/etc command
}

// NewRequest constructs new Request object
func NewRequest(cmd string, args [][]byte) *Request {
	return &Request{Time: time.Now(), Cmd: cmd, Args: args}
}

// GetArgumentInt returns int argument by index i. Return error if unable to parse int, or requested index too big
func (r *Request) GetArgumentInt(i int) (result int, err error) {
	if i > len(r.Args)-1 {
		return 0, errors.New(fmt.Sprintf("Trying to get not existing argument: %d > %d", i, len(r.Args)-1))
	}

	if result, err = strconv.Atoi(string(r.Args[i])); err != nil {
		return 0, errors.New(fmt.Sprintf("Args[%d] isn't int: %q", i, err.Error()))
	}

	return result, err
}

// GetArgumentInt returns string argument by index i. Return error if requested index too big
func (r *Request) GetArgumentString(i int) (result string, err error) {
	if i > len(r.Args)-1 {
		return "", errors.New(fmt.Sprintf("Trying to get not existing argument: %d > %d", i, len(r.Args)-1))
	}

	return string(r.Args[i]), nil
}

// GetArgumentVariadicString rest of returns string args beginning from i index
func (r *Request) GetArgumentVariadicString(i int) (result []string, err error) {
	if i > len(r.Args)-1 {
		return nil, errors.New(fmt.Sprintf("Trying to get not existing argument: %d > %d", i, len(r.Args)-1))
	}
	restArgs := r.Args[i:]
	result = make([]string, len(restArgs))
	for i, v := range restArgs {
		result[i] = string(v)
	}
	return result, nil
}

// GetArgumentVariadicBytes rest of returns bytes args beginning from i index
func (r *Request) GetArgumentVariadicBytes(i int) (result [][]byte, err error) {
	if i > len(r.Args)-1 {
		return nil, errors.New(fmt.Sprintf("Trying to get not existing argument: %d > %d", i, len(r.Args)-1))
	}
	return r.Args[i:], nil
}

// GetArgumentBytes returns bytes argument by index i. Return error if requested index too big
func (r *Request) GetArgumentBytes(i int) (result []byte, err error) {
	if i > len(r.Args)-1 {
		return nil, errors.New(fmt.Sprintf("Trying to get not existing argument: %d > %d", i, len(r.Args)-1))
	}

	return r.Args[i], nil
}

func (r *Request) String() string {
	return fmt.Sprintf(
		"Request{\n\tCmd: %q \n\tArgs: %q \n}",
		r.Cmd,
		r.Args,
	)
}

//go:generate stringer -type=Status
type Status int

const (
	StatusOk Status = iota
	StatusError
	StatusNotFound
	StatusInvalidCommand
	StatusInvalidArguments
	StatusTypeMismatch
)

//go:generate stringer -type=ResponseKind
type ResponseKind int

const (
	KindStatus ResponseKind = iota
	KindInt
	KindString
	KindStringSlice
)

// Response is a container, represents a Response to Request Command
type Response struct {
	Status   Status
	Kind     ResponseKind
	Payloads [][]byte
}

// NewResponse constructs new Response object with single payload
func NewResponse(status Status, kind ResponseKind, payloads [][]byte) *Response {
	return &Response{Status: status, Kind: kind, Payloads: payloads}
}

func (r *Response) String() string {
	multiPayload := make([]string, len(r.Payloads))
	for i, v := range r.Payloads {
		multiPayload[i] = string(v)
	}

	return fmt.Sprintf(
		"Response{\n\tStatus: %q \n\tKind: %q \n\tPayloads:%q \n}",
		r.Status,
		r.Kind,
		multiPayload,
	)
}

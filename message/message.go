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
	Args []string
	// Meta is a dict of named parameters such as TTL
	Meta map[string]string
	// Payload carrys value for SET/HSET/etc command
	Payload []byte
	// MultiPayloads intended for MSET/HMSET and other bulk commands
	MultiPayloads [][]byte
}

// NewRequestSingle constructs new Request object with single payload
func NewRequestSingle(cmd string, args []string, meta map[string]string, payload []byte) *Request {
	return &Request{Time: time.Now(), Cmd: cmd, Args: args, Meta: meta, Payload: payload}
}

// NewRequestMulti constructs new Request object with multi payloads
func NewRequestMulti(cmd string, args []string, meta map[string]string, multiPayloads [][]byte) *Request {
	return &Request{Time: time.Now(), Cmd: cmd, Args: args, Meta: meta, MultiPayloads: multiPayloads}
}

// GetArgumentInt returns int argument by index i. Return error if unable to parse int, or requested index too big
func (r *Request) GetArgumentInt(i int) (result int, err error) {
	if i > len(r.Args)-1 {
		return 0, errors.New(fmt.Sprintf("Trying to get not existing argument: %d > %d", i, len(r.Args)-1))
	}

	if result, err = strconv.Atoi(r.Args[i]); err != nil {
		return 0, errors.New(fmt.Sprintf("Args[%d] isn't int: %q", i, err.Error()))
	}

	return result, err
}

// GetArgumentInt returns int argument by index i. Return error if requested index too big
func (r *Request) GetArgumentString(i int) (result string, err error) {
	if i > len(r.Args)-1 {
		return "", errors.New(fmt.Sprintf("Trying to get not existing argument: %d > %d", i, len(r.Args)-1))
	}

	return r.Args[i], nil
}

// GetArgumentVariadicString rest of returns string args beginning from i index
func (r *Request) GetArgumentVariadicString(i int) (result []string, err error) {
	if i > len(r.Args)-1 {
		return nil, errors.New(fmt.Sprintf("Trying to get not existing argument: %d > %d", i, len(r.Args)-1))
	}

	return r.Args[i:], nil
}

func (r *Request) String() string {
	multiPayload := make([]string, len(r.MultiPayloads))
	for i, v := range r.MultiPayloads {
		multiPayload[i] = string(v)
	}

	return fmt.Sprintf(
		"Request{\n\tCmd: %q \n\tArgs: %q \n\tPayload: %q \n\tMultiPayloads: %q \n}",
		r.Cmd,
		r.Args,
		string(r.Payload),
		multiPayload,
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

// Response is a container, represents a Response to Request Command
type Response struct {
	Status Status
	// Payload carrys value for SET/DSET/etc command
	Payload []byte
	// MultiPayloads intended for LPush and other bulk commands
	MultiPayloads [][]byte
}

// NewResponse constructs new Response object with single payload
func NewResponseSingle(status Status, payload []byte) *Response {
	return &Response{Status: status, Payload: payload}
}

// NewResponse constructs new Response object with multi payloads
func NewResponseMulti(status Status, multiPayloads [][]byte) *Response {
	return &Response{Status: status, MultiPayloads: multiPayloads}
}

func (r *Response) String() string {
	multiPayload := make([]string, len(r.MultiPayloads))
	for i, v := range r.MultiPayloads {
		multiPayload[i] = string(v)
	}

	return fmt.Sprintf(
		"Response{\n\tStatus: %q \n\tPayload: %q \n\tMultiPayloads:%q \n}",
		r.Status,
		string(r.Payload),
		multiPayload,
	)
}

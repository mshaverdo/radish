package message

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

//TODO: change Request to an interface for uniformity with Response
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

// ArgumentsLen returns len of Request.Args
func (r *Request) ArgumentsLen() int {
	return len(r.Args)
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

// Response is a container, represents a Response to Request Command
type Response interface {
	fmt.Stringer
	Bytes() [][]byte
	Status() Status
}

///////////////////////// ResponseStatus ///////////////////////////////////
type ResponseStatus struct {
	status  Status
	payload string
}

func NewResponseStatus(status Status, payload string) *ResponseStatus {
	return &ResponseStatus{status: status, payload: payload}
}

func (r *ResponseStatus) Payload() string {
	return r.payload
}

func (r *ResponseStatus) Status() Status {
	return r.status
}

func (r *ResponseStatus) Bytes() [][]byte {
	return [][]byte{[]byte(r.payload)}
}

func (r *ResponseStatus) String() string {
	return fmt.Sprintf(
		"ResponseStatus{\n\tStatus: %q \n\tMessage: %q \n}",
		r.status,
		r.payload,
	)
}

///////////////////////// ResponseInt ///////////////////////////////////
type ResponseInt struct {
	status  Status
	payload int
}

func NewResponseInt(status Status, payload int) *ResponseInt {
	return &ResponseInt{status: status, payload: payload}
}

func (r *ResponseInt) Payload() int {
	return r.payload
}

func (r *ResponseInt) Status() Status {
	return r.status
}

func (r *ResponseInt) Bytes() [][]byte {
	return [][]byte{[]byte(strconv.Itoa(r.payload))}
}

func (r *ResponseInt) String() string {
	return fmt.Sprintf(
		"ResponseStatus{\n\tStatus: %q \n\tPayload: %d \n}",
		r.status,
		r.payload,
	)
}

///////////////////////// ResponseString ///////////////////////////////////
type ResponseString struct {
	status  Status
	payload []byte
}

func NewResponseString(status Status, payload []byte) *ResponseString {
	return &ResponseString{status: status, payload: payload}
}

func (r *ResponseString) Payload() []byte {
	return r.payload
}

func (r *ResponseString) Status() Status {
	return r.status
}

func (r *ResponseString) Bytes() [][]byte {
	return [][]byte{r.payload}
}

func (r *ResponseString) String() string {
	return fmt.Sprintf(
		"ResponseStatus{\n\tStatus: %q \n\tPayload: %q \n}",
		r.status,
		r.payload,
	)
}

///////////////////////// ResponseStringSlice ///////////////////////////////////
type ResponseStringSlice struct {
	status  Status
	payload [][]byte
}

func NewResponseStringSlice(status Status, payload [][]byte) *ResponseStringSlice {
	return &ResponseStringSlice{status: status, payload: payload}
}

func (r *ResponseStringSlice) Payload() [][]byte {
	return r.payload
}

func (r *ResponseStringSlice) Status() Status {
	return r.status
}

func (r *ResponseStringSlice) Bytes() [][]byte {
	return r.payload
}

func (r *ResponseStringSlice) String() string {
	strPayload := make([]string, len(r.payload))
	for i, v := range r.payload {
		strPayload[i] = string(v)
	}
	return fmt.Sprintf(
		"ResponseStatus{\n\tStatus: %q \n\tPayload: %q \n}",
		r.status,
		strPayload,
	)
}

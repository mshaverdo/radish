package message

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

//TODO: change Request to an interface for uniformity with Response

// Type Request defined via gencode: request.schema &  request.schema.gen.go using github.com/andyleap/gencode
//go:generate gencode go -schema request.schema -package message

// NewRequest constructs new Request object
func NewRequest(cmd string, args [][]byte) *Request {
	return &Request{Timestamp: time.Now().Unix(), Cmd: cmd, Args: args}
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
		"Request{\n\tId: %d \n\tCmd: %q \n\tArgs: %q \n}",
		r.Id,
		r.Cmd,
		r.Args,
	)
}

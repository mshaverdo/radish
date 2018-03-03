package message

import (
	"fmt"
	"strconv"
)

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

var _ Response = (*ResponseStatus)(nil)

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

var _ Response = (*ResponseInt)(nil)

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

var _ Response = (*ResponseString)(nil)

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

var _ Response = (*ResponseStringSlice)(nil)

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

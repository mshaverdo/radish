package controller

import (
	"fmt"
	"github.com/mshaverdo/assert"
	"github.com/mshaverdo/radish/core"
	"github.com/mshaverdo/radish/message"
	"strconv"
)

func getResponseInvalidArguments(cmd string, err error) *message.Response {
	return message.NewResponse(
		message.StatusInvalidArguments,
		message.KindStatus,
		[][]byte{[]byte(cmd + ": " + err.Error())},
	)
}

func getResponseCommandError(cmd string, err error) *message.Response {
	statusMap := map[error]message.Status{
		//nil: message.StatusOk,
		core.ErrInvalidParams: message.StatusInvalidArguments,
		core.ErrWrongType:     message.StatusTypeMismatch,
		core.ErrNotFound:      message.StatusNotFound,
		ErrServerShutdown:     message.StatusError,
	}

	status, ok := statusMap[err]
	assert.True(ok, "unknown error: "+err.Error())

	return message.NewResponse(
		status,
		message.KindStatus,
		[][]byte{[]byte(fmt.Sprintf("Error processing %q: %q", cmd, err.Error()))},
	)
}

func getResponseStringPayload(payload []byte) *message.Response {
	return message.NewResponse(
		message.StatusOk,
		message.KindString,
		[][]byte{payload},
	)
}

func getResponseIntPayload(value int) *message.Response {
	return message.NewResponse(
		message.StatusOk,
		message.KindInt,
		[][]byte{[]byte(strconv.Itoa(value))},
	)
}

func getResponseStringSlicePayload(payloads [][]byte) *message.Response {
	return message.NewResponse(
		message.StatusOk,
		message.KindStringSlice,
		payloads,
	)
}

func getResponseStatusOkPayload() *message.Response {
	return message.NewResponse(
		message.StatusOk,
		message.KindStatus,
		nil,
	)
}

func stringsSliceToBytesSlise(s []string) [][]byte {
	result := make([][]byte, len(s))
	for i, v := range s {
		result[i] = []byte(v)
	}

	return result
}

package controller

import (
	"fmt"
	"github.com/mshaverdo/assert"
	"github.com/mshaverdo/radish/core"
	"github.com/mshaverdo/radish/message"
)

func getResponseInvalidArguments(cmd string, err error) message.Response {
	return message.NewResponseStatus(
		message.StatusInvalidArguments,
		cmd+": "+err.Error(),
	)
}

func getResponseCommandError(cmd string, err error) message.Response {
	statusMap := map[error]message.Status{
		//nil: message.StatusOk,
		core.ErrInvalidParams: message.StatusInvalidArguments,
		core.ErrWrongType:     message.StatusTypeMismatch,
		core.ErrNotFound:      message.StatusNotFound,
		ErrServerShutdown:     message.StatusError,
	}

	status, ok := statusMap[err]
	assert.True(ok, "unknown error: "+err.Error())

	return message.NewResponseStatus(
		status,
		fmt.Sprintf("Error processing %q: %q", cmd, err.Error()),
	)
}

func getResponseStringPayload(payload []byte) message.Response {
	return message.NewResponseString(
		message.StatusOk,
		payload,
	)
}

func getResponseIntPayload(value int) message.Response {
	return message.NewResponseInt(
		message.StatusOk,
		value,
	)
}

func getResponseStringSlicePayload(payloads [][]byte) message.Response {
	return message.NewResponseStringSlice(
		message.StatusOk,
		payloads,
	)
}

func getResponseStatusOkPayload() message.Response {
	return message.NewResponseStatus(
		message.StatusOk,
		"",
	)
}

func stringsSliceToBytesSlise(s []string) [][]byte {
	result := make([][]byte, len(s))
	for i, v := range s {
		result[i] = []byte(v)
	}

	return result
}

package controller

import (
	"fmt"
	"github.com/mshaverdo/radish/core"
	"github.com/mshaverdo/radish/message"
	"strconv"
)

func getResponseInvalidArguments(cmd string, err error) *message.Response {
	return message.NewResponseSingle(message.StatusInvalidArguments, []byte(cmd+": "+err.Error()))
}

func getResponseCommandError(cmd string, err error) *message.Response {
	statusMap := map[error]message.Status{
		//nil: message.StatusOk,
		core.ErrInvalidParams: message.StatusInvalidArguments,
		core.ErrWrongType:     message.StatusTypeMismatch,
		core.ErrNotFound:      message.StatusNotFound,
	}

	status, ok := statusMap[err]
	if !ok {
		panic("Program logic error: unknown error: %q" + err.Error())
	}

	return message.NewResponseSingle(
		status,
		[]byte(fmt.Sprintf("Error processing %q: %q", cmd, err.Error())),
	)
}

func getResponseSinglePayload(payload []byte) *message.Response {
	return message.NewResponseSingle(
		message.StatusOk,
		payload,
	)
}

func getResponseEmptyPayload() *message.Response {
	return message.NewResponseSingle(
		message.StatusOk,
		nil,
	)
}

func getResponseIntPayload(value int) *message.Response {
	return message.NewResponseSingle(
		message.StatusOk,
		[]byte(strconv.Itoa(value)),
	)
}

func getResponseMultiPayload(payloads [][]byte) *message.Response {
	return message.NewResponseMulti(
		message.StatusOk,
		payloads,
	)
}

func getResponseMultiStringPayload(payloads []string) *message.Response {
	bytesPayloads := make([][]byte, len(payloads))
	for i, v := range payloads {
		bytesPayloads[i] = []byte(v)
	}
	return message.NewResponseMulti(
		message.StatusOk,
		bytesPayloads,
	)
}

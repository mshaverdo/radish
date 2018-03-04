package controller

import (
	"github.com/mshaverdo/radish/core"
	"github.com/mshaverdo/radish/log"
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
		core.ErrInvalidIndex: message.StatusInvalidArguments,
		core.ErrWrongType:    message.StatusTypeMismatch,
		core.ErrNotFound:     message.StatusNotFound,
		core.ErrNoSuchKey:    message.StatusInvalidArguments,
		ErrServerShutdown:    message.StatusError,
	}

	status, ok := statusMap[err]
	if !ok {
		status = message.StatusError
		log.Debugf("UNKNOWN ERROR: %s", err)
	}

	return message.NewResponseStatus(
		status,
		err.Error(),
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

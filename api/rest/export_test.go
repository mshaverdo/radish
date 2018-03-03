package rest

import (
	"github.com/mshaverdo/radish/message"
	"net/http"
)

func SendResponse(response message.Response, w http.ResponseWriter) {
	sendResponse(response, w)
}

func ParseRequest(httpRequest *http.Request) (*message.Request, error) {
	return parseRequest(httpRequest)
}

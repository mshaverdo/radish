package api

import "github.com/mshaverdo/radish/message"

// MessageHandler processes a Request message and return a response message
type MessageHandler interface {
	HandleMessage(request *message.Request) message.Response
}

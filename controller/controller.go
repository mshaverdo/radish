package controller

import (
	"github.com/mshaverdo/radish/controller/httpserver"
	"github.com/mshaverdo/radish/log"
	"github.com/mshaverdo/radish/message"
)

// ApiServer represents Radish API endpoint interface
type ApiServer interface {
	ListenAndServe() error
	Shutdown() error
}

// Core provides domain operations on the storage -- get, set, keys, hset, hdel, etc.
// Use interface to introduce availability to use mocks during testing
type Core interface {
}

type Controller struct {
	host string
	port int
	srv  ApiServer
	core Core
}

// ListenAndServe starts a new radish server
func (c *Controller) ListenAndServe() error {
	log.Infof("Radish ready to serve at %s:%d", c.host, c.port)
	return c.srv.ListenAndServe()
}

// Shutdown gracefully shuts server down
func (c *Controller) Shutdown() {
	log.Info("Shutting down Radish...")
	c.srv.Shutdown()
	log.Info("Goodbye!")
}

// HandleMessage processes Request and return Response
func (c *Controller) HandleMessage(request *message.Request) *message.Response {
	if len(request.MultiPayloads) > 0 {
		resp := map[string][]byte{}
		for i, v := range request.MultiPayloads {
			resp[i] = append(v, "!!!!!!1"...)
		}

		return message.NewResponse(message.StatusOk, nil, resp)
	} else {
		resp := append(request.Payload, "!!!! boobs"...)
		return message.NewResponse(message.StatusOk, resp, nil)
	}
}

// New Constructs new instance of Controller
func New(host string, port int) *Controller {
	c := Controller{host: host, port: port}
	c.srv = httpserver.New(host, port, &c)

	return &c
}

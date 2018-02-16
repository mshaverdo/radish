package controller

import (
	"fmt"
	"github.com/mshaverdo/radish/controller/httpserver"
	"github.com/mshaverdo/radish/core"
	"github.com/mshaverdo/radish/log"
	"github.com/mshaverdo/radish/message"
	"strconv"
	"time"
)

// ApiServer represents Radish API endpoint interface
type ApiServer interface {
	ListenAndServe() error
	Shutdown() error
}

// Core provides domain operations on the storage -- Get, Set, Keys, etc.
type Core interface {
	// CollectExpired removes expired garbage items from the storage
	CollectExpired() (count int)

	// Keys returns all keys matching glob pattern
	Keys(pattern string) (result []string)

	// Get the value of key. If the key does not exist the special value nil is returned.
	Get(key string) (result []byte, err error)

	// Set key to hold the string value.
	Set(key string, value []byte)

	// Set key to hold the string value and set key to timeout after a given number of seconds.
	SetEx(key string, seconds int, value []byte)

	// Del Removes the specified keys, ignoring not existing and returns count of actually removed values.
	Del(keys []string) (count int)

	// DSet Sets field in the hash stored at key to value.
	DSet(key, field string, value []byte) (count int, err error)

	// DGet Returns the value associated with field in the dict stored at key.
	DGet(key, field string) (result []byte, err error)

	// Returns all field names in the dict stored at key.
	DKeys(key, pattern string) (result []string, err error)

	// DGetAll Returns all fields and values of the hash stored at key.
	DGetAll(key string) (result [][]byte, err error)

	// DDel Removes the specified fields from the hash stored at key.
	DDel(key string, fields []string) (count int, err error)

	// LLen Returns the length of the list stored at key.
	LLen(key string) (count int, err error)

	// LRange returns the specified elements of the list stored at key.
	LRange(key string, start, stop int) (result [][]byte, err error)

	// LIndex Returns the element at index index in the list stored at key.
	LIndex(key string, index int) (result []byte, err error)

	// LSet Sets the list element at index to value.
	LSet(key string, index int, value []byte) (err error)

	// LPush Insert all the specified values at the head of the list stored at key.
	LPush(key string, values [][]byte) (count int, err error)

	// LPop Removes and returns the first element of the list stored at key.
	LPop(key string) (result []byte, err error)

	// Ttl Returns the remaining time to live of a key that has a timeout.
	Ttl(key string) (ttl int, err error)

	// Expire Sets a timeout on key. After the timeout has expired, the key will automatically be deleted.
	Expire(key string, seconds int) (err error)

	// Persist Removes the existing timeout on key.
	Persist(key string) (err error)
}

type Controller struct {
	host string
	port int
	srv  ApiServer
	core Core

	stopChan               chan struct{}
	collectExpiredInterval time.Duration
}

// New Constructs new instance of Controller
func New(host string, port int) *Controller {
	c := Controller{
		host:                   host,
		port:                   port,
		core:                   core.NewCore(core.NewHashEngine()),
		stopChan:               make(chan struct{}),
		collectExpiredInterval: 60 * time.Second,
	}
	c.srv = httpserver.New(host, port, &c)

	return &c
}

// ListenAndServe starts a new radish server
func (c *Controller) ListenAndServe() error {
	go c.runCollector()
	log.Infof("Radish ready to serve at %s:%d", c.host, c.port)
	return c.srv.ListenAndServe()
}

// Shutdown gracefully shuts server down
func (c *Controller) Shutdown() {
	log.Info("Shutting down Radish...")
	close(c.stopChan)
	c.srv.Shutdown()
	log.Info("Goodbye!")
}

// HandleMessage processes Request and return Response
func (c *Controller) HandleMessage(request *message.Request) *message.Response {
	return c.processCommand(request)
}

// runCollector runs Core.CollectExpired() periodically
func (c *Controller) runCollector() {
	tick := time.Tick(c.collectExpiredInterval)
	for {
		select {
		case <-c.stopChan:
			return
		case <-tick:
			count := c.core.CollectExpired()
			log.Debugf("Collected %d expired items", count)
		}
	}
}

func (c *Controller) getResponseInvalidArguments(cmd string, err error) *message.Response {
	return message.NewResponseSingle(message.StatusInvalidArguments, []byte(cmd+": "+err.Error()))
}

func (c *Controller) getResponseCommandError(cmd string, err error) *message.Response {
	statusMap := map[error]message.Status{
		//nil: message.StatusOk,
		core.ErrInvalidParams: message.StatusInvalidArguments,
		core.ErrWrongType:     message.StatusTypeMismatch,
		core.ErrNotFound:      message.StatusTypeMismatch,
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

func (c *Controller) getResponseSinglePayload(payload []byte) *message.Response {
	return message.NewResponseSingle(
		message.StatusOk,
		payload,
	)
}

func (c *Controller) getResponseEmptyPayload() *message.Response {
	return message.NewResponseSingle(
		message.StatusOk,
		nil,
	)
}

func (c *Controller) getResponseIntPayload(value int) *message.Response {
	return message.NewResponseSingle(
		message.StatusOk,
		[]byte(strconv.Itoa(value)),
	)
}

func (c *Controller) getResponseMultiPayload(payloads [][]byte) *message.Response {
	return message.NewResponseMulti(
		message.StatusOk,
		payloads,
	)
}

func (c *Controller) getResponseMultiStringPayload(payloads []string) *message.Response {
	bytesPayloads := make([][]byte, len(payloads))
	for i, v := range payloads {
		bytesPayloads[i] = []byte(v)
	}
	return message.NewResponseMulti(
		message.StatusOk,
		bytesPayloads,
	)
}

func (c *Controller) processCommand(r *message.Request) *message.Response {
	//TODO: use go generate!
	switch r.Cmd {
	case "KEYS":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		result := c.core.Keys(arg0)

		return c.getResponseMultiStringPayload(result)
	case "GET":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.Get(arg0)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseSinglePayload(result)
	case "SET":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		c.core.Set(arg0, r.Payload)

		return c.getResponseEmptyPayload()
	case "SETEX":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentInt(1)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		c.core.SetEx(arg0, arg1, r.Payload)

		return c.getResponseEmptyPayload()
	case "DEL":
		args, err := r.GetArgumentVariadicString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		result := c.core.Del(args)

		return c.getResponseIntPayload(result)
	case "DKEYS":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentString(1)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.DKeys(arg0, arg1)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseMultiStringPayload(result)
	case "DGETALL":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.DGetAll(arg0)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseMultiPayload(result)

	case "DGET":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentString(1)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.DGet(arg0, arg1)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseSinglePayload(result)
	case "DSET":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentString(1)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		count, err := c.core.DSet(arg0, arg1, r.Payload)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseIntPayload(count)
	case "DDEL":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}
		args, err := r.GetArgumentVariadicString(1)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		count, err := c.core.DDel(arg0, args)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseIntPayload(count)
	case "LLEN":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		count, err := c.core.LLen(arg0)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseIntPayload(count)
	case "LRANGE":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentInt(1)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}
		arg2, err := r.GetArgumentInt(2)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.LRange(arg0, arg1, arg2)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseMultiPayload(result)
	case "LINDEX":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentInt(1)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.LIndex(arg0, arg1)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseSinglePayload(result)
	case "LSET":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentInt(1)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		err = c.core.LSet(arg0, arg1, r.Payload)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseEmptyPayload()
	case "LPUSH":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		count, err := c.core.LPush(arg0, r.MultiPayloads)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseIntPayload(count)
	case "LPOP":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.LPop(arg0)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseSinglePayload(result)
	case "TTL":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		ttl, err := c.core.Ttl(arg0)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseIntPayload(ttl)
	case "EXPIRE":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentInt(1)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		err = c.core.Expire(arg0, arg1)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseEmptyPayload()
	case "PERSIST":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return c.getResponseInvalidArguments(r.Cmd, err)
		}

		err = c.core.Persist(arg0)
		if err != nil {
			return c.getResponseCommandError(r.Cmd, err)
		}

		return c.getResponseEmptyPayload()
	default:
		return message.NewResponseSingle(
			message.StatusInvalidCommand,
			[]byte("Unknown command: "+r.Cmd),
		)
	}
}

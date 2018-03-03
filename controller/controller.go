package controller

import (
	"errors"
	"github.com/mshaverdo/radish/api/resp"
	"github.com/mshaverdo/radish/api/rest"
	"github.com/mshaverdo/radish/core"
	"github.com/mshaverdo/radish/log"
	"github.com/mshaverdo/radish/message"
	"sync"
	"time"
)

// ApiServer represents Radish API endpoint interface
type ApiServer interface {
	// ListenAndServe starts the server
	ListenAndServe() error

	// Stop stops server to accept new requests and gracefully finishes current requests
	Stop() error

	// Shutdown shuts Radish and leads to return from Controller.ListenAndServe() that causes application termination
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
	DKeys(key string) (result []string, err error)

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
	Expire(key string, seconds int) (result int)

	// Persist Removes the existing timeout on key.
	Persist(key string) (result int)

	// Storage returns reference to underlying storage to persisting
	Storage() core.Storage

	// SetStorage sets storage after loading
	SetStorage(core.Storage)
}

var (
	ErrServerShutdown = errors.New("server shutdown")
)

//go:generate go run ../tools/gen-processor/main.go

type Controller struct {
	host                   string
	port                   int
	dataDir                string
	isPersistent           bool //if true, persists data on disk
	collectExpiredInterval time.Duration

	srv       ApiServer
	core      Core
	keeper    *Keeper
	processor *Processor

	// wg to wait for service storage-updating goroutines (CollectExpired(), etc)
	serviceWg sync.WaitGroup
	// wg to wait for request handlers
	handlerWg sync.WaitGroup

	isRunningMutex sync.Mutex
	isRunningFlag  bool
	stopChan       chan struct{}
}

// New Constructs new instance of Controller
func New(
	host string,
	port int,
	dataDir string,
	syncPolicy SyncPolicy,
	collectInterval, mergeWalInterval time.Duration,
	useHttp bool,
) *Controller {
	c := Controller{
		host:                   host,
		port:                   port,
		core:                   core.New(core.NewStorageHash()),
		stopChan:               make(chan struct{}),
		collectExpiredInterval: collectInterval,
		dataDir:                dataDir,
		isPersistent:           dataDir != "",
	}

	if useHttp {
		c.srv = rest.NewServer(host, port, &c)
	} else {
		c.srv = resp.NewServer(host, port, &c)
	}

	c.processor = NewProcessor(c.core)

	if c.isPersistent {
		c.keeper = NewKeeper(
			c.core,
			dataDir,
			syncPolicy,
			mergeWalInterval,
		)
	}

	return &c
}

// ListenAndServe starts a new radish server
func (c *Controller) ListenAndServe() error {
	if c.isPersistent {
		if err := c.keeper.Start(); err != nil {
			return err
		}
	}

	c.start()

	// Don't forget to add all background service processes to wg!
	c.serviceWg.Add(1)
	go c.runCollector()

	log.Notice("Radish ready to serve at %s:%d", c.host, c.port)
	return c.srv.ListenAndServe()
}

// Shutdown gracefully shuts server down
func (c *Controller) Shutdown() {
	for !c.isRunning() {
		//wait, while server finishes startup
		time.Sleep(100 * time.Millisecond)
	}

	log.Notice("Shutting down Radish...")
	c.stop()
	c.srv.Stop()

	//wait other goroutines that may interact with storage
	c.serviceWg.Wait()
	c.handlerWg.Wait()

	//OK, no more concurrent threads working with storage
	if c.isPersistent {
		if err := c.keeper.Shutdown(); err != nil {
			log.Error(err.Error())
		}
	}

	c.srv.Shutdown()
	log.Notice("Goodbye!")
}

// HandleMessage processes Request and return Response
func (c *Controller) HandleMessage(request *message.Request) message.Response {
	if !c.isRunning() {
		return getResponseCommandError(request.Cmd, ErrServerShutdown)
	}

	// It's OK to do wg.Add() inside a goroutine, due to c.stop() invoked BEFORE c.handlerWg.Wait()
	c.handlerWg.Add(1)
	defer c.handlerWg.Done()

	response := c.processor.Process(request)

	if c.isPersistent && response.Status() == message.StatusOk && c.processor.IsModifyingRequest(request) {
		if err := c.keeper.WriteToWal(request); err != nil {
			return getResponseCommandError(request.Cmd, err)
		}
	}

	return response
}

func (c *Controller) runCollector() {
	defer c.serviceWg.Done()

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

func (c *Controller) start() {
	c.isRunningMutex.Lock()
	defer c.isRunningMutex.Unlock()
	c.isRunningFlag = true
}

func (c *Controller) stop() {
	c.isRunningMutex.Lock()
	defer c.isRunningMutex.Unlock()
	c.isRunningFlag = false
	close(c.stopChan)
}

func (c *Controller) isRunning() bool {
	//TODO: change to RWmutex, get rid of defer to improve performance
	c.isRunningMutex.Lock()
	defer c.isRunningMutex.Unlock()
	return c.isRunningFlag
}

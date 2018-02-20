package controller

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/mshaverdo/radish/controller/httpserver"
	"github.com/mshaverdo/radish/core"
	"github.com/mshaverdo/radish/log"
	"github.com/mshaverdo/radish/message"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// ApiServer represents Radish API endpoint interface
type ApiServer interface {
	ListenAndServe() error
	Shutdown() error
	Stop() error
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
	Expire(key string, seconds int) (result int)

	// Persist Removes the existing timeout on key.
	Persist(key string) (result int)

	// RestoreData restores data from file.
	LoadData(src io.Reader) (messageId int64, err error)

	// DumpData dumps storage data to specified file.
	// It is NOT thread-safe
	DumpData(dst io.Writer, messageId int64) error
}

type SyncPolicy int

const (
	// SyncNever means newer do walFile.Sync()
	SyncNever SyncPolicy = iota

	// SyncNever means walFile.Sync() every second
	SyncSometimes

	// SyncNever means walFile.Sync() every message write
	SyncAlways
)

const (
	walFileName     = "wal_%v.gob"
	storageFileName = "storage.gob"
)

var (
	ErrServerShutdown = errors.New("server shutdown")
)

type Controller struct {
	host                   string
	port                   int
	dataDir                string
	isMemOnly              bool //if true, don't use any i/o disc operations
	collectExpiredInterval time.Duration
	takeSnapshotInterval   time.Duration
	syncPolicy             SyncPolicy

	srv  ApiServer
	core Core

	walMutex   sync.Mutex
	messageId  int64
	walFile    *os.File
	walEncoder *gob.Encoder
	lastSync   time.Time

	// wg to wait for service storage-updating goroutines (CollectExpired(), etc)
	serviceWg sync.WaitGroup
	// wg to wait for request handlers
	handlerWg sync.WaitGroup

	isRunningMutex sync.Mutex
	isRunningFlag  bool
	stopChan       chan struct{}
}

// New Constructs new instance of Controller
func New(host string, port int, dataDir string) *Controller {
	c := Controller{
		host:                   host,
		port:                   port,
		core:                   core.NewCore(core.NewHashEngine()),
		stopChan:               make(chan struct{}),
		collectExpiredInterval: 60 * time.Second,
		takeSnapshotInterval:   60 * time.Second,
		dataDir:                dataDir,
		isMemOnly:              dataDir == "",
		syncPolicy:             SyncSometimes,
	}
	c.srv = httpserver.New(host, port, &c)

	return &c
}

// ListenAndServe starts a new radish server
func (c *Controller) ListenAndServe() error {
	//TODO: реализовать периодический мерж лога в дмап базы
	//TODO: оставить возможность переключиться на fork(). в форке попробовать реализовать сериализацию через C protobuf
	if !c.isMemOnly {
		err := c.restoreState()
		if err != nil {
			return err
		}

		_, err = c.startNewWal()
		if err != nil {
			return err
		}
	}

	c.start()

	// Don't forget to add all background service processes to wg!
	c.serviceWg.Add(1)
	go c.runCollector()

	log.Infof("Radish ready to serve at %s:%d", c.host, c.port)
	return c.srv.ListenAndServe()
}

// Shutdown gracefully shuts server down
func (c *Controller) Shutdown() {
	log.Info("Shutting down Radish...")
	c.stop()
	c.srv.Stop()

	//wait other goroutines that may interact with storage
	c.serviceWg.Wait()
	c.handlerWg.Wait()

	//OK, no more concurrent threads working with storage
	if !c.isMemOnly {
		err := c.shutdownStorage()
		if err != nil {
			log.Error(err.Error())
		}
	}

	c.srv.Shutdown()
	log.Infof("Goodbye!")
}

// HandleMessage processes Request and return Response
func (c *Controller) HandleMessage(request *message.Request) *message.Response {
	if !c.isRunning() {
		return getResponseCommandError(request.Cmd, ErrServerShutdown)
	}

	// It's OK to do wg.Add() inside a goroutine, due to c.stop() invoked BEFORE c.handlerWg.Wait()
	c.handlerWg.Add(1)
	defer c.handlerWg.Done()

	response := c.processCommand(request)

	if response.Status != message.StatusOk {
		// we don't add any failed requests to WAL: they are not changed any data
		return response
	}

	if c.isMemOnly {
		// all done with this request
		return response
	}

	//TODO: use go generate
	switch request.Cmd {
	case "SET", "SETEX", "DEL", "DSET", "DDEL", "LSET", "LPUSH", "LPOP", "EXPIRE", "PERSIST":
		if err := c.writeToLog(request); err != nil {
			return getResponseCommandError(request.Cmd, err)
		}
	}

	return response
}

func (c *Controller) writeToLog(request *message.Request) error {
	c.walMutex.Lock()
	defer c.walMutex.Unlock()

	c.messageId++
	request.Id = c.messageId
	err := c.walEncoder.Encode(request)

	if c.syncPolicy == SyncAlways || c.syncPolicy == SyncSometimes && time.Since(c.lastSync) > 1*time.Second {
		c.walFile.Sync()
		c.lastSync = time.Now()
	}

	return err
}

func (c *Controller) restoreState() error {
	if c.isRunning() {
		panic("Program logic error: trying to restoreState() on running controller")
	}
	if c.isMemOnly {
		panic("Program logic error: trying to restoreState() in MemOnly mode")
	}
	if err := c.loadStorage(); err != nil {
		return err
	}
	processedWals, err := c.processAllWals()
	if err != nil {
		return err
	}
	// dump storage with merged WALs to disk
	if err := c.dumpStorage(); err != nil {
		return err
	}

	// all OK, remove processed WALs
	for _, v := range processedWals {
		err := os.Remove(v)
		if err != nil {
			log.Warningf("Unable to remove processed WAL %s: %s", v, err)
		}
	}

	return nil
}

func (c *Controller) loadStorage() error {
	if c.isRunning() {
		panic("Program logic error: trying to load storage on running controller")
	}
	if c.isMemOnly {
		panic("Program logic error: trying to load storage in MemOnly mode")
	}

	filename := path.Join(c.dataDir, storageFileName)
	file, err := os.Open(filename)
	if os.IsNotExist(err) {
		// no data file found, just skip
		return nil
	} else if err != nil {
		return fmt.Errorf("Controller.loadStorage(). Unable to open %s: %s", filename, err)
	}
	defer file.Close()

	log.Infof("Loading storage data from %s...", filename)
	//restore commandID from the Storage file
	c.messageId, err = c.core.LoadData(file)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) processAllWals() (processedWals []string, err error) {
	if c.isRunning() {
		panic("Program logic error: trying to processAllWals() on running controller")
	}
	if c.isMemOnly {
		panic("Program logic error: trying to processAllWals() in MemOnly mode")
	}

	allFiles, err := filepath.Glob(c.walFileName("*"))
	if err != nil {
		return nil, fmt.Errorf("Controller.processAllWals(): %s", err)
	}

	var messageIds []int
	for _, v := range allFiles {
		id := 0
		fmt.Sscanf(v, c.walFileName("%d"), &id)
		if id > 0 {
			messageIds = append(messageIds, id)
		}
	}

	sort.Ints(messageIds)

	// process all WALs from earliest to latest
	for _, messageId := range messageIds {
		filename := c.walFileName(messageId)
		if err := c.processWal(filename); err != nil {
			return nil, err
		}
		processedWals = append(processedWals, filename)
	}

	return processedWals, nil
}

func (c *Controller) processWal(filename string) error {
	if c.isRunning() {
		panic("Program logic error: trying to load storage on running controller")
	}
	if c.isMemOnly {
		panic("Program logic error: trying to load storage in MemOnly mode")
	}

	log.Infof("processing WAL %s...", filename)

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	dec := gob.NewDecoder(file)
	req := new(message.Request)
	for err := dec.Decode(req); err != io.EOF; err = dec.Decode(req) {
		if err != nil {
			return fmt.Errorf("Controller.processWal(): can't process %s: %s", filename, err)
		}

		if req.Id <= c.messageId {
			// skip messages, that already in the storage
			continue
		}

		err = fixWalRequestTtl(req)
		if err != nil {
			return fmt.Errorf("Controller.processWal(): can't process %s: %s \nrequest: %s", filename, err, req)
		}

		resp := c.processCommand(req)
		if resp.Status != message.StatusOk {
			// we got an error, but this request was successful. Something went wrong
			return fmt.Errorf("Controller.processWal(): can't process %s: \nrequest: %s \nresponse: %s", filename, req, resp)
		}

		c.messageId = req.Id
		req = new(message.Request)
	}

	return nil
}

func (c *Controller) dumpStorage() error {
	if c.isRunning() {
		panic("Program logic error: trying to dumpStorage() on running controller")
	}
	if c.isMemOnly {
		panic("Program logic error: trying to dumpStorage() in MemOnly mode")
	}

	//remove expired items to decrease dump size
	c.core.CollectExpired()

	file, err := ioutil.TempFile(filepath.Dir(c.storageFileName()), filepath.Base(c.storageFileName()))
	defer file.Close()

	if err != nil {
		return fmt.Errorf("Controller.dumpStorage(): %s", err)
	}

	//save commandID to beginning of Storage file
	err = c.core.DumpData(file, c.messageId)
	if err != nil {
		return fmt.Errorf("Controller.dumpStorage(): %s", err)
	}

	err = os.Rename(file.Name(), c.storageFileName())
	if err != nil {
		return fmt.Errorf("Controller.dumpStorage(): %s", err)
	}

	return nil
}

func (c *Controller) shutdownStorage() error {
	if c.isRunning() {
		panic("Program logic error: trying to shutdownStorage() on running controller")
	}
	if c.isMemOnly {
		panic("Program logic error: trying to shutdownStorage() in MemOnly mode")
	}

	log.Infof("Persisting storage...")
	err := c.dumpStorage()
	if err != nil {
		return err
	}

	oldWalFilename := c.walFile.Name()
	c.walFile.Close()
	os.Remove(oldWalFilename)

	return nil
}

func (c *Controller) startNewWal() (oldWalFilename string, err error) {
	c.walMutex.Lock()
	defer c.walMutex.Unlock()

	c.messageId++
	filename := c.walFileName(c.messageId)

	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		err = fmt.Errorf("trying to write WAL to existing file: %s", filename)
		log.Error(err.Error())
		return "", err
	}

	file, err := os.Create(filename)
	if err != nil {
		err = fmt.Errorf("error creating WAL file %s: %s", filename, err.Error())
		log.Error(err.Error())
		return "", err
	}

	if c.walFile != nil {
		oldWalFilename = c.walFile.Name()
		c.walFile.Close()
	}

	c.walFile = file
	c.walEncoder = gob.NewEncoder(c.walFile)

	return oldWalFilename, nil
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
	c.isRunningMutex.Lock()
	defer c.isRunningMutex.Unlock()
	return c.isRunningFlag
}

func (c *Controller) walFileName(messageId interface{}) string {
	return path.Join(c.dataDir, fmt.Sprintf(walFileName, messageId))
}

func (c *Controller) storageFileName() string {
	return path.Join(c.dataDir, storageFileName)
}

func (c *Controller) processCommand(r *message.Request) *message.Response {
	//TODO: use go generate!
	//TODO: move Cmd strings to constants!
	switch r.Cmd {
	case "KEYS":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		result := c.core.Keys(arg0)

		return getResponseMultiStringPayload(result)
	case "GET":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.Get(arg0)
		if err != nil {
			return getResponseCommandError(r.Cmd, err)
		}

		return getResponseSinglePayload(result)
	case "SET":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		c.core.Set(arg0, r.Payload)

		return getResponseEmptyPayload()
	case "SETEX":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentInt(1)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		c.core.SetEx(arg0, arg1, r.Payload)

		return getResponseEmptyPayload()
	case "DEL":
		args, err := r.GetArgumentVariadicString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		result := c.core.Del(args)

		return getResponseIntPayload(result)
	case "DKEYS":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentString(1)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.DKeys(arg0, arg1)
		if err != nil {
			return getResponseCommandError(r.Cmd, err)
		}

		return getResponseMultiStringPayload(result)
	case "DGETALL":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.DGetAll(arg0)
		if err != nil {
			return getResponseCommandError(r.Cmd, err)
		}

		return getResponseMultiPayload(result)

	case "DGET":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentString(1)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.DGet(arg0, arg1)
		if err != nil {
			return getResponseCommandError(r.Cmd, err)
		}

		return getResponseSinglePayload(result)
	case "DSET":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentString(1)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		count, err := c.core.DSet(arg0, arg1, r.Payload)
		if err != nil {
			return getResponseCommandError(r.Cmd, err)
		}

		return getResponseIntPayload(count)
	case "DDEL":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}
		args, err := r.GetArgumentVariadicString(1)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		count, err := c.core.DDel(arg0, args)
		if err != nil {
			return getResponseCommandError(r.Cmd, err)
		}

		return getResponseIntPayload(count)
	case "LLEN":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		count, err := c.core.LLen(arg0)
		if err != nil {
			return getResponseCommandError(r.Cmd, err)
		}

		return getResponseIntPayload(count)
	case "LRANGE":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentInt(1)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}
		arg2, err := r.GetArgumentInt(2)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.LRange(arg0, arg1, arg2)
		if err != nil {
			return getResponseCommandError(r.Cmd, err)
		}

		return getResponseMultiPayload(result)
	case "LINDEX":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentInt(1)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.LIndex(arg0, arg1)
		if err != nil {
			return getResponseCommandError(r.Cmd, err)
		}

		return getResponseSinglePayload(result)
	case "LSET":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentInt(1)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		err = c.core.LSet(arg0, arg1, r.Payload)
		if err != nil {
			return getResponseCommandError(r.Cmd, err)
		}

		return getResponseEmptyPayload()
	case "LPUSH":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		count, err := c.core.LPush(arg0, r.MultiPayloads)
		if err != nil {
			return getResponseCommandError(r.Cmd, err)
		}

		return getResponseIntPayload(count)
	case "LPOP":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		result, err := c.core.LPop(arg0)
		if err != nil {
			return getResponseCommandError(r.Cmd, err)
		}

		return getResponseSinglePayload(result)
	case "TTL":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		ttl, err := c.core.Ttl(arg0)
		if err != nil {
			return getResponseCommandError(r.Cmd, err)
		}

		return getResponseIntPayload(ttl)
	case "EXPIRE":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}
		arg1, err := r.GetArgumentInt(1)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		result := c.core.Expire(arg0, arg1)

		return getResponseIntPayload(result)
	case "PERSIST":
		arg0, err := r.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(r.Cmd, err)
		}

		result := c.core.Persist(arg0)

		return getResponseIntPayload(result)
	default:
		return message.NewResponseSingle(
			message.StatusInvalidCommand,
			[]byte("Unknown command: "+r.Cmd),
		)
	}
}

// Correct TTL value for TTL-related requests due to ttl is time.Now() -related value
func fixWalRequestTtl(request *message.Request) error {
	//TODO: use go generate
	switch request.Cmd {
	case "SETEX", "EXPIRE":
		seconds, err := request.GetArgumentInt(1)
		if err != nil {
			return err
		}

		seconds -= int(time.Since(request.Time).Seconds())
		request.Args[1] = strconv.Itoa(seconds)
	}

	return nil
}

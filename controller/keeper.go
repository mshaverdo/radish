package controller

import (
	"encoding/gob"
	"fmt"
	"github.com/mshaverdo/assert"
	"github.com/mshaverdo/radish/core"
	"github.com/mshaverdo/radish/log"
	"github.com/mshaverdo/radish/message"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type SyncPolicy int

const (
	// SyncNever means newer do walFile.Sync()
	SyncNever SyncPolicy = iota

	// SyncNever means walFile.Sync() every second
	SyncSometimes

	// SyncNever means walFile.Sync() every message write
	SyncAlways
)

//TODO: storage.gob loading is TOO slow
//TODO: write WAL in separate thread if syncPolicy is never or sometimes
//TODO: писать в WAL в отдельном треде, чтобы не аффектить общий перфоманс, если policy Never или Sometimes
//TODO: пропускать ПОСЛЕДНЮЮ запись из WAL если она битая они могут получитсья в рещультате сбоя питания. Если после битой записи есть другие -- валиться с ошибкой.
//TODO: GOB marshall все равно пишет слишком долго!

// Avoid "Unused Constant" warning
var _ = SyncNever

const (
	walFileName     = "wal_%v.gob"
	storageFileName = "storage.gob"
)

type storageData struct {
	MessageId int64
	Engine    core.Engine
}

type Keeper struct {
	mergeWalInterval time.Duration
	syncPolicy       SyncPolicy
	dataDir          string
	core             Core

	processor *Processor

	mutex      sync.Mutex
	messageId  int64
	walFile    *os.File
	walEncoder *gob.Encoder
	lastSync   time.Time

	// wg to wait for service storage-updating goroutines (runSnapshotter, etc)
	serviceWg sync.WaitGroup
	stopChan  chan struct{}
}

func NewKeeper(core Core, dataDir string, policy SyncPolicy, mergeWalInterval time.Duration) *Keeper {
	return &Keeper{
		core:             core,
		dataDir:          dataDir,
		syncPolicy:       policy,
		mergeWalInterval: mergeWalInterval,
		processor:        NewProcessor(core),
		stopChan:         make(chan struct{}),
	}
}

// WriteToWal writes request to WAL
func (k *Keeper) WriteToWal(request *message.Request) error {
	k.mutex.Lock()
	defer k.mutex.Unlock()

	k.messageId++
	request.Id = k.messageId
	err := k.walEncoder.Encode(request)

	if k.syncPolicy == SyncAlways || k.syncPolicy == SyncSometimes && time.Since(k.lastSync) > 1*time.Second {
		k.walFile.Sync()
		k.lastSync = time.Now()
	}

	return err
}

// restoreStorageState restores k.core state from dataDir
func (k *Keeper) restoreStorageState() error {
	if err := k.loadStorage(); err != nil {
		return err
	}

	wals, err := k.getDataDirWals()
	if err != nil {
		return err
	}

	processedWals, err := k.processWals(wals)
	if err != nil {
		return err
	}
	// dump storage with merged WALs to disk
	if err := k.persistStorage(); err != nil {
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

func (k *Keeper) loadStorage() error {
	filename := path.Join(k.dataDir, storageFileName)
	file, err := os.Open(filename)
	if os.IsNotExist(err) {
		// no data file found, just skip
		return nil
	} else if err != nil {
		return fmt.Errorf("Controller.loadStorage(). Unable to open %s: %s", filename, err)
	}
	defer file.Close()

	log.Infof("Loading storage data from %s...", filename)

	data := storageData{}
	dec := gob.NewDecoder(file)
	if err := dec.Decode(&data); err != nil {
		return fmt.Errorf("Keeper.loadStorage(): Unable to decode stream: %s", err)
	}

	k.core.SetEngine(data.Engine)
	k.messageId = data.MessageId

	if err != nil {
		return err
	}

	return nil
}

func (k *Keeper) getDataDirWals() (wals []string, err error) {
	wals, err = filepath.Glob(k.walFileName("*"))
	if err != nil {
		return nil, fmt.Errorf("Keeper.getDataDirWals(): %s", err)
	}

	return wals, nil
}

func (k *Keeper) processWals(wals []string) (processedWals []string, err error) {
	var messageIds []int
	for _, v := range wals {
		id := 0
		fmt.Sscanf(v, k.walFileName("%d"), &id)
		if id > 0 {
			messageIds = append(messageIds, id)
		}
	}

	sort.Ints(messageIds)

	// process all WALs from earliest to latest
	for _, messageId := range messageIds {
		filename := k.walFileName(messageId)
		if err := k.processWal(filename); err != nil {
			return nil, err
		}
		processedWals = append(processedWals, filename)
	}

	return processedWals, nil
}

func (k *Keeper) processWal(filename string) error {
	log.Infof("processing WAL %s...", filename)

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	dec := gob.NewDecoder(file)
	req := new(message.Request)
	processed := 0
	//TODO: add optional broken records passing
	for err := dec.Decode(req); err != io.EOF; err = dec.Decode(req) {
		if err != nil {
			return fmt.Errorf("Keeper.processWal(): can't process %s: %s", filename, err)
		}

		if req.Id <= k.messageId {
			// skip messages, that already in the storage
			continue
		}

		err = k.processor.FixRequestTtl(req)
		if err != nil {
			return fmt.Errorf("Keeper.processWal(): can't process %s: %s \nrequest: %s", filename, err, req)
		}

		resp := k.processor.Process(req)
		if resp.Status != message.StatusOk {
			// we got an error, but this request was successful. Something went wrong
			return fmt.Errorf("Keeper.processWal(): can't process %s: \nrequest: %s \nresponse: %s", filename, req, resp)
		}

		k.messageId = req.Id
		req = new(message.Request)
		processed++
	}

	log.Infof("%d requests processed if WAL %s", processed, filename)
	return nil
}

func (k *Keeper) persistStorage() error {
	//remove expired items to decrease dump size
	k.core.CollectExpired()

	file, err := ioutil.TempFile(filepath.Dir(k.storageFileName()), filepath.Base(k.storageFileName()))
	defer file.Close()

	if err != nil {
		return fmt.Errorf("Keeper.persistStorage(): %s", err)
	}

	data := storageData{
		Engine:    k.core.Engine(),
		MessageId: k.messageId,
	}

	// ensure exclusive access to engine during encoding
	data.Engine.FullLock()
	defer data.Engine.FullUnlock()

	enc := gob.NewEncoder(file)
	if err := enc.Encode(data); err != nil {
		return fmt.Errorf("Keeper.persistStorage(): %s", err)
	}

	err = os.Rename(file.Name(), k.storageFileName())
	if err != nil {
		return fmt.Errorf("Keeper.persistStorage(): %s", err)
	}

	return nil
}

// Shutdown shuts Keeper down and persists storage
func (k *Keeper) Shutdown() error {
	assert.True(k.isRunning(), "Tying to shut down not running Keeper")

	// wait for background updater finishes
	close(k.stopChan)
	k.serviceWg.Wait()

	log.Infof("Persisting storage...")
	err := k.persistStorage()
	if err != nil {
		return err
	}

	oldWalFilename := k.walFile.Name()
	k.walFile.Close()
	os.Remove(oldWalFilename)

	return nil
}

// Start restores storage state and starts new WAL
func (k *Keeper) Start() (err error) {
	assert.True(!k.isRunning(), "Tying to start already running Keeper")

	err = k.restoreStorageState()
	if err != nil {
		return err
	}

	_, _, err = k.startNewWal()

	k.serviceWg.Add(1)
	go k.runSnapshotUpdater()

	return err
}

// startNewWal closes current WAL file and starts new
func (k *Keeper) startNewWal() (oldWalFilename, newWalFilename string, err error) {
	k.mutex.Lock()
	defer k.mutex.Unlock()

	k.messageId++
	filename := k.walFileName(k.messageId)

	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		err = fmt.Errorf("Keeper.startNewWal(): trying to write WAL to existing file: %s", filename)
		log.Warning(err.Error())
		return "", "", err
	}

	file, err := os.Create(filename)
	if err != nil {
		err = fmt.Errorf("Keeper.startNewWal(): error creating WAL file %s: %s", filename, err.Error())
		log.Warning(err.Error())
		return "", "", err
	}

	if k.walFile != nil {
		oldWalFilename = k.walFile.Name()
		k.walFile.Close()
	}

	k.walFile = file
	k.walEncoder = gob.NewEncoder(k.walFile)

	return oldWalFilename, k.walFile.Name(), nil
}

func (k *Keeper) walFileName(messageId interface{}) string {
	return path.Join(k.dataDir, fmt.Sprintf(walFileName, messageId))
}

func (k *Keeper) storageFileName() string {
	return path.Join(k.dataDir, storageFileName)
}

func (k *Keeper) isRunning() bool {
	k.mutex.Lock()
	defer k.mutex.Unlock()
	return k.walFile != nil
}

func (k *Keeper) runSnapshotUpdater() {
	defer k.serviceWg.Done()

	tick := time.Tick(k.mergeWalInterval)
	for {
		select {
		case <-k.stopChan:
			return
		case <-tick:
			k.updateSnapshot()
		}
	}
}

// updateSnapshot starts new WAL and processes old WALs into existing storage snapshot
func (k *Keeper) updateSnapshot() error {
	log.Info("Updating a snapshot")
	_, newWal, err := k.startNewWal()
	if err != nil {
		return err
	}

	allWals, err := k.getDataDirWals()
	if err != nil {
		return err
	}

	// remove newWal from list
	var processingWals, processedWals []string
	for _, v := range allWals {
		if v != newWal {
			processingWals = append(processingWals, v)
		}
	}
	assert.True(len(allWals) != len(processingWals), "new WAL must be in datadir: "+k.dataDir+" "+newWal)

	snapshotKeeper := NewKeeper(
		core.New(core.NewHashEngine()),
		k.dataDir,
		SyncNever,
		0,
	)

	if err := snapshotKeeper.loadStorage(); err != nil {
		return err
	}

	processedWals, err = snapshotKeeper.processWals(processingWals)
	if err != nil {
		return err
	}

	// dump storage with merged WALs to disk
	if err := snapshotKeeper.persistStorage(); err != nil {
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

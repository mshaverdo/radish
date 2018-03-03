package main

import (
	"flag"
	"github.com/mshaverdo/assert"
	"github.com/mshaverdo/radish/controller"
	"github.com/mshaverdo/radish/log"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"
)

var assertionEnabled = "1"

func init() {
	assert.Enabled = (assertionEnabled == "1")
}

//TODO: сделать все тесты внешними pkgname_test
//TODO: in tests rename v -> tst
//TODO: добавить описание опции -http в README. Написать, что основной режим -- RESP, дополнительный HTTP
//TODO: в build.sh добавить пункт full-test c tag integration для полного тестирования
//TODO: оптимизировать перфоманс
//TODO: перейти на go 1.10
//TODO: !!!! при включенном WAL оно работает на GET на 20% быстрее!!! и SET тоже. Самое быстрое: persis включен, synPolicy 0
//TODO: погонять ПОЛНЫЙ набор бенчмаркков (LPUSH, HSET)
//TODO: посмотреть, как у редиса реализован TTL, почитать.
//TODO: REST в HTTP API
//TODO: пройтись по возвращаемым ошибкам и подобавлять вызывающую функу для тексовых ошибок
//TODO: написать в readme, что не поддерживается команда SET key val EX ttl, вместо нее надо использоавть SETEX -- поэтому в go-redis SET с указанием TTL работать не будет
//TODO: написать вижн по реализации шардинга
//TODO: возможно сделать экспериментальны форк
//TODO: !!!!!! deep.equal не работает!!!! он не может сравнить Item -- потому что сравнивает только экспортированные поля
//TODO: посмотреть, кто жрет столько оперативы при 10М записей 4+ гига
func main() {
	var (
		host, dataDir               string
		port                        int
		collectInterval             int
		mergeWalInterval            int
		syncPolicy                  int
		quiet, verbose, veryVerbose bool
		cpuProfile                  string
		useHttp                     bool
	)

	flag.StringVar(&host, "h", "", "The listening host.")
	flag.StringVar(&cpuProfile, "cpuprofile", "", "dump cpu profile into specified file")
	flag.IntVar(&port, "p", 6380, "The listening port.")
	flag.IntVar(&collectInterval, "e", 100, "Expired items collection interval in seconds")
	flag.IntVar(&mergeWalInterval, "m", 600, "Merge WAL into snapshot interval in seconds")
	flag.IntVar(&syncPolicy, "s", 1, "WAL sync policy: 0 - never, 1 - once per second, 2 - always")
	flag.StringVar(&dataDir, "d", "./", "Data dir")
	flag.BoolVar(&verbose, "v", false, "Enable verbose logging.")
	flag.BoolVar(&quiet, "q", false, "Quiet logging. Totally silent.")
	flag.BoolVar(&veryVerbose, "vv", false, "Enable very verbose logging.")
	flag.BoolVar(&useHttp, "http", false, "Use HTTP API")
	flag.Parse()

	//TODO: disable in production
	//TODO: разобраться, почему с включенным профилем обрабатывается на 10% больше RPS
	//TODO: написать в readme, что поддерживает pipeline, написать как запустить бенчмарк
	if cpuProfile != "" {
		if f, err := os.Create(cpuProfile); err == nil {
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		} else {
			log.Errorf("Can't create file %s: %s", cpuProfile, err)
		}
	}

	switch {
	case veryVerbose:
		log.SetLevel(log.DEBUG)
	case verbose:
		log.SetLevel(log.INFO)
	case quiet:
		log.SetLevel(-1)
	default:
		log.SetLevel(log.NOTICE)
	}

	c := controller.New(
		host,
		port,
		dataDir,
		controller.SyncPolicy(syncPolicy),
		time.Duration(collectInterval)*time.Second,
		time.Duration(mergeWalInterval)*time.Second,
		useHttp,
	)

	go handleSignals(c)

	if err := c.ListenAndServe(); err != nil {
		log.Critical(err.Error())
	}
}

func handleSignals(c *controller.Controller) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	for {
		s := <-sigs
		switch s {
		case syscall.SIGINT, syscall.SIGTERM:
			c.Shutdown()
			return
		}
	}
}

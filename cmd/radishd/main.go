package main

import (
	"flag"
	"github.com/mshaverdo/radish/controller"
	"github.com/mshaverdo/radish/log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	//TODO: сделать (найти) паект assert с ссответствующими функциями и использовать его вместо if... panic
	//TODO: посмотреть, чтобы возвращались более информативные ошибки. посмотреть best prectices. Возможно, просто подобавлять к ошибкам функцию, где они призошли
	//TODO: привести в порядок уровни логгинга (поменять местами в коде info и notice)

	var (
		host, dataDir               string
		port                        int
		collectInterval             int
		mergeWalInterval            int
		syncPolicy                  int
		quiet, verbose, veryVerbose bool
	)

	flag.StringVar(&host, "h", "", "The listening host.")
	flag.IntVar(&port, "p", 6380, "The listening port.")
	flag.IntVar(&collectInterval, "e", 100, "Expired items collection interval in seconds")
	flag.IntVar(&mergeWalInterval, "m", 600, "Merge WAL into snapshot interval in seconds")
	flag.IntVar(&syncPolicy, "s", 1, "WAL sync policy: 0 - never, 1 - once per second, 2 - always")
	flag.StringVar(&dataDir, "d", "./", "Data dir")
	flag.BoolVar(&verbose, "v", false, "Enable verbose logging.")
	flag.BoolVar(&quiet, "q", false, "Quiet logging. Totally silent.")
	flag.BoolVar(&veryVerbose, "vv", false, "Enable very verbose logging.")
	flag.Parse()

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

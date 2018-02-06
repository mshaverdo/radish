package main

import (
	"flag"
	"github.com/mshaverdo/radish/controller"
	"github.com/mshaverdo/radish/log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var (
		host                        string
		port                        int
		quiet, verbose, veryVerbose bool
	)

	flag.StringVar(&host, "h", "", "The listening host.")
	flag.IntVar(&port, "p", 6380, "The listening port.")
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
		log.SetLevel(log.CRITICAL)
	default:
		log.SetLevel(log.WARNING)
	}

	c := controller.New(host, port)

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

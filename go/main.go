package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

var (
	stopCh   = make(chan struct{})
	stopOnce sync.Once
)

func requestStop() {
	stopOnce.Do(func() { close(stopCh) })
	shutdownServer()
}

func stopped() bool {
	select {
	case <-stopCh:
		return true
	default:
		return false
	}
}

func installSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-signals
		requestStop()
	}()
}

func newFlagSet() *flag.FlagSet {
	return flag.NewFlagSet(os.Args[0], flag.ExitOnError)
}

func parseFlags(flags *flag.FlagSet, args []string) {
	_ = flags.Parse(args)
}

func main() {
	installSignals()
	args := os.Args[1:]
	command := "supervisor"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command = args[0]
		args = args[1:]
	}
	switch command {
	case "supervisor":
		runSupervisor(args)
	case "store":
		runStore(args)
	case "gateway":
		runGateway(args)
	case "order":
		runOrder(args)
	case "recommender":
		runRecommender(args)
	case "worker":
		runWorker(args)
	default:
		fmt.Fprintf(os.Stderr, "未知子命令 %s。可用：supervisor、store、gateway、order、recommender、worker\n", command)
		os.Exit(2)
	}
}

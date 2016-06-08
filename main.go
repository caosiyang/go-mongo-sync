package main

import (
	"log"
	"runtime"

	"go-mongo-sync/sync"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var config sync.Config
	if err := config.Load(); err != nil {
		log.Fatal(err)
	}
	syncer := sync.NewSynchronizer(config)
	if syncer == nil {
		log.Fatal("NewSyncronizer failed")
	}
	if err := syncer.Run(); err != nil {
		log.Fatal(err)
	}
}

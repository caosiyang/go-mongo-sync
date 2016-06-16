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
		log.Fatalln(err)
	}
	syncer := sync.NewSynchronizer(config)
	if syncer == nil {
		log.Fatalln("NewSyncronizer failed")
	}
	if err := syncer.Run(); err != nil {
		log.Fatalln(err)
	}
}

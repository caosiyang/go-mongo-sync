package main

import (
	"fmt"

	"go-mongo-sync/sync"
)

func main() {
	var config sync.Config
	if err := config.Load(); err != nil {
		fmt.Print(err)
		return
	}
	syncer := sync.NewSynchronizer(config)
	if syncer == nil {
		fmt.Println("NewSyncronizer failed")
		return
	}
	if err := syncer.Run(); err != nil {
		fmt.Println(err)
		return
	}
}

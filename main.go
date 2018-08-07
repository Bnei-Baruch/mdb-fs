package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Bnei-Baruch/mdb-fs/config"
	"github.com/Bnei-Baruch/mdb-fs/core"
)

func sync(cfg *config.Config) {
	// Initialize shutdown hook
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Start syncer
	log.Println("Starting FS sync")
	syncer := new(core.Syncer)
	go syncer.DoSync(cfg)

	// Blocking wait for signals
	go func() {
		sig := <-sigs
		log.Println()
		log.Printf("Signal: %s\n", sig)
		done <- true
	}()

	log.Println("Press Ctrl+C to exit")
	<-done

	log.Println("Stopping FS sync")
	syncer.Close()
}

func index(cfg *config.Config) {
	log.Println("Starting FS index")

	fs := core.NewSha1FS(cfg.RootDir)
	if err := fs.ScanReap(); err != nil {
		log.Fatalf("fs.ScanReap: %s", err.Error())
	}

	log.Println("FS index complete")
}

func main() {
	log.Println("mdb-fs start")

	var cfg = new(config.Config)
	cfg.Load()

	cmd := "sync"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	switch cmd {
	case "index":
		index(cfg)
	case "sync":
		sync(cfg)
	default:
		log.Printf("Unknown command: %s, default is sync\n", cmd)
		sync(cfg)
	}

	log.Println("mdb-fs end")
}

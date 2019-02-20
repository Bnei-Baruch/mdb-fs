package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Bnei-Baruch/mdb-fs/config"
	"github.com/Bnei-Baruch/mdb-fs/core"
	"github.com/Bnei-Baruch/mdb-fs/version"
)

func sync(cfg *config.Config) {
	// Initialize shutdown hook
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Start syncer
	log.Println("[INFO] Starting FS sync")
	syncer := new(core.Syncer)
	go syncer.DoSync(cfg)

	// Blocking wait for signals
	go func() {
		sig := <-sigs
		log.Printf("[INFO] Signal: %s\n", sig)
		done <- true
	}()

	log.Println("[INFO] Press Ctrl+C to exit")
	<-done

	log.Println("[INFO] Stopping FS sync")
	syncer.Close()
}

func index(cfg *config.Config) {
	log.Println("[INFO] Starting FS index")

	fs := core.NewSha1FS(cfg)
	if err := fs.ScanReap(); err != nil {
		log.Fatalf("fs.ScanReap: %s", err.Error())
	}

	log.Println("[INFO] FS index complete")
}

func main() {
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
	case "version":
		fmt.Printf("BB archive MDB FileSystem version %s\n", version.Version)
	default:
		log.Printf("[ERROR] Unknown command: %s, default is sync\n", cmd)
		sync(cfg)
	}
}

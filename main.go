package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Bnei-Baruch/mdb-fs/config"
	"github.com/Bnei-Baruch/mdb-fs/core"
)

func main() {
	log.Println("mdb-fs start")

	var cfg = new(config.Config)
	cfg.Load()

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

	log.Println("mdb-fs end")
}

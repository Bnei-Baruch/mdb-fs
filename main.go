package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/subosito/gotenv"

	"github.com/Bnei-Baruch/mdb-fs/common"
	"github.com/Bnei-Baruch/mdb-fs/core"
	"github.com/Bnei-Baruch/mdb-fs/importer"
	"github.com/Bnei-Baruch/mdb-fs/version"
)

func sync() {
	// Initialize shutdown hook
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Start syncer
	log.Println("[INFO] Starting FS sync")
	syncer := core.NewSyncer()
	go syncer.DoSync()

	// Blocking wait for signals
	go func() {
		sig := <-sigs
		log.Printf("[INFO] Signal: %s\n", sig)
		done <- true
	}()

	log.Println("[INFO] Press Ctrl+C to exit")
	<-done

	log.Println("[INFO] Stopping FS sync")
	syncer.(core.Closer).Close()
}

func index() {
	log.Println("[INFO] Starting FS index")

	fs := core.NewSha1FS()
	if err := fs.ScanReap(); err != nil {
		log.Fatalf("fs.ScanReap: %s", err.Error())
	}

	log.Println("[INFO] FS index complete")
}

func reshape(folder string, mode string) {
	log.Printf("[INFO] Starting folder reshape for %s\n", folder)

	fr := importer.NewFolderReshaper()
	if err := fr.Reshape(folder, mode); err != nil {
		log.Fatalf("fr.Reshape: %s", err.Error())
	}

	log.Println("[INFO] folder reshape complete")
}

func main() {
	cmd := "version"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	if cmd == "version" {
		fmt.Printf("BB archive MDB FileSystem version %s\n", version.Version)
		return
	}

	gotenv.Load()
	common.Init()

	switch cmd {
	case "index":
		index()
	case "sync":
		sync()
	case "reshape":
		reshape(os.Args[2], os.Args[3])
	default:
		log.Printf("[ERROR] Unknown command: %s\n", cmd)
	}
}

package main

import (
	"log"
	"time"

	"github.com/Bnei-Baruch/mdb-fs/config"
	"github.com/Bnei-Baruch/mdb-fs/index"
)

func main() {
	log.Println("mdb-fs start")
	var cfg = new(config.Config)
	cfg.Load()

	log.Printf("Config: %+v\n", cfg)

	syncer := new(index.Syncer)
	go syncer.DoSync(cfg)

	// TODO: register shutdown hook
	time.Sleep(24 * time.Hour)
	syncer.Close()
	log.Println("mdb-fs end")
}

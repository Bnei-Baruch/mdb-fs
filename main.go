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

	time.Sleep(20 * time.Second)

	syncer.Close()
	time.Sleep(5 * time.Second)
	log.Println("mdb-fs end")
}

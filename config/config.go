package config

import (
	"log"
	"time"

	"github.com/pelletier/go-toml"
)

type Config struct {
	RootDir  string
	MdbUrl   string
	Origins  []string
	Fetchers int
	SyncUpdateInterval time.Duration
	SuitcaseID   string
}

func (c *Config) Load() {
	config, err := toml.LoadFile("config.toml")
	if err != nil {
		log.Fatalf("Load config file: %s\n", err.Error())
	}

	c.RootDir = config.Get("root").(string)
	c.MdbUrl = config.Get("mdb").(string)

	o := config.Get("origins").([]interface{})
	c.Origins = make([]string, len(o))
	for i := range o {
		c.Origins[i] = o[i].(string)
	}

	c.Fetchers = int(config.GetDefault("fetchers", 1).(int64))

	c.SyncUpdateInterval, err = time.ParseDuration(config.Get("sync-update-interval").(string))
	if err != nil {
		log.Fatalf("sync-update-interval: %s\n", err.Error())
	}

	c.SuitcaseID = config.Get("suitcase-id").(string)
}

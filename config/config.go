package config

import (
	"time"

	"github.com/pelletier/go-toml"
	"github.com/pkg/errors"
)

type Config struct {
	RootDir            string
	MdbUrl             string
	Origins            []string
	MerkazAccess       bool
	Fetchers           int
	IndexWorkers       int
	SyncUpdateInterval time.Duration
	SuitcaseID         string
}

func (c *Config) Load() error {
	config, err := toml.LoadFile("config.toml")
	if err != nil {
		return errors.Wrap(err, "Load config file")
	}

	c.RootDir = config.Get("root").(string)
	c.MdbUrl = config.Get("mdb").(string)

	o := config.Get("origins").([]interface{})
	c.Origins = make([]string, len(o))
	for i := range o {
		c.Origins[i] = o[i].(string)
	}

	c.MerkazAccess = config.GetDefault("merkaz-access", false).(bool)
	c.Fetchers = int(config.GetDefault("fetchers", 1).(int64))
	c.IndexWorkers = int(config.GetDefault("index-workers", 1).(int64))

	c.SyncUpdateInterval, err = time.ParseDuration(config.Get("sync-update-interval").(string))
	if err != nil {
		return errors.Wrapf(err, "sync-update-interval: %s\n", err.Error())
	}

	c.SuitcaseID = config.Get("suitcase-id").(string)

	return nil
}

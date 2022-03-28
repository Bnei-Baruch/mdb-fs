package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type config struct {
	RootDir            string
	MDBUrl             string
	Origins            []string
	MerkazAccess       bool
	Fetchers           int
	IndexWorkers       int
	SyncUpdateInterval time.Duration
	SuitcaseID         string
}

func newConfig() *config {
	return &config{
		RootDir:            "",
		MDBUrl:             "postgres://user:password@localhost/mdb?sslmode=disable",
		Origins:            []string{},
		MerkazAccess:       false,
		Fetchers:           1,
		IndexWorkers:       1,
		SyncUpdateInterval: 60 * time.Second,
		SuitcaseID:         "",
	}
}

var Config *config

func Init() {
	Config = newConfig()

	if val := os.Getenv("ROOT_DIR"); val != "" {
		Config.RootDir = val
	}
	if val := os.Getenv("MDB_URL"); val != "" {
		Config.MDBUrl = val
	}
	if val := os.Getenv("ORIGINS"); val != "" {
		Config.Origins = strings.Split(val, ",")
	}
	if val := os.Getenv("MERKAZ_ACCESS"); val != "" {
		Config.MerkazAccess = val == "true"
	}
	if val := os.Getenv("FETCHERS"); val != "" {
		pVal, err := strconv.Atoi(val)
		if err != nil {
			panic(err)
		}
		Config.Fetchers = pVal
	}
	if val := os.Getenv("INDEX_WORKERS"); val != "" {
		pVal, err := strconv.Atoi(val)
		if err != nil {
			panic(err)
		}
		Config.IndexWorkers = pVal
	}
	if val := os.Getenv("SYNC_UPDATE_INTERVAL"); val != "" {
		pVal, err := time.ParseDuration(val)
		if err != nil {
			panic(err)
		}
		if pVal <= 0 {
			panic(fmt.Errorf("SYNC_UPDATE_INTERVAL must be positive, got %d", pVal))
		}
		Config.SyncUpdateInterval = pVal
	}
	if val := os.Getenv("SUITCASE_ID"); val != "" {
		Config.SuitcaseID = val
	}
}

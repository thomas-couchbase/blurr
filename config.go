package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"

	"github.com/pavel-paulau/blurr/databases"
	"github.com/pavel-paulau/blurr/workloads"
)

// High-level configuration structure
type Config struct {
	Database databases.Config
	Workload workloads.Config
}

// Read conifuration file (defined as CLI argument);
// also calculate per client target throughput
func ReadConfig() Config {
	path := flag.String("path", "samples/workload.conf",
		"Path to workload configuration")
	flag.Parse()

	b, err := ioutil.ReadFile(*path)
	if err != nil {
		log.Fatal(err)
	}
	var config Config
	err = json.Unmarshal(b, &config)
	if err != nil {
		log.Fatal(err)
	}
	config.Workload.TargetThroughput /= config.Workload.Workers
	return config
}

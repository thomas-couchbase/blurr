package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"

	"github.com/pavel-paulau/blurr/databases"
	"github.com/pavel-paulau/blurr/workloads"
)

type Config struct {
	Database databases.Config
	Workload workloads.Config
}

func ReadConfig() (config Config) {
	workload_path := flag.String("workload", "samples/workload.conf",
		"Path to workload configuration")
	flag.Parse()

	workload, err := ioutil.ReadFile(*workload_path)
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(workload, &config)
	if err != nil {
		log.Fatal(err)
	}

	config.Workload.Throughput /= config.Workload.Workers
	return
}

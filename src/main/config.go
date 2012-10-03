package main


import (
	"io/ioutil"
	"encoding/json"
	"flag"

	"databases"
	"workloads"
)


// High-level configuration structure
type Config struct {
	Competitor string
	Database databases.Config
	Workload workloads.Config
}


// Read conifuration file (defined as CLI argument);
// also calculate per client target throughput
func ReadConfig() Config {
	var path string
	flag.StringVar(&path, "path", "samples/mongodb.conf", "Path to workload configuration")
	flag.Parse()

	b, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	var config Config
	err = json.Unmarshal(b, &config)
	if err != nil {
		panic(err)
	}
	config.Workload.TargetThroughput /= config.Workload.Workers
	return config
}

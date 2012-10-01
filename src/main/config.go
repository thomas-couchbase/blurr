package main


import (
	"io/ioutil"
	"encoding/json"
	"flag"

	"databases"
	"workloads"
)


type Config struct {
	Competitor string
	Database databases.Config
	Workload workloads.Config
}


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
	return config
}

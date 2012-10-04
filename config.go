/*
 Copyright 2012 Pavel Paulau <Pavel.Paulau@gmail.com>
 All Rights Reserved

 Licensed under the Apache License, Version 2.0 (the "License")
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
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
	var path string
	flag.StringVar(&path, "path", "samples/workload.conf", "Path to workload configuration")
	flag.Parse()

	b, err := ioutil.ReadFile(path)
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

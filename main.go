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
	"log"
	"sync"
	"time"

	"github.com/pavel-paulau/blurr/databases"
	"github.com/pavel-paulau/blurr/workloads"
)

func main() {
	var database databases.Database
	var workload workloads.Workload

	// Read configuration file
	config := ReadConfig()

	// Create driver instance
	switch config.Database.Driver {
	case "MongoDB":
		database = &databases.MongoDB{}
	case "Couchbase":
		database = &databases.Couchbase{}
	default:
		log.Fatal("Unsupported competitor")
	}
	switch config.Workload.Type {
	case "DefaultWorkload":
		workload = &workloads.DefaultWorkload{}
	default:
		log.Fatal("Unsupported workload type")
	}

	// Initialize database and workload
	database.Init(config.Database)
	workload.Init(config.Workload)

	// Run concurrent workload
	wg := sync.WaitGroup{}
	wgStats := sync.WaitGroup{}
	state := workloads.State{}
	state.Records = config.Workload.Records

	// Initialize benchmark events
	state.Init()

	// Start concurrent goroutines
	state.Events["Started"] = time.Now()
	for worker := 0; worker < config.Workload.Workers; worker++ {
		wg.Add(1)
		go workload.RunWorkload(database, &state, &wg)
	}
	// Continuously report performance stats
	wgStats.Add(2)
	go state.ReportThroughput(config.Workload, &wgStats)
	go state.MeasureLatency(database, workload, config.Workload, &wgStats)

	wg.Wait()
	state.Events["Finished"] = time.Now()
	wgStats.Wait()

	// Close active connections (if any) and report final summary
	database.Shutdown()
	state.ReportSummary()
}

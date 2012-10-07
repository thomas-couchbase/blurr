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
	case "Default":
		workload = &workloads.Default{Config: config.Workload}
	case "HotSpot":
		workload = &workloads.HotSpot{Config: config.Workload,
			DefaultWorkload: &workloads.Default{Config: config.Workload}}
	default:
		log.Fatal("Unsupported workload type")
	}

	// Initialize database and workload
	database.Init(config.Database)

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

package main

import (
	"log"
	"sync"
	"time"

	"github.com/pavel-paulau/blurr/databases"
	"github.com/pavel-paulau/blurr/workloads"
)

var config Config
var database databases.Database
var workload workloads.Workload
var state workloads.State

func init() {
	config = ReadConfig()

	switch config.Database.Driver {
	case "MongoDB":
		database = &databases.MongoDB{}
	case "Couchbase":
		database = &databases.Couchbase{}
	default:
		log.Fatal("Unsupported driver")
	}

	switch config.Workload.Type {
	case "Default":
		workload = &workloads.Default{Config: config.Workload}
	case "HotSpot":
		workload = &workloads.HotSpot{Config: config.Workload,
			DefaultWorkload: &workloads.Default{Config: config.Workload}}
	default:
		log.Fatal("Unsupported workload")
	}

	database.Init(config.Database)

	state = workloads.State{}
	state.Records = config.Workload.Records
	state.Init()
}

func main() {
	wg := sync.WaitGroup{}
	wgStats := sync.WaitGroup{}

	state.Events["Started"] = time.Now()
	for worker := 0; worker < config.Workload.Workers; worker++ {
		wg.Add(1)
		go workload.RunWorkload(database, &state, &wg)
	}

	wgStats.Add(2)
	go state.ReportThroughput(config.Workload, &wgStats)
	go state.MeasureLatency(database, workload, config.Workload, &wgStats)

	wg.Wait()
	state.Events["Finished"] = time.Now()
	wgStats.Wait()

	database.Shutdown()
	state.ReportSummary()
}

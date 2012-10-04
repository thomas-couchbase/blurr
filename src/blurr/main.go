package main


import (
	"sync"
	"time"

	"databases"
	"workloads"
)


func main() {
	var database databases.Database
	var workload workloads.Workload

	// Read configuration file
	config := ReadConfig()

	// Create driver instance
	switch config.Competitor {
	case "MongoDB":
		database = new(databases.MongoDB)
	default:
		panic("Unsupported competitor")
	}
	switch config.Workload.Type {
	case "DefaultWorkload":
		workload = new(workloads.DefaultWorkload)
	default:
		panic("Unsupported workload type")
	}

	// Initialize database and workload
	database.Init(config.Database)
	workload.Init(config.Workload)

	// Run concurrent workload
	wg := new(sync.WaitGroup)
	wgStats := new(sync.WaitGroup)
	state := new(workloads.State)
	state.Records = config.Workload.Records

	// Initialize benchmark events
	state.Init()

	// Start concurrent goroutines
	state.Events["Started"] = time.Now()
	for worker := 0; worker < config.Workload.Workers; worker++ {
		wg.Add(1)
		go workload.RunWorkload(database, state, wg)
	}
	// Continuously report performance stats
	wgStats.Add(2)
	go state.ReportThroughput(config.Workload, wgStats)
	go state.MeasureLatency(database, workload, config.Workload, wgStats)

	wg.Wait()
	state.Events["Finished"] = time.Now()
	wgStats.Wait()

	// Close active connections (if any) and report final summary
	database.Shutdown()
	state.ReportSummary()
}

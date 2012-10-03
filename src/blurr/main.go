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
	state.Errors = make(map[string]int)
	state.Events = make(map[string]time.Time)
	state.Events["Started"] = time.Now()

	// Start concurrent goroutines
	for worker := 0; worker < config.Workload.Workers; worker++ {
		wg.Add(1)
		go workload.RunWorkload(database, state, wg)
	}
	// Continuously report performance stats
	wgStats.Add(1)
	go state.ReportThroughput(config.Workload, wgStats)

	wg.Wait()
	state.Events["Finished"] = time.Now()
	wgStats.Wait()

	// Close active connections (if any) and report final summary
	database.Shutdown()
	state.ReportSummary()
}

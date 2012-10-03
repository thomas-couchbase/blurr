package main


import (
	"sync"
	"time"

	"databases"
	"workloads"
)


func main() {
	var database databases.Database

	// Read configuration file
	config := ReadConfig()

	// Create driver instance
	switch config.Competitor {
	case "MongoDB":
		mongo := new(databases.MongoDB)
		database = mongo
	default:
		panic("Unsupported competitor")
	}

	// Initialize database
	database.Init(config.Database)

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
		go workloads.RunWorkload(database, config.Workload, state, wg)
	}
	// Continuously report performance stats
	wgStats.Add(1)
	go ReportThroughput(config.Workload, state, wgStats)

	wg.Wait()
	state.Events["Finished"] = time.Now()
	wgStats.Wait()

	// Close active connections (if any) and report final summary
	database.Shutdown()
	ReportSummary(state)
}

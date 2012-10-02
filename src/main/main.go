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
	wg_stats := new(sync.WaitGroup)
	state := new(workloads.State)
	state.Records = config.Workload.Records
	state.Events = make(map[string]time.Time)
	state.Events["Started"] = time.Now()
	for worker := 0; worker < config.Workload.Workers; worker++ {
		wg.Add(1)
		go workloads.RunWorkload(database, config.Workload, state, wg)
	}
	// Measure performance
	wg_stats.Add(1)
	go report_throughput(config.Workload, state, wg_stats)

	wg.Wait()
	state.Events["Finished"] = time.Now()

	database.Shutdown()

	wg_stats.Wait()
	report_summary(state)
}

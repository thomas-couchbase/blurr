package main

import (
	"sync"

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
	state := new(workloads.State)
	state.Records = config.Workload.Records
	for worker := 0; worker < config.Workload.Workers; worker++ {
		wg.Add(1)
		go workloads.RunWorkload(database, config.Workload, state, wg)
	}

	wg.Wait()

	database.Shutdown()
}

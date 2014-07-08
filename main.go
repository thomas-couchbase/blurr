package main

import (
	"log"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/couchbaselabs/blurr/databases"
	"github.com/couchbaselabs/blurr/workloads"
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
	case "Cassandra":
		database = &databases.Cassandra{}
	case "Tuq":
		database = &databases.Tuq{}
	default:
		log.Fatal("Unsupported driver")
	}

	switch config.Workload.Type {
	case "Default":
		workload = &workloads.Default{
			Config: config.Workload,
		}
	case "HotSpot":
		workload = &workloads.HotSpot{
			Config:  config.Workload,
			Default: workloads.Default{Config: config.Workload},
		}
	case "N1QL":
		r := rand.New(rand.NewSource(0))
		zipf := rand.NewZipf(r, 1.4, 9.0, 1000)
		workload = &workloads.N1QL{
			Config:  config.Workload,
			Zipf:    *zipf,
			Default: workloads.Default{Config: config.Workload},
		}
	default:
		log.Fatal("Unsupported workload")
	}
	workload.SetImplementation(workload)

	database.Init(config.Database)

	state = workloads.State{}
	state.Records = config.Workload.Records
	state.Init()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	wg := sync.WaitGroup{}
	wgStats := sync.WaitGroup{}

	state.Events["Started"] = time.Now()
	for worker := 0; worker < config.Workload.Workers; worker++ {
		wg.Add(1)
		go workload.RunCRUDWorkload(database, &state, &wg)
	}

	for worker := 0; worker < config.Workload.QueryWorkers; worker++ {
		wg.Add(1)
		go workload.RunQueryWorkload(database, &state, &wg)
	}

	wgStats.Add(2)
	go state.ReportThroughput(config.Workload, &wgStats)
	go state.MeasureLatency(database, workload, config.Workload, &wgStats)

	if config.Workload.RunTime > 0 {
		time.Sleep(time.Duration(config.Workload.RunTime) * time.Second)
		log.Println("Shutting down workers")
	} else {
		wg.Wait()
		wgStats.Wait()
	}

	database.Shutdown()
	state.Events["Finished"] = time.Now()
	state.ReportSummary()
}

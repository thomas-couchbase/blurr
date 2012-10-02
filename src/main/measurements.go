package main


import (
	"fmt"
	"time"
	"sync"

	"workloads"
)


func report_throughput(config workloads.Config, state *workloads.State, wg *sync.WaitGroup) {
	var ops_done int64 = 0
	var samples int = 0
	for state.Operations < config.Operations {
		time.Sleep(10 * time.Second)
		throughput := (state.Operations - ops_done) / 10
		ops_done = state.Operations
		fmt.Printf("%v - %v seconds: %v ops/sec; total operations: %v\n",
			samples * 10, samples * 10 + 10,throughput, ops_done)
		samples ++
	}
	wg.Done()
}


func report_summary(state *workloads.State) {
	fmt.Printf("Total errors: %v\n", len(state.Errors))
	fmt.Printf("Time elapsed: %v\n", state.Events["Finished"].Sub(state.Events["Started"]))
}

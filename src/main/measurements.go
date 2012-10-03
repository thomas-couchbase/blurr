package main


import (
	"fmt"
	"time"
	"sync"

	"workloads"
)


// Report average throughput and overall progress every 10 seconds
// TODO: report latency stats
func report_throughput(config workloads.Config, state *workloads.State, wg *sync.WaitGroup) {
	var ops_done int64 = 0
	var samples int = 1
	for state.Operations < config.Operations {
		time.Sleep(10 * time.Second)
		throughput := (state.Operations - ops_done) / 10
		ops_done = state.Operations
		fmt.Printf("%6v seconds: %10v ops/sec; total operations: %v\n",
			samples * 10, throughput, ops_done)
		samples ++
	}
	wg.Done()
}


// Report final summary: errors and elapsed time
// TODO: report latency histogram
func report_summary(state *workloads.State) {
	fmt.Printf("Total errors: %v\n", len(state.Errors))  // TODO: report errors by type
	fmt.Printf("Time elapsed: %v\n", state.Events["Finished"].Sub(state.Events["Started"]))
}

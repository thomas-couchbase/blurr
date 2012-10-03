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
		fmt.Printf("%6v seconds: %10v ops/sec; total operations: %v; total errors: %v\n",
			samples * 10, throughput, ops_done, state.Errors["total"])
		samples ++
	}
	wg.Done()
}


// Report final summary: errors and elapsed time
// TODO: report latency histogram
func report_summary(state *workloads.State) {
	if len(state.Errors) > 0 {
		fmt.Println("Errors:")
		fmt.Printf("\tCreate : %v\n", state.Errors["c"])
		fmt.Printf("\tRead   : %v\n", state.Errors["r"])
		fmt.Printf("\tUpdate : %v\n", state.Errors["u"])
		fmt.Printf("\tDelete : %v\n", state.Errors["d"])
		fmt.Printf("\tQuery  : %v\n", state.Errors["q"])
		fmt.Printf("\tTotal  : %v\n", state.Errors["total"])
	}
	fmt.Printf("Time elapsed:\n\t%v\n", state.Events["Finished"].Sub(state.Events["Started"]))
}

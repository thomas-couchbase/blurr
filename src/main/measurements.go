package main


import (
	"fmt"
	"time"
	"sync"

	"workloads"
)


// Report average throughput and overall progress every 10 seconds
// TODO: report latency stats
func ReportThroughput(config workloads.Config, state *workloads.State, wg *sync.WaitGroup) {
	var opsDone int64 = 0
	var samples int = 1
	for state.Operations < config.Operations {
		time.Sleep(10 * time.Second)
		throughput := (state.Operations - opsDone) / 10
		opsDone = state.Operations
		fmt.Printf("%6v seconds: %10v ops/sec; total operations: %v; total errors: %v\n",
			samples * 10, throughput, opsDone, state.Errors["total"])
		samples ++
	}
	wg.Done()
}


// Report final summary: errors and elapsed time
// TODO: report latency histogram
func ReportSummary(state *workloads.State) {
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

package workloads


import (
	"fmt"
	"time"
	"sync"
)


// Type to store benchmark state
type State struct {
	Operations, Records int64    // operations done and total number of records in database
	Errors map[string]int        // total errors by operation type
	Events map[string]time.Time  // runtime events ("Started", "Finished", and etc.)
}


// Report average throughput and overall progress every 10 seconds
// TODO: report latency stats
func (state *State) ReportThroughput(config Config, wg *sync.WaitGroup) {
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
func (state *State) ReportSummary() {
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

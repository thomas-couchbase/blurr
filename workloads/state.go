package workloads

import (
	"fmt"
	"sync"
	"time"

	"github.com/patrick-higgins/summstat"

	"github.com/pavel-paulau/blurr/databases"
)

type State struct {
	Operations, Records int64
	Errors              map[string]int
	Events              map[string]time.Time
	Latency             map[string]*summstat.Stats
}

func (state *State) Init() {
	state.Errors = map[string]int{}
	state.Events = map[string]time.Time{}
	state.Latency = map[string]*summstat.Stats{
		"Create": summstat.NewStats(),
		"Read": summstat.NewStats(),
		"Update": summstat.NewStats(),
		"Delete": summstat.NewStats(),
	}
}

func (state *State) ReportThroughput(config Config, wg *sync.WaitGroup) {
	defer wg.Done()
	opsDone := int64(0)
	samples := 1
	fmt.Println("Benchmark started:")
	for state.Operations < config.Operations {
		time.Sleep(10 * time.Second)
		throughput := (state.Operations - opsDone) / 10
		opsDone = state.Operations
		fmt.Printf("%6v seconds: %10v ops/sec; total operations: %v; total errors: %v\n",
			samples*10, throughput, opsDone, state.Errors["total"])
		samples++
	}
}

func (state *State) MeasureLatency(database databases.Database,
	workload Workload, config Config, wg *sync.WaitGroup) {
	defer wg.Done()

	for state.Operations < config.Operations {
		if config.CreatePercentage > 0 {
			state.Operations++
			state.Records++
			key := workload.GenerateNewKey(state.Records)
			value := workload.GenerateValue(key, config.ValueSize)
			t0 := time.Now()
			database.Create(key, value)
			t1 := time.Now()
			state.Latency["Create"].AddSample(summstat.Sample(t1.Sub(t0)))
		}
		if config.ReadPercentage > 0 {
			state.Operations++
			key := workload.GenerateExistingKey(state.Records)
			t0 := time.Now()
			database.Read(key)
			t1 := time.Now()
			state.Latency["Read"].AddSample(summstat.Sample(t1.Sub(t0)))
		}
		if config.UpdatePercentage > 0 {
			state.Operations++
			key := workload.GenerateExistingKey(state.Records)
			value := workload.GenerateValue(key, config.ValueSize)
			t0 := time.Now()
			database.Update(key, value)
			t1 := time.Now()
			state.Latency["Update"].AddSample(summstat.Sample(t1.Sub(t0)))
		}
		if config.DeletePercentage > 0 {
			state.Operations++
			key := workload.GenerateKeyForRemoval()
			t0 := time.Now()
			database.Delete(key)
			t1 := time.Now()
			state.Latency["Delete"].AddSample(summstat.Sample(t1.Sub(t0)))
		}
		time.Sleep(time.Second)
	}
}

func (state *State) ReportSummary() {
	for _, op := range []string{"Create", "Read", "Update", "Delete"} {
		if state.Latency[op].Count() > 0 {
			fmt.Printf("%v latency:\n", op)
			for _, percentile := range []float64{0.8, 0.9, 0.95, 0.99} {
				value := time.Duration(state.Latency[op].Percentile(percentile))
				fmt.Printf("\t%vth percentile: %v\n", percentile*100, value)
			}
			mean := time.Duration(state.Latency[op].Mean())
			fmt.Printf("\tMean: %v\n", mean)
		}
	}
	if len(state.Errors) > 0 {
		fmt.Println("Errors:")
		fmt.Printf("\tCreate : %v\n", state.Errors["c"])
		fmt.Printf("\tRead   : %v\n", state.Errors["r"])
		fmt.Printf("\tUpdate : %v\n", state.Errors["u"])
		fmt.Printf("\tDelete : %v\n", state.Errors["d"])
		fmt.Printf("\tTotal  : %v\n", state.Errors["total"])
	}
	fmt.Printf("Time elapsed:\n\t%v\n",
		state.Events["Finished"].Sub(state.Events["Started"]))
}

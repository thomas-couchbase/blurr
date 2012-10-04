package workloads


import (
	"fmt"
	"time"
	"sync"
	"math"

	"databases"
	"summstat"
)


// Type to store benchmark state
type State struct {
	Operations, Records int64          // operations done and total number of records in database
	Errors map[string]int              // total errors by operation type
	Events map[string]time.Time        // runtime events ("Started", "Finished", and etc.)
	Latency map[string]*summstat.Stats // latency arrays per request type
}


func (state *State) Init() {
	state.Errors = make(map[string]int)
	state.Events = make(map[string]time.Time)
	state.Latency = make(map[string]*summstat.Stats)
	state.Latency["Create"] = summstat.NewStats()
	state.Latency["Read"] = summstat.NewStats()
	state.Latency["Update"] = summstat.NewStats()
	state.Latency["Delete"] = summstat.NewStats()
	state.Latency["Query"] = summstat.NewStats()
}


// Report average throughput and overall progress every 10 seconds
func (state *State) ReportThroughput(config Config, wg *sync.WaitGroup) {
	var opsDone int64 = 0
	var samples int = 1
	fmt.Println("Benchmark started:")
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


func (state *State) MeasureLatency(database databases.Database, workload Workload, config Config, wg *sync.WaitGroup) {
	for state.Operations < config.Operations {
		if config.CreatePercentage > 0 {
			state.Operations ++
			state.Records ++
			key := workload.GenerateNewKey(state.Records)
			value := workload.GenerateValue(key, config.IndexableFields, config.ValueSize)
			t0 := time.Now()
			database.Create(key, value)
			t1 := time.Now()
			state.Latency["Create"].AddSample(summstat.Sample(t1.Sub(t0).Seconds()))
		}
		if config.ReadPercentage > 0 {
			state.Operations ++
			key := workload.GenerateExistingKey(state.Records)
			t0 := time.Now()
			database.Read(key)
			t1 := time.Now()
			state.Latency["Read"].AddSample(summstat.Sample(t1.Sub(t0).Seconds()))
		}
		if config.UpdatePercentage > 0 {
			state.Operations ++
			key := workload.GenerateExistingKey(state.Records)
			value := workload.GenerateValue(key, config.IndexableFields, config.ValueSize)
			t0 := time.Now()
			database.Update(key, value)
			t1 := time.Now()
			state.Latency["Update"].AddSample(summstat.Sample(t1.Sub(t0).Seconds()))
		}
		if config.DeletePercentage > 0 {
			state.Operations ++
			key := workload.GenerateKeyForRemoval()
			t0 := time.Now()
			database.Delete(key)
			t1 := time.Now()
			state.Latency["Delete"].AddSample(summstat.Sample(t1.Sub(t0).Seconds()))
		}
		if config.QueryPercentage > 0 {
			state.Operations ++
			fieldName, fieldValue, limit := workload.GenerateQuery(config.IndexableFields, state.Records)
			t0 := time.Now()
			database.Query(fieldName, fieldValue, limit)
			t1 := time.Now()
			state.Latency["Query"].AddSample(summstat.Sample(t1.Sub(t0).Seconds()))
		}
		time.Sleep(1 * time.Second)
	}
	wg.Done()
}


// Report final summary: errors and elapsed time
func (state *State) ReportSummary() {
	for _, op := range []string{"Create", "Read", "Update", "Delete", "Query"} {
		if state.Latency[op].Count() > 0 {
			fmt.Printf("%v latency:\n", op)
			perc80th := time.Duration(float64(state.Latency[op].Percentile(0.8)) * math.Pow(10, 9)) * time.Nanosecond
			perc90th := time.Duration(float64(state.Latency[op].Percentile(0.9)) * math.Pow(10, 9)) * time.Nanosecond
			perc95th := time.Duration(float64(state.Latency[op].Percentile(0.95)) * math.Pow(10, 9)) * time.Nanosecond
			mean := time.Duration(float64(state.Latency[op].Mean()) * math.Pow(10, 9)) * time.Nanosecond
			fmt.Printf("\t80th percentile: %v\n", perc80th)
			fmt.Printf("\t90th percentile: %v\n", perc90th)
			fmt.Printf("\t95th percentile: %v\n", perc95th)
			fmt.Printf("\tMean: %v\n", mean)
		}
	}
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

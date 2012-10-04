/*
 Copyright 2012 Pavel Paulau <Pavel.Paulau@gmail.com>
 All Rights Reserved

 Licensed under the Apache License, Version 2.0 (the "License")
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
package workloads

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/patrick-higgins/summstat"

	"github.com/pavel-paulau/blurr/databases"
)

// Type to store benchmark state
type State struct {
	// operations done and total number of records in database
	Operations, Records int64
	// total errors by operation type
	Errors map[string]int
	// runtime events ("Started", "Finished", and etc.)
	Events map[string]time.Time
	// latency arrays per request type
	Latency map[string]*summstat.Stats
}

func (state *State) Init() {
	state.Errors = map[string]int{}
	state.Events = map[string]time.Time{}
	state.Latency = map[string]*summstat.Stats{}
	state.Latency["Create"] = summstat.NewStats()
	state.Latency["Read"] = summstat.NewStats()
	state.Latency["Update"] = summstat.NewStats()
	state.Latency["Delete"] = summstat.NewStats()
	state.Latency["Query"] = summstat.NewStats()
}

// Report average throughput and overall progress every 10 seconds
func (state *State) ReportThroughput(config Config, wg *sync.WaitGroup) {
	defer wg.Done()
	var opsDone int64 = 0
	var samples int = 1
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
			value := workload.GenerateValue(key, config.IndexableFields, config.ValueSize)
			t0 := time.Now()
			database.Create(key, value)
			t1 := time.Now()
			state.Latency["Create"].AddSample(summstat.Sample(t1.Sub(t0).Seconds()))
		}
		if config.ReadPercentage > 0 {
			state.Operations++
			key := workload.GenerateExistingKey(state.Records)
			t0 := time.Now()
			database.Read(key)
			t1 := time.Now()
			state.Latency["Read"].AddSample(summstat.Sample(t1.Sub(t0).Seconds()))
		}
		if config.UpdatePercentage > 0 {
			state.Operations++
			key := workload.GenerateExistingKey(state.Records)
			value := workload.GenerateValue(key, config.IndexableFields, config.ValueSize)
			t0 := time.Now()
			database.Update(key, value)
			t1 := time.Now()
			state.Latency["Update"].AddSample(summstat.Sample(t1.Sub(t0).Seconds()))
		}
		if config.DeletePercentage > 0 {
			state.Operations++
			key := workload.GenerateKeyForRemoval()
			t0 := time.Now()
			database.Delete(key)
			t1 := time.Now()
			state.Latency["Delete"].AddSample(summstat.Sample(t1.Sub(t0).Seconds()))
		}
		if config.QueryPercentage > 0 {
			state.Operations++
			fieldName, fieldValue, limit := workload.GenerateQuery(config.IndexableFields, state.Records)
			t0 := time.Now()
			database.Query(fieldName, fieldValue, limit)
			t1 := time.Now()
			state.Latency["Query"].AddSample(summstat.Sample(t1.Sub(t0).Seconds()))
		}
		time.Sleep(1 * time.Second)
	}
}

// Report final summary: errors and elapsed time
func (state *State) ReportSummary() {
	for _, op := range []string{"Create", "Read", "Update", "Delete", "Query"} {
		if state.Latency[op].Count() > 0 {
			fmt.Printf("%v latency:\n", op)
			perc80th := time.Duration(float64(state.Latency[op].Percentile(0.8))*math.Pow(10, 9)) * time.Nanosecond
			perc90th := time.Duration(float64(state.Latency[op].Percentile(0.9))*math.Pow(10, 9)) * time.Nanosecond
			perc95th := time.Duration(float64(state.Latency[op].Percentile(0.95))*math.Pow(10, 9)) * time.Nanosecond
			mean := time.Duration(float64(state.Latency[op].Mean())*math.Pow(10, 9)) * time.Nanosecond
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

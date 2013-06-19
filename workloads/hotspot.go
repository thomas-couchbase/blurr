// TODO: fix stupid code copy-paste
package workloads

import (
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/pavel-paulau/blurr/databases"
)

type HotSpot struct {
	Config          Config
	DeletedItems    int64
	DefaultWorkload *Default
}

// Generate new *unique* key
func (w *HotSpot) GenerateNewKey(currentRecords int64) string {
	return w.DefaultWorkload.GenerateNewKey(currentRecords)
}

// Generate hot or load key from current key space
func (w *HotSpot) GenerateExistingKey(currentRecords int64) string {
	var key string
	rand.Seed(time.Now().UnixNano())

	if rand.Intn(100) < w.Config.HotSpotAccessPercentage {
		total_records := currentRecords - w.DeletedItems

		randRecord := rand.Int63n(total_records * w.Config.HotDataPercentage / 100)
		randRecord += w.DeletedItems +
			total_records*(100-w.Config.HotDataPercentage)/100
		strRandRecord := strconv.FormatInt(randRecord, 10)
		key = Hash(strRandRecord)
	} else {
		key = w.DefaultWorkload.GenerateExistingKey(currentRecords)
	}
	return key
}

// Generate sequential key for removal
func (w *HotSpot) GenerateKeyForRemoval() string {
	return w.DefaultWorkload.GenerateKeyForRemoval()
}

// Generate value with deterministic indexable fields and arbitrary body
func (w *HotSpot) GenerateValue(key string, indexableFields,
	size int) map[string]interface{} {
	return w.DefaultWorkload.GenerateValue(key, indexableFields, size)
}

// Generate query on secondary index
func (w *HotSpot) GenerateQuery(indexableFields int,
	currentRecords int64) (string, string, int) {

	i := rand.Intn(indexableFields)
	fieldName := "field" + strconv.Itoa(i)
	fieldValue := fieldName + "-" + w.GenerateExistingKey(currentRecords)[i:i+10]
	limit := 10 + rand.Intn(10)
	return fieldName, fieldValue, limit
}

// Generate slice of shuffled characters (CRUD-Q shorthands)
func (w *HotSpot) PrepareBatch() []string {
	return w.DefaultWorkload.PrepareBatch()
}

// Sequentially send 100 requests
func (w *HotSpot) DoBatch(db databases.Database, state *State) {
	batch := w.PrepareBatch()

	for _, v := range batch {
		// Increase number of passed operarions *before* batch
		// execution in order to normally share key space with
		// other workers
		if state.Operations < w.Config.Operations {
			var err error
			state.Operations++
			switch v {
			case "c":
				state.Records++
				key := w.GenerateNewKey(state.Records)
				value := w.GenerateValue(key,
					w.Config.IndexableFields, w.Config.ValueSize)
				err = db.Create(key, value)
			case "r":
				key := w.GenerateExistingKey(state.Records)
				err = db.Read(key)
			case "u":
				key := w.GenerateExistingKey(state.Records)
				value := w.GenerateValue(key,
					w.Config.IndexableFields, w.Config.ValueSize)
				err = db.Update(key, value)
			case "d":
				key := w.GenerateKeyForRemoval()
				err = db.Delete(key)
			case "q":
				fieldName, fieldValue, limit := w.GenerateQuery(
					w.Config.IndexableFields, state.Records)
				err = db.Query(fieldName, fieldValue, limit)
			}
			if err != nil {
				state.Errors[v]++
				state.Errors["total"]++
			}
		}
	}
}

// Continuously run batches of operations
func (w *HotSpot) RunWorkload(database databases.Database,
	state *State, wg *sync.WaitGroup) {
	defer wg.Done()

	// Calculate target time for batch execution. +Inf if not defined
	targetBatchTimeF := float64(100) /
		float64(w.Config.TargetThroughput)

	for state.Operations < w.Config.Operations {
		// Send batch of request and measure execution time
		t0 := time.Now()
		w.DoBatch(database, state)
		t1 := time.Now()

		// Sleep if necessary
		if !math.IsInf(targetBatchTimeF, 0) {
			targetBatchTime := time.Duration(targetBatchTimeF * math.Pow10(9))
			actualBatchTime := t1.Sub(t0)
			sleepTime := (targetBatchTime - actualBatchTime)
			if sleepTime > 0 {
				time.Sleep(time.Duration(sleepTime))
			}
		}
	}
}

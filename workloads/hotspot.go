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

func (w *HotSpot) GenerateNewKey(currentRecords int64) string {
	return w.DefaultWorkload.GenerateNewKey(currentRecords)
}

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

func (w *HotSpot) GenerateKeyForRemoval() string {
	return w.DefaultWorkload.GenerateKeyForRemoval()
}

func (w *HotSpot) GenerateValue(key string, indexableFields,
	size int) map[string]interface{} {
	return w.DefaultWorkload.GenerateValue(key, indexableFields, size)
}

func (w *HotSpot) GenerateQuery(indexableFields int,
	currentRecords int64) (string, string, int) {

	i := rand.Intn(indexableFields)
	fieldName := "field" + strconv.Itoa(i)
	fieldValue := fieldName + "-" + w.GenerateExistingKey(currentRecords)[i:i+10]
	limit := 10 + rand.Intn(10)
	return fieldName, fieldValue, limit
}

func (w *HotSpot) PrepareBatch() []string {
	return w.DefaultWorkload.PrepareBatch()
}

func (w *HotSpot) DoBatch(db databases.Database, state *State) {
	batch := w.PrepareBatch()

	for _, v := range batch {
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

func (w *HotSpot) RunWorkload(database databases.Database,
	state *State, wg *sync.WaitGroup) {
	defer wg.Done()

	targetBatchTimeF := float64(100) /
		float64(w.Config.TargetThroughput)

	for state.Operations < w.Config.Operations {
		t0 := time.Now()
		w.DoBatch(database, state)
		t1 := time.Now()

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

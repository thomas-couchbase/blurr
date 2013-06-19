package workloads

import (
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/pavel-paulau/blurr/databases"
)

type Default struct {
	Config       Config
	DeletedItems int64
}

func (w *Default) GenerateNewKey(currentRecords int64) string {
	strCurrentRecords := strconv.FormatInt(currentRecords, 10)
	return Hash(strCurrentRecords)
}

func (w *Default) GenerateExistingKey(currentRecords int64) string {
	rand.Seed(time.Now().UnixNano())
	randRecord := w.DeletedItems + rand.Int63n(currentRecords-w.DeletedItems)
	strRandRecord := strconv.FormatInt(randRecord, 10)
	return Hash(strRandRecord)
}

func (w *Default) GenerateKeyForRemoval() string {
	keyForRemoval := strconv.FormatInt(w.DeletedItems+1, 10)
	w.DeletedItems++
	return Hash(keyForRemoval)
}

func (w *Default) GenerateValue(key string,
	indexableFields, size int) map[string]interface{} {
	if indexableFields >= 20 {
		log.Fatal("Too much fields! It must be less than 20")
	}

	value := map[string]interface{}{}
	for i := 0; i < indexableFields; i++ {
		fieldName := "field" + strconv.Itoa(i)
		value[fieldName] = fieldName + "-" + key[i:i+10]
	}

	fieldName := "field" + strconv.Itoa(indexableFields)
	expectedLength := size - len(fieldName+"-"+key[:10])*indexableFields
	value[fieldName] = RandString(key, expectedLength)
	return value
}

func (w *Default) GenerateQuery(indexableFields int,
	currentRecords int64) (string, string, int) {
	i := rand.Intn(indexableFields)
	fieldName := "field" + strconv.Itoa(i)
	fieldValue := fieldName + "-" + w.GenerateExistingKey(currentRecords)[i:i+10]
	limit := 10 + rand.Intn(10)
	return fieldName, fieldValue, limit
}

func (w *Default) PrepareBatch() []string {
	operations := make([]string, 0, 100)
	randOperations := make([]string, 100, 100)
	for i := 0; i < w.Config.CreatePercentage; i++ {
		operations = append(operations, "c")
	}
	for i := 0; i < w.Config.ReadPercentage; i++ {
		operations = append(operations, "r")
	}
	for i := 0; i < w.Config.UpdatePercentage; i++ {
		operations = append(operations, "u")
	}
	for i := 0; i < w.Config.DeletePercentage; i++ {
		operations = append(operations, "d")
	}
	for i := 0; i < w.Config.QueryPercentage; i++ {
		operations = append(operations, "q")
	}
	if len(operations) != 100 {
		log.Fatal("Wrong workload configuration: sum of percentages is not equal 100")
	}
	for i, randI := range rand.Perm(100) {
		randOperations[i] = operations[randI]
	}
	return randOperations
}

func (w *Default) Something() chan string {
	operations := w.PrepareBatch()

	ch := make(chan string, 100000)

	go func() {
		for {
			ch <- operations[rand.Intn(100)]
		}
	}()

	return ch
}

func (w *Default) DoBatch(db databases.Database, state *State) {
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

func (w *Default) RunWorkload(database databases.Database,
	state *State, wg *sync.WaitGroup) {
	defer wg.Done()

	targetBatchTimeF := float64(100) / float64(w.Config.TargetThroughput)

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

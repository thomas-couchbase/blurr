package workloads

import (
	"bytes"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/pavel-paulau/blurr/databases"
)

type DefaultWorkload struct {
	Config       Config
	DeletedItems int64
}

func (workload *DefaultWorkload) Init(config Config) {
	workload.Config = config
}

// Generate new *unique* key
func (workload *DefaultWorkload) GenerateNewKey(currentRecords int64) string {
	strCurrentRecords := strconv.FormatInt(currentRecords, 10)
	return Hash(strCurrentRecords)
}

// Generate random key from current key space
func (workload *DefaultWorkload) GenerateExistingKey(currentRecords int64) string {
	rand.Seed(time.Now().UnixNano())
	randRecord := workload.DeletedItems + rand.Int63n(currentRecords-workload.DeletedItems)
	strRandRecord := strconv.FormatInt(randRecord, 10)
	return Hash(strRandRecord)
}

// Generate sequential key for removal
func (workload *DefaultWorkload) GenerateKeyForRemoval() string {
	keyForRemoval := strconv.FormatInt(workload.DeletedItems+1, 10)
	workload.DeletedItems++
	return Hash(keyForRemoval)
}

// Generate value with deterministic indexable fields and arbitrary body
func (workload *DefaultWorkload) GenerateValue(key string,
	indexableFields, size int) map[string]interface{} {

	// Hex lengh is 32 characters, so only 22 indexable fields are allowed
	if indexableFields >= 20 {
		log.Fatal("Too much fields! It must be less than 20")
	}
	// Gererate indexable fields (shifting over key name)
	value := map[string]interface{}{}
	for i := 0; i < indexableFields; i++ {
		fieldName := "field" + strconv.Itoa(i)
		value[fieldName] = fieldName + "-" + key[i:i+10]
	}
	// Generate value body in order to meet value size specification
	fieldName := "field" + strconv.Itoa(indexableFields)
	buffer := bytes.Buffer{}
	bodyHash := Hash(key)
	iterations := (size - len(fieldName+"-"+key[:10])*indexableFields) / 32
	for i := 0; i < iterations; i++ {
		buffer.WriteString(bodyHash)
	}
	value[fieldName] = buffer.String()
	return value
}

func (workload *DefaultWorkload) GenerateQuery(indexableFields int,
	currentRecords int64) (string, string, int) {

	i := rand.Intn(indexableFields)
	fieldName := "field" + strconv.Itoa(i)
	fieldValue := fieldName + "-" + workload.GenerateExistingKey(currentRecords)[i:i+10]
	limit := 10 + rand.Intn(10)
	return fieldName, fieldValue, limit
}

// Generate slice of shuffled characters (CRUD-Q shorthands)
func (workload *DefaultWorkload) PrepareBatch() []string {
	operations := make([]string, 0, 100)
	randOperations := make([]string, 100, 100)
	for i := 0; i < workload.Config.CreatePercentage; i++ {
		operations = append(operations, "c")
	}
	for i := 0; i < workload.Config.ReadPercentage; i++ {
		operations = append(operations, "r")
	}
	for i := 0; i < workload.Config.UpdatePercentage; i++ {
		operations = append(operations, "u")
	}
	for i := 0; i < workload.Config.DeletePercentage; i++ {
		operations = append(operations, "d")
	}
	for i := 0; i < workload.Config.QueryPercentage; i++ {
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

func (w *DefaultWorkload) Something() chan string {
	operations := w.PrepareBatch()

	ch := make(chan string, 100000)

	go func() {
		for {
			ch <- operations[rand.Intn(100)]
		}
	}()

	return ch
}

// Sequentially send 100 requests
func (workload *DefaultWorkload) DoBatch(db databases.Database, state *State) {
	batch := workload.PrepareBatch()

	for _, v := range batch {
		// Increase number of passed operarions *before* batch
		// execution in order to normally share key space with
		// other workers
		if state.Operations < workload.Config.Operations {
			var err error
			state.Operations++
			switch v {
			case "c":
				state.Records++
				key := workload.GenerateNewKey(state.Records)
				value := workload.GenerateValue(key,
					workload.Config.IndexableFields, workload.Config.ValueSize)
				err = db.Create(key, value)
			case "r":
				key := workload.GenerateExistingKey(state.Records)
				err = db.Read(key)
			case "u":
				key := workload.GenerateExistingKey(state.Records)
				value := workload.GenerateValue(key,
					workload.Config.IndexableFields, workload.Config.ValueSize)
				err = db.Update(key, value)
			case "d":
				key := workload.GenerateKeyForRemoval()
				err = db.Delete(key)
			case "q":
				fieldName, fieldValue, limit := workload.GenerateQuery(
					workload.Config.IndexableFields, state.Records)
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
func (workload *DefaultWorkload) RunWorkload(database databases.Database,
	state *State, wg *sync.WaitGroup) {
	defer wg.Done()

	// Calculate target time for batch execution. +Inf if not defined
	targetBatchTimeF := float64(100) /
		float64(workload.Config.TargetThroughput)

	for state.Operations < workload.Config.Operations {
		// Send batch of request and measure execution time
		t0 := time.Now()
		workload.DoBatch(database, state)
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

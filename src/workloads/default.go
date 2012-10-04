package workloads


import (
	"sync"
	"math"
	"math/rand"
	"strconv"
	"bytes"
	"time"

	"databases"
)


type DefaultWorkload struct {
	Config Config
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
	randRecord := workload.DeletedItems + rand.Int63n(currentRecords - workload.DeletedItems)
	strRandRecord := strconv.FormatInt(randRecord, 10)
	return Hash(strRandRecord)
}


// Generate sequential key for removal
func (workload *DefaultWorkload) GenerateKeyForRemoval() string {
	keyForRemoval := strconv.FormatInt(workload.DeletedItems, 10)
	workload.DeletedItems++
	return Hash(keyForRemoval)
}


// Generate value with deterministic indexable fields and arbitrary body
func (workload *DefaultWorkload) GenerateValue(key string, indexableFields, size int) map[string]interface{} {
	// Hex lengh is 32 characters, so only 22 indexable fields are allowed
	if indexableFields >= 20 {
		panic("Too much fields! It must be less than 20")
	}
	// Gererate indexable fields (shifting over key name)
	value := make(map[string]interface{})
	for i := 0; i < indexableFields; i++ {
		fieldName := "field" + strconv.Itoa(i)
		value[fieldName] = fieldName + "-" +key[i:i + 10]
	}
	// Generate value body in order to meet value size specification
	fieldName := "field" + strconv.Itoa(indexableFields)
	var buffer bytes.Buffer
	var bodyHash string = Hash(key)
	iterations := (size - len(fieldName + "-" + key[:10]) * indexableFields) / 32
	for i := 0; i < iterations; i++ {
		buffer.WriteString(bodyHash)
	}
	value[fieldName] = buffer.String()
	return value
}


func (workload *DefaultWorkload) GenerateQuery(indexableFields int, currentRecords int64) (string, string, int) {
	i := rand.Intn(indexableFields)
	fieldName := "field" + strconv.Itoa(i)
	fieldValue := fieldName + "-" + workload.GenerateExistingKey(currentRecords)[i:i + 10]
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
		panic("Wrong workload configuration: sum of percentages is not equal 100")
	}
	for i, randI := range rand.Perm(100) {
		randOperations[i] = operations[randI]
	}
	return randOperations
}


// Sequentially send 100 requests
func (workload *DefaultWorkload) DoBatch(db databases.Database, state *State) {
	var key string
	var value map[string]interface{}
	var status error
	var batch = workload.PrepareBatch()

	for _, v := range batch {
		// Increase number of passed operarions *before* batch execution in order to normally share key space with
		// other workers
		if state.Operations < workload.Config.Operations {
			state.Operations ++

			switch v {
			case "c":
				state.Records ++
				key = workload.GenerateNewKey(state.Records)
				value = workload.GenerateValue(key, workload.Config.IndexableFields, workload.Config.ValueSize)
				status = db.Create(key, value)
			case "r":
				key = workload.GenerateExistingKey(state.Records)
				status = db.Read(key)
			case "u":
				key = workload.GenerateExistingKey(state.Records)
				value = workload.GenerateValue(key, workload.Config.IndexableFields, workload.Config.ValueSize)
				status = db.Update(key, value)
			case "d":
				key = workload.GenerateKeyForRemoval()
				status = db.Delete(key)
			case "q":
				fieldName, fieldValue, limit := workload.GenerateQuery(workload.Config.IndexableFields, state.Records)
				status = db.Query(fieldName, fieldValue, limit)
			}
			if status != nil {
				state.Errors[v] ++
				state.Errors["total"] ++
			}
		}
	}
}


// Continuously run batches of operations
func (workload *DefaultWorkload) RunWorkload(database databases.Database, state *State, wg *sync.WaitGroup) {
	// Calculate target time for batch execution. +Inf if not defined
	targetBatchTime := float64(100) / float64(workload.Config.TargetThroughput)

	for state.Operations < workload.Config.Operations {
		// Send batch of request and measure execution time
		t0 := time.Now()
		workload.DoBatch(database, state)
		t1 := time.Now()

		// Sleep if necessary
		if !math.IsInf(targetBatchTime, 0) {
			actualBatchTime := t1.Sub(t0).Seconds()
			sleepTime := (targetBatchTime - actualBatchTime) * math.Pow(10, 9)
			if sleepTime > 0 {
				time.Sleep(time.Duration(sleepTime) * time.Nanosecond)
			}
		}
	}
	wg.Done()
}

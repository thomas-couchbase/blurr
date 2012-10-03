package workloads


import (
	"sync"
	"math"
	"math/rand"
	"strconv"
	"crypto/md5"
	"encoding/hex"
	"bytes"
	"time"

	"databases"
)


// General workload configuration
type Config struct {
	CreatePercentage int  // shorthand "c"
	ReadPercentage int    // shorthand "r"
	UpdatePercentage int  // shorthand "u"
	DeletePercentage int  // shorthand "d"
	QueryPercentage int   // shorthand "q"
	Records int64
	Operations int64
	ValueSize int
	IndexableFields int
	Workers int
	TargetThroughput int
}


// Generate hexdecimal representation of md5 hash for string
func Hash(inString string) string {
	h := md5.New()
	h.Write([]byte(inString))
	return hex.EncodeToString(h.Sum(nil))
}


// Generate new *unique* key
func GenerateNewKey(currentRecords int64) string {
	strCurrentRecords := strconv.FormatInt(currentRecords, 10)
	return Hash(strCurrentRecords)
}


// Generate random key from current key space
func GenerateExistingKey(currentRecords int64) string {
	rand.Seed(time.Now().UnixNano())
	randRecord := rand.Int63n(currentRecords)
	strRandRecord := strconv.FormatInt(randRecord, 10)
	return Hash(strRandRecord)
}


// Generate value with deterministic indexable fields and arbitrary body
func GenerateValue(key string, indexableFields, size int) map[string]interface{} {
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


func GenerateQuery(indexableFields int, currentRecords int64) (fieldName, fieldValue string, limit int) {
	i := rand.Intn(indexableFields)
	fieldName = "field" + strconv.Itoa(i)
	fieldValue = fieldName + "-" + GenerateExistingKey(currentRecords)[i:i + 10]
	limit = 10 + rand.Intn(10)
	return fieldName, fieldValue, limit
}


// Generate slice of shuffled characters (CRUD-Q shorthands)
func PrepareBatch(config Config) []string {
	operations := make([]string, 0, 100)
	randOperations := make([]string, 100, 100)
	for i := 0; i < config.CreatePercentage; i++ {
		operations = append(operations, "c")
	}
	for i := 0; i < config.ReadPercentage; i++ {
		operations = append(operations, "r")
	}
	for i := 0; i < config.UpdatePercentage; i++ {
		operations = append(operations, "u")
	}
	for i := 0; i < config.DeletePercentage; i++ {
		operations = append(operations, "d")
	}
	for i := 0; i < config.QueryPercentage; i++ {
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
func DoBatch(db databases.Database, config Config, state *State) {
	var key string
	var value map[string]interface{}
	var status error
	var batch = PrepareBatch(config)

	for _, v := range batch {
		switch v {
		case "c":
			state.Records ++
			key = GenerateNewKey(state.Records)
			value = GenerateValue(key, config.IndexableFields, config.ValueSize)
			status = db.Create(key, value)
		case "r":
			key = GenerateExistingKey(state.Records)
			status = db.Read(key)
		case "u":
			key = GenerateExistingKey(state.Records)
			value = GenerateValue(key, config.IndexableFields, config.ValueSize)
			status = db.Update(key, value)
		case "d":
			key = GenerateExistingKey(state.Records)
			status = db.Delete(key)
		case "q":
			fieldName, fieldValue, limit := GenerateQuery(config.IndexableFields, state.Records)
			status = db.Query(fieldName, fieldValue, limit)
		}
		if status != nil {
			state.Errors[v] ++
			state.Errors["total"] ++
		}
	}
}


// Continuously run batches of operations
func RunWorkload(database databases.Database, config Config, state *State, wg *sync.WaitGroup) {
	// Calculate target time for batch execution. +Inf if not defined
	targetBatchTime := float64(100) / float64(config.TargetThroughput)
	for state.Operations < config.Operations {
		// Increase number of passed operarions *before* batch execution in order to normally share key space with
		// other workers
		state.Operations += 100

		// Send batch of request and measure execution time
		t0 := time.Now()
		DoBatch(database, config, state)
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

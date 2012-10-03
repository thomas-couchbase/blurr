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


// Type to store benchmark state
type State struct {
	Operations, Records int64    // operations done and total number of records in database
	Errors []string              // total errors by operation type
	Events map[string]time.Time  // runtime events ("Started", "Finished", and etc.)
}


// Generate hexdecimal representation of md5 hash for string
func hash(in_string string) string {
	h := md5.New()
	h.Write([]byte(in_string))
	return hex.EncodeToString(h.Sum(nil))
}


// Generate new *unique* key
func GenerateNewKey(current_records int64) string {
	str_current_records := strconv.FormatInt(current_records, 10)
	return hash(str_current_records)
}


// Generate random key from current key space
func GenerateExistingKey(current_records int64) string {
	rand.Seed(time.Now().UnixNano())
	rand_record := rand.Int63n(current_records)
	str_rand_record := strconv.FormatInt(rand_record, 10)
	return hash(str_rand_record)
}


// Generate value with deterministic indexable fields and arbitrary body
func GenerateValue(key string, indexable_fields, size int) map[string]interface{} {
	// Hex lengh is 32 characters, so only 22 indexable fields are allowed
	if indexable_fields >= 20 {
		panic("Too much fields! It must be less than 20")
	}
	// Gererate indexable fields (shifting over key name)
	map_value := make(map[string]interface{})
	for i := 0; i < indexable_fields; i++ {
		fieldName := "field" + strconv.Itoa(i)
		map_value[fieldName] = fieldName + "-" +key[i:i + 10]
	}
	// Generate value body in order to meet value size specification
	fieldName := "field" + strconv.Itoa(indexable_fields)
	var buffer bytes.Buffer
	var body_hash string = hash(key)
	iterations := (size - len(fieldName + "-" + key[:10]) * indexable_fields) / 32
	for i := 0; i < iterations; i++ {
		buffer.WriteString(body_hash)
	}
	map_value[fieldName] = buffer.String()
	return map_value
}


// Generate slice of shuffled characters (CRUD-Q shorthands)
func PrepareBatch(config Config) []string {
	operations := make([]string, 0, 100)
	rand_operations := make([]string, 100, 100)
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
	for i, rand_i := range rand.Perm(100) {
		rand_operations[i] = operations[rand_i]
	}
	return rand_operations
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
		}
		if status != nil {
			state.Errors = append(state.Errors, v)
		}
	}
}


// Continuously run batches of operations
func RunWorkload(database databases.Database, config Config, state *State, wg *sync.WaitGroup) {
	// Calculate target time for batch execution. +Inf if not defined
	target_batch_time := float64(100) / float64(config.TargetThroughput)
	for state.Operations < config.Operations {
		// Increase number of passed operarions *before* batch execution in order to normally share key space with
		// other workers
		state.Operations += 100

		// Send batch of request and measure execution time
		t0 := time.Now()
		DoBatch(database, config, state)
		t1 := time.Now()

		// Sleep if necessary
		if !math.IsInf(target_batch_time, 0) {
			actual_batch_time := t1.Sub(t0).Seconds()
			sleep_time := (target_batch_time - actual_batch_time) * math.Pow(10, 9)
			if sleep_time > 0 {
				time.Sleep(time.Duration(sleep_time) * time.Nanosecond)
			}
		}
	}
	wg.Done()
}

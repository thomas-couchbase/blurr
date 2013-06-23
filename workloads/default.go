package workloads

import (
	"crypto/md5"
	"encoding/hex"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/pavel-paulau/blurr/databases"
)

const BatchSize int = 100

type Default struct {
	Config       Config
	DeletedItems int64
	i            Workload
}

func Hash(inString string) string {
	h := md5.New()
	h.Write([]byte(inString))
	return hex.EncodeToString(h.Sum(nil))
}

func RandString(key string, expectedLength int) string {
	var randString string
	if expectedLength > 64 {
		baseString := RandString(key, expectedLength/2)
		randString = baseString + baseString
	} else {
		randString = (Hash(key) + Hash(key[:len(key)-1]))[:expectedLength]
	}
	return randString
}

func (w *Default) SetImplementation(i Workload) {
	w.i = i
}

func (w *Default) GenerateNewKey(currentRecords int64) string {
	strCurrentRecords := strconv.FormatInt(currentRecords, 10)
	return Hash(strCurrentRecords)
}

func (w *Default) GenerateExistingKey(currentRecords int64) string {
	randRecord := 1 + rand.Int63n(currentRecords-w.DeletedItems)
	randRecord += w.DeletedItems
	strRandRecord := strconv.FormatInt(randRecord, 10)
	return Hash(strRandRecord)
}

func (w *Default) GenerateKeyForRemoval() string {
	w.DeletedItems++
	keyForRemoval := strconv.FormatInt(w.DeletedItems, 10)
	return Hash(keyForRemoval)
}

func (w *Default) GenerateValue(key string, size int) map[string]interface{} {
	return map[string]interface{}{
		key: RandString(key, size),
	}
}

func (w *Default) PrepareBatch() []string {
	operations := make([]string, 0, BatchSize)
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
	if len(operations) != BatchSize {
		log.Fatal("Wrong workload configuration: sum of percentages is not equal 100")
	}
	return operations
}

func (w *Default) PrepareSeq(size int64) chan string {
	operations := w.PrepareBatch()
	seq := make(chan string, BatchSize)
	go func() {
		for i := int64(0); i < size; i += int64(BatchSize) {
			for _, randI := range rand.Perm(BatchSize) {
				seq <- operations[randI]
			}
		}
	}()
	return seq
}

func (w *Default) DoBatch(db databases.Database, state *State, seq chan string) {
	for i := 0; i < BatchSize; i++ {
		op := <-seq
		if state.Operations < w.Config.Operations {
			var err error
			state.Operations++
			switch op {
			case "c":
				state.Records++
				key := w.GenerateNewKey(state.Records)
				value := w.GenerateValue(key, w.Config.ValueSize)
				err = db.Create(key, value)
			case "r":
				key := w.i.GenerateExistingKey(state.Records)
				err = db.Read(key)
			case "u":
				key := w.i.GenerateExistingKey(state.Records)
				value := w.GenerateValue(key, w.Config.ValueSize)
				err = db.Update(key, value)
			case "d":
				key := w.GenerateKeyForRemoval()
				err = db.Delete(key)
			}
			if err != nil {
				state.Errors[op]++
				state.Errors["total"]++
			}
		}
	}
}

func (w *Default) RunWorkload(database databases.Database,
	state *State, wg *sync.WaitGroup) {
	defer wg.Done()

	rand.Seed(time.Now().UnixNano())
	seq := w.PrepareSeq(w.Config.Operations)

	targetBatchTimeF := float64(BatchSize) / float64(w.Config.TargetThroughput)

	for state.Operations < w.Config.Operations {
		t0 := time.Now()
		w.i.DoBatch(database, state, seq)
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

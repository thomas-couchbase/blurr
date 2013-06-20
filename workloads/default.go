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
	randRecord := w.DeletedItems + rand.Int63n(currentRecords-w.DeletedItems)
	strRandRecord := strconv.FormatInt(randRecord, 10)
	return Hash(strRandRecord)
}

func (w *Default) GenerateKeyForRemoval() string {
	keyForRemoval := strconv.FormatInt(w.DeletedItems+1, 10)
	w.DeletedItems++
	return Hash(keyForRemoval)
}

func (w *Default) GenerateValue(key string, size int) map[string]interface{} {
	return map[string]interface{}{
		key: RandString(key, size),
	}
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
	if len(operations) != 100 {
		log.Fatal("Wrong workload configuration: sum of percentages is not equal 100")
	}
	for i, randI := range rand.Perm(100) {
		randOperations[i] = operations[randI]
	}
	return randOperations
}

func (w *Default) DoBatch(db databases.Database, state *State) {
	rand.Seed(time.Now().UnixNano())

	for _, v := range w.PrepareBatch() {
		if state.Operations < w.Config.Operations {
			var err error
			state.Operations++
			switch v {
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
		w.i.DoBatch(database, state)
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

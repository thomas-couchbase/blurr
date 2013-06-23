package workloads

import (
	"math/rand"
	"strconv"
)

type HotSpot struct {
	Config       Config
	DeletedItems int64
	Default
}

func (w *HotSpot) GenerateExistingKey(currentRecords int64) string {
	randRecord := w.DeletedItems
	total_records := currentRecords - w.DeletedItems
	if rand.Intn(100) < w.Config.HotSpotAccessPercentage {
		randRecord += 1 + rand.Int63n(total_records*w.Config.HotDataPercentage/100)
		randRecord += total_records * (100 - w.Config.HotDataPercentage) / 100
	} else {
		randRecord += 1 + rand.Int63n(total_records)
	}
	strRandRecord := strconv.FormatInt(randRecord, 10)
	return Hash(strRandRecord)
}

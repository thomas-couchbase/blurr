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
	var randRecord int64
	total_records := currentRecords - w.DeletedItems
	hot_records := total_records * w.Config.HotDataPercentage / 100
	cold_records := total_records - hot_records
	if rand.Intn(100) < w.Config.HotSpotAccessPercentage {
		randRecord = 1 + w.DeletedItems + cold_records + rand.Int63n(hot_records)
	} else {
		randRecord = 1 + w.DeletedItems + rand.Int63n(cold_records)
	}
	strRandRecord := strconv.FormatInt(randRecord, 10)
	return Hash(strRandRecord)
}

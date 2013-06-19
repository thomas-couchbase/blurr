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
	var strRandRecord string
	total_records := currentRecords - w.DeletedItems

	if rand.Intn(100) < w.Config.HotSpotAccessPercentage {
		randRecord := rand.Int63n(total_records * w.Config.HotDataPercentage / 100)
		randRecord += w.DeletedItems + total_records*(100-w.Config.HotDataPercentage)/100
		strRandRecord = strconv.FormatInt(randRecord, 10)
	} else {
		randRecord := w.DeletedItems + rand.Int63n(total_records)
		strRandRecord = strconv.FormatInt(randRecord, 10)
	}
	return Hash(strRandRecord)
}

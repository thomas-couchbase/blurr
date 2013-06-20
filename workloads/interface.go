package workloads

import (
	"sync"

	"github.com/pavel-paulau/blurr/databases"
)

type Config struct {
	Type                    string
	CreatePercentage        int
	ReadPercentage          int
	UpdatePercentage        int
	DeletePercentage        int
	Records                 int64
	Operations              int64
	ValueSize               int
	Workers                 int
	TargetThroughput        int
	HotDataPercentage       int64
	HotSpotAccessPercentage int
}

type Workload interface {
	SetImplementation(i Workload)

	GenerateNewKey(currentRecords int64) string

	GenerateExistingKey(currentRecords int64) string

	GenerateKeyForRemoval() string

	GenerateValue(key string, size int) map[string]interface{}

	PrepareBatch() []string

	DoBatch(database databases.Database, state *State)

	RunWorkload(database databases.Database, state *State, wg *sync.WaitGroup)
}

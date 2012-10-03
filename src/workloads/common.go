package workloads


import (
	"crypto/md5"
	"encoding/hex"
	"sync"

	"databases"
)


// General workload configuration
type Config struct {
	Type string
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


type Workload interface {
	Init(config Config)

	RunWorkload(database databases.Database, state *State, wg *sync.WaitGroup)
}


func Hash(inString string) string {
	h := md5.New()
	h.Write([]byte(inString))
	return hex.EncodeToString(h.Sum(nil))
}

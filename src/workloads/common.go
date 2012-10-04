/*
 Copyright 2012 Pavel Paulau <Pavel.Paulau@gmail.com>
 All Rights Reserved

 Licensed under the Apache License, Version 2.0 (the "License")
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
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

	GenerateNewKey(currentRecords int64) string

	GenerateExistingKey(currentRecords int64) string

	GenerateKeyForRemoval() string

	GenerateValue(key string, indexableFields, size int) map[string]interface{}

	GenerateQuery(indexableFields int, currentRecords int64) (fieldName, fieldValue string, limit int)

	RunWorkload(database databases.Database, state *State, wg *sync.WaitGroup)
}


func Hash(inString string) string {
	h := md5.New()
	h.Write([]byte(inString))
	return hex.EncodeToString(h.Sum(nil))
}

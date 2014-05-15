package databases

import (
	"log"
	"strings"

	"github.com/couchbaselabs/go-couchbase"
)

type Couchbase struct {
	Bucket *couchbase.Bucket
}

func (cb *Couchbase) Init(config Config) {
	address := strings.Replace(config.Addresses[0], "8093", "8091", -1)
	bucket, err := couchbase.GetBucket(address, config.Name, config.Table)
	if err != nil {
		log.Fatal(err)
	}
	cb.Bucket = bucket
}

func (cb *Couchbase) Shutdown() {
	cb.Bucket.Close()
}

func (cb *Couchbase) Create(key string, value map[string]interface{}) error {
	err := cb.Bucket.Set(key, 0, value)
	return err
}

func (cb *Couchbase) Read(key string) error {
	result := map[string]interface{}{}
	err := cb.Bucket.Get(key, &result)
	return err
}

func (cb *Couchbase) Update(key string, value map[string]interface{}) error {
	err := cb.Bucket.Set(key, 0, value)
	return err
}

func (cb *Couchbase) Delete(key string) error {
	err := cb.Bucket.Delete(key)
	return err
}

var DDOC_NAME = "ddoc"

func (cb *Couchbase) Query(key string, args []interface{}) error {
	index := args[0].(string)
	params := map[string]interface{}{"limit": 20}

	switch index {
	case "name_and_street_by_city":
		params["key"] = args[1]
	case "name_and_email_by_county":
		params["key"] = args[1]
	case "achievements_by_realm":
		params["key"] = args[1]
	case "name_by_coins":
		params["startkey"] = args[1].(float64) * 0.5
		params["endkey"] = args[1]
	case "email_by_achievement_and_category":
		params["startkey"] = []interface{}{0, args[2]}
		params["endkey"] = []interface{}{args[1].([]int16)[0], args[2]}
	case "street_by_year_and_coins":
		params["startkey"] = []interface{}{args[1], args[2]}
		params["endkey"] = []interface{}{args[1], 655.35}
	case "coins_stats_by_state_and_year":
		params["key"] = []interface{}{args[1], args[2]}
		params["group"] = true
	case "coins_stats_by_gmtime_and_year":
		params["key"] = []interface{}{args[1], args[2]}
		params["group_level"] = 2
	case "coins_stats_by_full_state_and_year":
		params["key"] = []interface{}{args[1], args[2]}
		params["group"] = true
	case "name_and_email_and_street_and_achievements_and_coins_by_city":
		params["key"] = args[1]
	case "street_and_name_and_email_and_achievement_and_coins_by_county":
		params["key"] = args[1]
	case "category_name_and_email_and_street_and_gmtime_and_year_by_country":
		params["key"] = args[1]
	case "calc_by_city":
		params["key"] = args[1]
	case "calc_by_county":
		params["key"] = args[1]
	case "calc_by_realm":
		params["key"] = args[1]
	case "body_by_city":
		params["key"] = args[1]
	case "body_by_realm":
		params["key"] = args[1]
	case "body_by_country":
		params["key"] = args[1]
	}
	_, err := cb.Bucket.View(DDOC_NAME, index, params)
	return err
}

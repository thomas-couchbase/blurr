package databases

import (
	"log"
	"time"

	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type MongoDB struct {
	Session        *mgo.Session
	DBName         string
	CollectionName string
}

func (mongo *MongoDB) Init(config Config) {
	dialInfo := &mgo.DialInfo{
		Addrs:   config.Addresses,
		Timeout: 10 * time.Minute,
	}

	var err error
	mongo.Session, err = mgo.DialWithInfo(dialInfo)
	if err != nil {
		log.Fatal(err)
	}
	mongo.Session.SetMode(mgo.Monotonic, true)
	mongo.DBName = config.Name
	mongo.CollectionName = config.Table
}

func (mongo *MongoDB) Shutdown() {
	mongo.Session.Close()
}

func (mongo *MongoDB) Create(key string, value map[string]interface{}) error {
	session := mongo.Session.New()
	defer session.Close()
	collection := session.DB(mongo.DBName).C(mongo.CollectionName)

	value["_id"] = key
	err := collection.Insert(bson.M(value))
	if !mgo.IsDup(err) {
		return err
	} else {
		return nil
	}
}

func (mongo *MongoDB) Read(key string) error {
	session := mongo.Session.New()
	defer session.Close()
	collection := session.DB(mongo.DBName).C(mongo.CollectionName)

	result := map[string]interface{}{}
	err := collection.FindId(key).One(&result)
	return err
}

func (mongo *MongoDB) Update(key string, value map[string]interface{}) error {
	session := mongo.Session.New()
	defer session.Close()
	collection := session.DB(mongo.DBName).C(mongo.CollectionName)

	err := collection.Update(bson.M{"_id": key}, bson.M(value))
	return err
}

func (mongo *MongoDB) Delete(key string) error {
	session := mongo.Session.New()
	defer session.Close()
	collection := session.DB(mongo.DBName).C(mongo.CollectionName)

	err := collection.Remove(bson.M{"_id": key})
	return err
}

func (mongo *MongoDB) Query(key string, args []interface{}) error {
	view := args[0].(string)

	session := mongo.Session.New()
	defer session.Close()
	collection := session.DB(mongo.DBName).C(mongo.CollectionName)

	var q, s bson.M
	var d string
	var pipe *mgo.Pipe
	switch view {
	case "name_and_street_by_city":
		q = bson.M{
			"city.f.f": args[1],
		}
		s = bson.M{
			"name.f.f.f": 1,
			"street.f.f": 1,
		}
	case "name_and_email_by_county":
		q = bson.M{
			"county.f.f": args[1],
		}
		s = bson.M{
			"name.f.f.f": 1,
			"email.f.f":  1,
		}
	case "achievements_by_realm":
		q = bson.M{
			"realm.f": args[1],
		}
		s = bson.M{
			"achievements": 1,
		}
	case "name_by_coins":
		q = bson.M{
			"coins.f": bson.M{
				"$gt": args[1].(float64) * 0.5,
				"$lt": args[1].(float64),
			},
		}
		s = bson.M{
			"name.f.f.f": 1,
		}
	case "email_by_achievement_and_category":
		q = bson.M{
			"category": args[2].(int16),
			"achievements.0": bson.M{
				"$gt": 0,
				"$lt": args[1].([]int16)[0] + 2,
			},
		}
		s = bson.M{
			"email.f.f": 1,
		}
	case "street_by_year_and_coins":
		q = bson.M{
			"year": args[1],
			"coins.f": bson.M{
				"$gt": args[2].(float64),
				"$lt": 655.35,
			},
		}
		s = bson.M{
			"street.f.f": 1,
		}
	case "name_and_email_and_street_and_achievements_and_coins_by_city":
		q = bson.M{
			"city.f.f": args[1],
		}
		s = bson.M{
			"name.f.f.f":   1,
			"email.f.f":    1,
			"street.f.f":   1,
			"achievements": 1,
			"coins.f":      1,
		}
	case "street_and_name_and_email_and_achievement_and_coins_by_county":
		q = bson.M{
			"county.f.f": args[1],
		}
		s = bson.M{
			"street.f.f":   1,
			"name.f.f.f":   1,
			"email.f.f":    1,
			"achievements": bson.M{"$slice": 1},
			"coins.f":      1,
		}
	case "category_name_and_email_and_street_and_gmtime_and_year_by_country":
		q = bson.M{
			"country.f": args[1],
		}
		s = bson.M{
			"category":   1,
			"name.f.f.f": 1,
			"email.f.f":  1,
			"street.f.f": 1,
			"gmtime":     1,
			"year":       1,
		}
	case "body_by_city":
		q = bson.M{
			"city.f.f": args[1],
		}
		s = bson.M{
			"body": 1,
		}
	case "body_by_realm":
		q = bson.M{
			"realm.f": args[1],
		}
		s = bson.M{
			"body": 1,
		}
	case "body_by_country":
		q = bson.M{
			"country.f": args[1],
		}
		s = bson.M{
			"body": 1,
		}
	case "distinct_states":
		d = args[1].(string)
	case "distinct_full_states":
		d = args[1].(string)
	case "distinct_years":
		d = args[1].(string)
	case "coins_stats_by_state_and_year":
		pipe = collection.Pipe(
			[]bson.M{
				{
					"$match": bson.M{
						"state.f": args[1],
						"year":    args[2],
					},
				},
				{
					"$group": bson.M{
						"_id": bson.M{
							"state": "$state.f",
							"year":  "$year",
						},
						"count": bson.M{"$sum": 1},
						"sum":   bson.M{"$sum": "$coins.f"},
						"avg":   bson.M{"$avg": "$coins.f"},
						"min":   bson.M{"$min": "$coins.f"},
						"max":   bson.M{"$max": "$coins.f"},
					},
				},
			},
		)
	case "coins_stats_by_gmtime_and_year":
		pipe = collection.Pipe(
			[]bson.M{
				{
					"$match": bson.M{
						"gmtime": args[1],
						"year":   args[2],
					},
				},
				{
					"$group": bson.M{
						"_id": bson.M{
							"gmtime": "$gmtime",
							"year":   "$year",
						},
						"count": bson.M{"$sum": 1},
						"sum":   bson.M{"$sum": "$coins.f"},
						"avg":   bson.M{"$avg": "$coins.f"},
						"min":   bson.M{"$min": "$coins.f"},
						"max":   bson.M{"$max": "$coins.f"},
					},
				},
			},
		)
	case "coins_stats_by_full_state_and_year":
		pipe = collection.Pipe(
			[]bson.M{
				{
					"$match": bson.M{
						"full_state.f": args[1],
						"year":         args[2],
					},
				},
				{
					"$group": bson.M{
						"_id": bson.M{
							"year":       "$year",
							"full_state": "$full_state.f",
						},
						"count": bson.M{"$sum": 1},
						"sum":   bson.M{"$sum": "$coins.f"},
						"avg":   bson.M{"$avg": "$coins.f"},
						"min":   bson.M{"$min": "$coins.f"},
						"max":   bson.M{"$max": "$coins.f"},
					},
				},
			},
		)
	}

	var err error
	result := []map[string]interface{}{}
	if len(q) != 0 {
		err = collection.Find(q).Select(s).Limit(20).All(&result)
	} else if len(d) != 0 {
		err = collection.Find(bson.M{}).Distinct(d, &result)
	} else {
		err = pipe.All(&result)
	}
	session.Close()

	return err
}

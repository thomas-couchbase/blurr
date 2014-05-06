package databases

import (
	"log"
	"strings"

	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type MongoDB struct {
	Collection *mgo.Collection
	Session    *mgo.Session
}

func (mongo *MongoDB) Init(config Config) {
	pool := strings.Join(config.Addresses, ",")
	var err error
	mongo.Session, err = mgo.Dial(pool)
	if err != nil {
		log.Fatal(err)
	}
	mongo.Session.SetMode(mgo.Monotonic, true)
	mongo.Collection = mongo.Session.DB(config.Name).C(config.Table)
}

func (mongo *MongoDB) Shutdown() {
	mongo.Session.Close()
}

func (mongo *MongoDB) Create(key string, value map[string]interface{}) error {
	value["_id"] = key
	err := mongo.Collection.Insert(bson.M(value))
	return err
}

func (mongo *MongoDB) Read(key string) error {
	result := map[string]interface{}{}
	err := mongo.Collection.FindId(key).One(&result)
	return err
}

func (mongo *MongoDB) Update(key string, value map[string]interface{}) error {
	err := mongo.Collection.Update(bson.M{"_id": key}, bson.M(value))
	return err
}

func (mongo *MongoDB) Delete(key string) error {
	err := mongo.Collection.Remove(bson.M{"_id": key})
	return err
}

func (mongo *MongoDB) Query(key string, args []interface{}) error {
	view := args[0].(string)

	var q, s bson.M
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
				"$lt": args[1].([]int16)[0],
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
	}

	result := []map[string]interface{}{}
	err := mongo.Collection.Find(q).Select(s).Limit(20).All(&result)
	return err
}

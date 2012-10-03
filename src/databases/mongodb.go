package databases


import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)


type MongoDB struct {
	Collection *mgo.Collection
	Session *mgo.Session
}


func (mongo *MongoDB) Init(config Config) {
	var err error
	var pool string = ""
	for _, address := range config.Addresses {
		pool += address + ","
	}
	mongo.Session, err = mgo.Dial(pool)
	if err != nil {
		panic(err)
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


func (mongo *MongoDB) Query(key string, value map[string]interface{}) error {
	return nil  // TODO: implement queries on secondary indexes
}

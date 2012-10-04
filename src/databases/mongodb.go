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


func (mongo *MongoDB) Query(fieldName, fieldValue string, limit int) error {
	var result []map[string]interface {}
	err := mongo.Collection.Find(bson.M{fieldName: bson.M{"$gte": fieldValue}}).Limit(limit).Iter().All(&result)
	return err
}

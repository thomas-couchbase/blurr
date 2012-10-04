blurr
-----

blurr is simple and flexible tool for database benchmarking. It's written in Go and supports custom database drivers and workloads.

Dependencies
------------

    go get labix.org/v2/mgo

Usage
-----
    blurr -path=workloads/workload.conf


Configuration files
-------------------
blurr uses JSON format for configuration. Example is below:

    {
        "Database": {
            "Driver": "MongoDB",
            "Name": "default",
            "Table": "default",
            "Addresses": [
                "10.2.2.1:27017",
                "10.2.2.2:27017",
                "10.2.2.3:27017"
            ]
        },
        "Workload": {
            "Type": "DefaultWorkload",
            "CreatePercentage": 4,
            "ReadPercentage": 60,
            "UpdatePercentage": 12,
            "DeletePercentage": 4,
            "QueryPercentage": 20,
            "Records": 100000,
            "Operations": 100000,
            "ValueSize": 2048,
            "IndexableFields": 9,
            "Workers": 16,
            "TargetThroughput": 2000
        }
    }

Configuration includes two groups of parameters:
* Database.Driver - database driver for benchmark
* Database.Name - name of database
* Database.Table - name of table, collection, bucket and etc.
* Database.Addresses - list of host:port string to use in connection pool
* Workload.Type - workload type (only DefaultWorkload is available so far)
* Workload.(Create|Read|Update|Delete|Query)Percentage - CRUD-Q operations ratio, sum must be equal 100
* Workload.Records - number of existing records(row, documents) in database before benchmark
* Workload.Operations - total number of operations to perform, defines benchmark run time
* Workload.ValueSize - total size of synthetic values
* Workload.IndexableFields - number of fields that must support secondary indexes (basically they have deterministic values)
* Workload.Workers - number of concurrent workers (threads, clients, and etc.)
* Workload.TargetThroughput - enable limited throughput if provided

Database interface
------------------
To add new database driver you should implement following protocol:

    type Database interface {
        Init(config Config)
  
      	Shutdown()
      
      	Create(key string, value map[string]interface{}) error
      
      	Read(key string) error
      
      	Update(key string, value map[string]interface{}) error
      
      	Delete(key string) error
      
      	Query(fieldName, fieldValue string, limit int) error
    }

Workload interface
------------------
Current workload interface:

    type Workload interface {
        Init(config Config)
      
      	GenerateNewKey(currentRecords int64) string
      
      	GenerateExistingKey(currentRecords int64) string
      
      	GenerateKeyForRemoval() string
      
      	GenerateValue(key string, indexableFields, size int) map[string]interface{}
      
      	GenerateQuery(indexableFields int, currentRecords int64) (fieldName, fieldValue string, limit int)
      
      	RunWorkload(database databases.Database, state *State, wg *sync.WaitGroup)
    }
blurr
-----

blurr is simple and flexible tool for database benchmarking. It's written in Go and supports custom database drivers and workloads.

Prerequisites
-------------

* go
* bzr

Installation
------------

    go get github.com/pavel-paulau/blurr

Usage
-----

    blurr workload.conf

Configuration files
-------------------

blurr uses JSON format for configuration. There are two groups of parameters, example is below:

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
            "Type": "N1QL",
            "CreatePercentage": 4,
            "ReadPercentage": 60,
            "UpdatePercentage": 32,
            "DeletePercentage": 4,
            "Records": 100000,
            "Operations": 100000,
            "ValueSize": 2048,
            "Workers": 16,
            "Throughput": 2000,
            "HotDataPercentage": 20,
            "HotSpotAccessPercentage": 95,
            "RunTime": 3600,
            "QueryWorkers": 10,
            "QueryThroughput": 100,
            "Indexes": [
        		"coins_stats_by_state_and_year",
        		"street_by_year_and_coins",
        		"distinct_years"
        	]
        }
    }

Basic parameters:

* Database.Driver - database driver for benchmark (MongoDB, Couchbase, Tuq or Cassandra)
* Database.Name - name of database
* Database.Table - name of table, collection, bucket and etc.
* Database.Addresses - list of host:port string to use in connection pool
* Workload.Type - workload type (Default or HotSpot)
* Workload.(Create|Read|Update|Delete)Percentage - CRUD operations ratio, sum must be equal 100
* Workload.Records - number of existing records(rows, documents) in database before benchmark
* Workload.Operations - total number of operations to perform, defines benchmark run time
* Workload.ValueSize - size of synthetic values
* Workload.Workers - number of concurrent CRUD workers (threads, clients, and etc.)
* Workload.Throughput - enable limited throughput of CRUD ops if provided
* Workload.HotDataPercentage - percentage of hot records in dataset (HotSpot workload)
* Workload.HotSpotAccessPercentage - percentage of operations that hit hot subset (HotSpot workload)
* Workload.RunTime - optional benchmark run time in seconds

Additional parameters for secondary indexes:

* Workload.QueryWorkers - number of concurrent query workers
* Workload.QueryThroughput - enable limited throughput of queries if provided
* Workload.Indexes - [optional] list of secondary indexes

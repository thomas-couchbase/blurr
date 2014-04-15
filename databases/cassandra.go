package databases

import (
	"log"
	"os"
	"strconv"

	"github.com/carloscm/gossie/src/gossie"
)

type Cassandra struct {
	Pool         gossie.ConnectionPool
	ColumnFamily string
}

type Column struct {
	Key   string `cf:"default" key:"Key" value:"Value"`
	Value string
}

func (cs *Cassandra) Init(config Config) {
	var err error
	var pool_size int64
	max_cores := os.Getenv("GOMAXPROCS")
	if len(max_cores) > 0 {
		pool_size, err = strconv.ParseInt(max_cores, 10, 0)
		if err != nil {
			log.Fatal(err)
		}
	}
	cs.Pool, err = gossie.NewConnectionPool(config.Addresses, config.Name,
		gossie.PoolOptions{Size: int(pool_size), Timeout: 5000})
	if err != nil {
		log.Fatal(err)
	}
	cs.ColumnFamily = config.Table
}

func (cs *Cassandra) Shutdown() {
	cs.Pool.Close()
}

func valueToRow(key string, value map[string]interface{}) *gossie.Row {
	mapping, _ := gossie.NewMapping(&Column{})
	row, _ := mapping.Map(&Column{key, value[key].(string)})
	return row
}

func (cs *Cassandra) Create(key string, value map[string]interface{}) error {
	row := valueToRow(key, value)
	err := cs.Pool.Writer().Insert(cs.ColumnFamily, row).Run()
	return err
}

func (cs *Cassandra) Read(key string) error {
	_, err := cs.Pool.Reader().Cf(cs.ColumnFamily).Get([]byte(key))
	return err
}

func (cs *Cassandra) Update(key string, value map[string]interface{}) error {
	row := valueToRow(key, value)
	err := cs.Pool.Writer().Insert(cs.ColumnFamily, row).Run()
	return err
}

func (cs *Cassandra) Delete(key string) error {
	err := cs.Pool.Writer().Delete(cs.ColumnFamily, []byte(key)).Run()
	return err
}

func (cb *Cassandra) Query(key string, args []interface{}) error {
	return nil
}

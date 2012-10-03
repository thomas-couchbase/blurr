package databases


type Config struct {
	Name string
	Table string
	Addresses []string
	Username string
	Password string
}


type Database interface {
	Init(config Config)

	Shutdown()

	Create(key string, value map[string]interface{}) error

	Read(key string) error

	Update(key string, value map[string]interface{}) error

	Delete(key string) error

	Query(fieldName, fieldValue string, limit int) error
}

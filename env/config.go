package env

type Config struct {
	DbUsername string
	DbPassword string
	DbSqlPort string
	Database string
}

func NewConfig(username, password, port, database string) Config {
	return Config{
		DbUsername: username,
		DbPassword: password,
		DbSqlPort: port,
		Database: database,
	}
}
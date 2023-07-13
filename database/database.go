package database

import (
	"log"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/driver/mysql"

	"github.com/pranav1698/data-ingest-pipeline/env"
)

func GetDatabaseConnection() (*gorm.DB, error) {
	conf := env.NewConfig("*******", "********", "3306", "******")
	dbUrl := fmt.Sprintf("%s:%s@tcp(127.0.0.1:%s)/%s", conf.DbUsername, conf.DbPassword, conf.DbSqlPort, conf.Database)

	db, err := gorm.Open(mysql.Open(dbUrl), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	log.Println("Connected to Database!")
	return db, nil
}
package support

import (
	"fmt"
	"log"
	"os"
	"reflect"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type DBConfig struct {
	Host, Port, DB, Username, Password string
}

func getDBConfigFromEnv() (*DBConfig, error) {
	config := &DBConfig{
		Host:     os.Getenv("DB_HOST"),
		Port:     os.Getenv("DB_PORT"),
		DB:       os.Getenv("DB_NAME"),
		Username: os.Getenv("DB_USERNAME"),
		Password: os.Getenv("DB_PASSWORD"),
	}

	values := reflect.ValueOf(*config)
	for i := 0; i < values.NumField(); i++ {
		if values.Field(i).Interface() == "" {
			return nil, fmt.Errorf("Not all env variables needed to connect to database were set")
		}
	}
	return config, nil
}

func ConnectToDB() (*gorm.DB, error) {
	conf, err := getDBConfigFromEnv()
	if err != nil {
		return nil, err
	}
	connectionString := "host=%s port=%s user=%s password=%s dbname=%s"
	connectionString = fmt.Sprintf(connectionString, conf.Host, conf.Port, conf.Username, conf.Password, conf.DB)

	log.Printf("Connect string: %s", connectionString)
	return gorm.Open(postgres.Open(connectionString), &gorm.Config{})
}

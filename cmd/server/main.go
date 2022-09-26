package main

import (
	"log"

	api_server "github.com/c12s/blackhole/pkg/api"
	"github.com/c12s/blackhole/pkg/models"
	"github.com/c12s/blackhole/support"
	"gorm.io/gorm"
)

func migrateDBstructure(db *gorm.DB) {
	db.AutoMigrate(&models.TokenBucket{})
	db.AutoMigrate(&models.TaskQueue{})
}
func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Println("Starting server up...")
	db, err := support.ConnectToDB()
	if err != nil {
		log.Fatalf("Cant connect to the DB: %s", err.Error())
	}
	migrateDBstructure(db)
	api_server.Serve(db)
}

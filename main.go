package main

import (
	"context"
	"fmt"
	"github.com/c12s/blackhole/model"
	"github.com/c12s/blackhole/service"
	"github.com/c12s/blackhole/storage/etcd"
	"time"
)

func main() {
	conf, err := model.LoadConfig("./config.yml")
	if err != nil {
		fmt.Println(err)
		return
	}

	requestTimeout := 10 * time.Second
	db, dbErr := etcd.New(conf.DB, requestTimeout)
	if dbErr != nil {
		fmt.Println(dbErr)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	service.Run(ctx, db, conf.Address, conf.Opts)
	cancel()
}

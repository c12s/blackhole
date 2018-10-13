package main

import (
	"fmt"
	"github.com/c12s/blackhole/model"
	// "github.com/c12s/blackhole/service"
)

func main() {
	conf, err := model.LoadConfig("./config.yml")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(conf)
	for _, q := range conf.Opts {
		fmt.Println(q)
	}
	// service.Run(nil, "localhost:8081")
}

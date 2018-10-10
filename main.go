package main

import (
	"fmt"
	"github.com/c12s/blackhole/service"
)

func main() {
	fmt.Println("Hello world!")
	service.Run(nil, "localhost:8081")
}

package model

import (
	"time"
)

type TaskOption struct {
	Name         string
	MaxWorkers   int
	MaxQueued    int
	Capacity     int
	Tokens       int
	FillInterval time.Duration
}

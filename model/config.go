package model

import (
	"time"
)

type TaskOption struct {
	Name         string
	NaxWorkers   int
	MaxQueued    int
	Capacity     int
	Tokens       int
	FillInterval time.Duration
}

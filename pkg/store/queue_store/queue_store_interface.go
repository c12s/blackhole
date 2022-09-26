package store

import (
	"github.com/c12s/blackhole/pkg/models/tasks"
)

type QueueStore interface {
	Enqueue(*tasks.Task) error
	Dequeue() (*tasks.Task, error)
	Size() int
}

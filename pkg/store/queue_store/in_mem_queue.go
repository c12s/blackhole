package store

import (
	"fmt"

	"github.com/c12s/blackhole/pkg/models/tasks"
)

type inMemQueue struct {
	queue []*tasks.Task
}

func NewInMemQueue() *inMemQueue {
	return &inMemQueue{
		queue: make([]*tasks.Task, 0),
	}
}
func (imq *inMemQueue) Enqueue(task *tasks.Task) error {
	imq.queue = append(imq.queue, task)
	return nil
}

func (imq *inMemQueue) Dequeue() (*tasks.Task, error) {
	queueLength := len(imq.queue)
	if queueLength == 0 {
		return nil, fmt.Errorf("Queue is empty")
	}
	task := imq.queue[queueLength-1]
	imq.queue = imq.queue[:queueLength-1]
	return task, nil
}

func (imq *inMemQueue) Size() int {
	return len(imq.queue)
}

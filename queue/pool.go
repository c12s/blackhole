package queue

import (
	"context"
	"fmt"
	pb "github.com/c12s/scheme/core"
	"log"
)

func newWorker(ctx context.Context, jobs chan *pb.Task, done, active chan string, id int) *Worker {
	wid := fmt.Sprintf("worker_%d")
	kill := make(chan bool)
	go func(idx string) {
		for {
			select {
			case <-ctx.Done():
				log.Print(ctx.Err())
				return
			case <-jobs:
				active <- wid // signal that worker is taken the job

				// do some job send task to clusters/regions/nodes using nats or some other pbu/sub system.
				log.Print("Do something")

				done <- wid // signal that worker is free
			case <-kill:
				log.Print("Worker killed")
				return
			}
		}
	}(wid)
	return &Worker{
		ID:   wid,
		Kill: kill,
	}
}

func (wp *WorkerPool) init(ctx context.Context) {
	for i := 0; i < wp.MaxWorkers; i++ {
		wk := newWorker(ctx, wp.Pipe, wp.done, wp.active, i)
		wp.Workers[wk.ID] = wk
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Print(ctx.Err())
				return
			case wIdx := <-wp.active:
				wp.ActiveWorkers[wIdx] = wp.Workers[wIdx]
			case wIdx := <-wp.done:
				delete(wp.ActiveWorkers, wIdx)
			}
		}
	}()
}

// We hove number_of_tokens, max_workers and active_workers
// We need to determine is there free workers to pick update
// new tasks
func (wp *WorkerPool) Ready(tokens int64) bool {
	return (int64(wp.MaxWorkers) - int64(len(wp.ActiveWorkers))) >= tokens
}

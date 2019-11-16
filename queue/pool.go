package queue

import (
	"context"
	"fmt"
	cPb "github.com/c12s/scheme/celestial"
	pb "github.com/c12s/scheme/core"
	"log"
)

func (wp *WorkerPool) newWorker(ctx context.Context, jobs chan *pb.Task, done, active chan string, id int, celestial string) {
	wid := fmt.Sprintf("worker_%d")
	kill := make(chan bool)
	for {
		select {
		case <-ctx.Done():
			log.Print(ctx.Err())
			return
		case task := <-jobs:
			active <- wid // signal that worker is taken the job

			mt := &cPb.MutateReq{Mutate: task}
			client := NewCelestialClient(celestial)
			_, err := client.Mutate(ctx, mt)
			if err != nil {
				log.Println(err)
			}
			fmt.Println("Otisao zahtev u celestial")

			done <- wid // signal that worker is free
		case <-kill:
			log.Print("Worker killed")
			return
		}
	}

	wp.Workers[wid] = &Worker{
		ID:   wid,
		Kill: kill,
	}
}

func (wp *WorkerPool) init(ctx context.Context) {
	for i := 0; i < wp.MaxWorkers; i++ {
		go wp.newWorker(ctx, wp.Pipe, wp.done, wp.active, i, wp.Celestial)
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

package queue

import (
	"context"
	pb "github.com/c12s/blackhole/pb"
	"log"
	"math"
	"time"
)

func (b *TokenBucket) Start(ctx context.Context) {
	//every fillTime we try to add new token to the bucket (execute the task)
	go func(bucket *TokenBucket) {
		ticker := time.NewTicker(b.FillInterval)
		for {
			select {
			case <-ticker.C:
				if bucket.Tokens < bucket.Capacity {
					bucket.Tokens++
				} else {
					b.Notify <- true
				}
			case <-ctx.Done():
				log.Print(ctx.Err())
				ticker.Stop()
				return
			}
		}
	}(b)
}

func (b *TokenBucket) TakeAll() (bool, int) {
	if b.Tokens == 0 {
		return false, 0
	}

	if b.Tokens == b.Capacity {
		b.Tokens = 0
		return true, b.Capacity
	}
	return false, 0
}

func (t *TaskQueue) newWorker(ctx context.Context, killOnDone bool) *Worker {
	jobs := make(chan *pb.Task)
	kill := make(chan bool)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Print(ctx.Err())
				return
			case <-jobs: //TODO: Do something with the task
				log.Print("Do something")

				if killOnDone {
					return
				}
				done <- true
			case <-kill:
				return
			}
		}
	}()

	return &Worker{
		Kill: kill,
		Jobs: jobs,
		Done: done,
	}
}

func (t *TaskQueue) spawn(ctx context.Context, tokens int) bool {
	tasks, err := t.Queue.TakeTasks(ctx, tokens)
	if err != nil {
		return false
	}

	if tokens <= t.MaxWorkers {
		miss := math.Abs(len(t.WorkerPool) - tokens)
		for i := 0; i < miss; i++ {
			t.newWorker(ctx, true)
		}
	}

	for i, v := range tasks {
		t.WoorkerPool[i] <- ch
	}
	return true
}

func (t *TaskQueue) Loop(ctx context.Context) {
	go func() {
		for {
			select {
			case <-t.Bucket.Notify:
				done, tokens := t.Bucket.TakeAll()
				if done {
					t.spawn(ctx, tokens)
				}
			case <-ctx.Done():
				log.Print(ctx.Err())
				return
			}
		}
	}()
}

func (t *TaskQueue) StartQueue(ctx context.Context) {
	t.Bucket.Start(ctx)
	t.Loop(ctx)
}

func (t *TaskQueue) PutTasks(ctx context.Context, req *pb.PutReq) (*pb.Resp, error) {
	return t.Queue.PutTasks(ctx, req)
}

package queue

import (
	"context"
	"github.com/c12s/blackhole/model"
	pb "github.com/c12s/scheme/blackhole"
	"log"
	"math"
	"time"
)

func (b *TokenBucket) retry() time.Duration {
	switch b.TRetry.Delay {
	case "linear":
		lVal := b.Attempt * b.TRetry.Doubling
		return time.Duration(lVal) * model.DetermineInterval(b.FillInterval)
	case "exp":
		exVal := math.Pow(float64(b.TRetry.Doubling), float64(b.Attempt))
		return time.Duration(exVal) * model.DetermineInterval(b.FillInterval)
	default:
		return time.Second
	}
}

func (b *TokenBucket) Start(ctx context.Context) {
	//every fillTime we try to add new token to the bucket (execute the task)
	go func() {
		interval := model.DetermineInterval(b.FillInterval)
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-ticker.C:
				if b.Tokens < b.Capacity {
					b.Tokens++
				} else {
					b.Notify <- true
				}
			case <-ctx.Done():
				log.Print(ctx.Err())
				ticker.Stop()
				return
			case <-b.Delay:
				// Increase attempt value
				if b.Attempt < b.TRetry.Limit {
					b.Attempt++

					//Increase ticking time based on strategy user provided
					ticker.Stop()
					ticker = time.NewTicker(b.retry())
				} else {
					ticker.Stop()
					ticker = nil // put tockenbucket in sleep state
				} // we got to Limit value, and should put timer to sleep

			case <-b.Reset:
				// if ticker is nil, than token bucket is in 'sleep state' -> not ticking
				// and need to be restarted! This is done on next task update
				if ticker == nil {
					// Reset retry
					b.Attempt = 0

					// Reset Ticker
					interval = model.DetermineInterval(b.FillInterval)
					ticker = time.NewTicker(interval)
				}
			}
		}
	}()
}

func (b *TokenBucket) TakeAll() (bool, int64) {
	if b.Tokens == 0 {
		return false, 0
	}

	if b.Tokens == b.Capacity {
		b.Tokens = 0
		return true, b.Capacity
	}
	return false, 0
}

func (t *TaskQueue) Loop(ctx context.Context) {
	go func() {
		for {
			select {
			case <-t.Bucket.Notify:
				//if pool is ready to take new tasks, take them...oterwise signal tockenbucket to increase notify time.
				//When pool is ready to take new tasks reset toke bucket to regular interval
				if t.Pool.Ready(t.Bucket.Capacity) {
					done, tokens := t.Bucket.TakeAll()
					if done {
						if sync := t.Sync(ctx, tokens); !sync {
							t.Bucket.Delay <- true // if there are no tasks in queue. Try it for a few times, than go to sleep unitl next task submit
						}
					}
				} else {
					t.Bucket.Delay <- true // if pool not ready, make delay
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
	t.Bucket.Reset <- true
	return t.Queue.PutTasks(ctx, req)
}

func (t *TaskQueue) Sync(ctx context.Context, tokens int64) bool {
	tasks, err := t.Queue.TakeTasks(ctx, t.Name, t.Namespace, tokens)
	if err != nil {
		log.Println(err)
		return false
	}
	for _, task := range tasks {
		t.Pool.Pipe <- task
	}

	return len(tasks) > 0 // if TakeTasks return 0 that means that there is no tasks to pick from queue.
}

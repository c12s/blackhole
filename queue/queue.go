package queue

import (
	"context"
	pb "github.com/c12s/blackhole/pb"
	"log"
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

func (t *TaskQueue) worker(ctx context.Context, killOnDone bool) chan *pb.Task {
	ch := make(chan *pb.Task)
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Print(ctx.Err())
				return
			case <-ch: //TODO: Do something with the task
				log.Print("Do something")
				return
			}
		}
	}()
	return ch
}

func (t *TaskQueue) spawn(ctx context.Context, tokens int) bool {
	tasks, err := t.Queue.Take(ctx, tokens)
	if err != nil {
		return false
	}

	// TODO: Check how mush workers user want to spawn, and than spawn them!!!
	for _, v := range tasks {
		ch := t.worker(ctx, false)
		ch <- v
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

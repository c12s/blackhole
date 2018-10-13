package queue

import (
	"context"
	"errors"
	"github.com/c12s/blackhole/model"
	pb "github.com/c12s/blackhole/pb"
	storage "github.com/c12s/blackhole/storage"
	"time"
)

type Worker struct {
	Kill chan bool     //channel to kill worker
	Jobs chan *pb.Task // channel to take jobs from
	Done chan bool     // channel to notify that he is done so he can take new job
}

type TokenBucket struct {
	Capacity     int
	Tokens       int
	FillInterval time.Duration
	Notify       chan bool
}

type TaskQueue struct {
	Queue      storage.DB
	Bucket     *TokenBucket
	MaxQueued  int
	MaxWorkers int
	WorkerPool []*Worker
}

type BlackHole struct {
	Queues map[string]*TaskQueue
}

func (bh *BlackHole) GetTK(name string) (*TaskQueue, error) {
	if tk, ok := bh.Queue[name]; ok {
		return tk, nil
	}
	return nil, errors.New("Queue not exists!")
}

func newQueue(bucket *TokenBucket, maxQueued, maxWorkers int, db storage.DB) *TaskQueue {
	return &TaskQueue{
		Queue:      db,
		Bucket:     bucket,
		MaxQueued:  maxQueued,
		MaxWorkers: maxWorkers,
		WorkerPool: []*Worker{},
	}
}

func newBucket(capacity, tokens int, fillInterval time.Duration) *TokenBucket {
	return &TokenBucket{
		Capacity:     capacity,
		Tokens:       tokens,
		FillInterval: fillInterval,
		Notify:       make(chan bool),
	}
}

func New(ctx context.Context, db storage.DB, options []*model.TaskOption) *BlackHole {
	q := map[string]*TaskQueue{}
	for _, opt := range options {
		tb := newBucket(opt.Capacity, opt.Tokens, opt.FillInterval)
		tq := newQueue(tb, opt.MaxQueued, opt.MaxWorkers, db)

		q[opt.Name] = tq
		tq.StartQueue(ctx)
	}

	return &BlackHole{
		Queues: q,
	}
}

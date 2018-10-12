package queue

import (
	"context"
	"errors"
	"github.com/c12s/blackhole/model"
	storage "github.com/c12s/blackhole/storage"
	"time"
)

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
}

type BlackHole struct {
	Queues map[string]*TaskQueue
}

func (bh *BlackHole) GetTK(name string) (*TaskQueue, error) {
	if tk, ok := bh.Queue[name]; ok {
		return tk, nil
	}
	return nil, errors.New("Queue dont exists!")
}

func newQueue(bucket *TokenBucket, maxQueued, maxWorkers int, db storage.DB) *TaskQueue {
	return &TaskQueue{
		Queue:      db,
		Bucket:     bucket,
		MaxQueued:  maxQueued,
		MaxWorkers: maxWorkers,
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

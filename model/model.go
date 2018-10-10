package model

import (
	"github.com/c12s/blackhole/storage"
	"time"
)

type TokenBucket struct {
	Capacity     int
	Tokens       int
	FillInterval time.Duration
}

type TaskQueue struct {
	Queue      *storage.DB
	Bucket     *TokenBucket
	MaxQueued  int
	MaxWorkers int
}

type BlackHole struct {
	Queues map[string]*TaskQueue
}

func newQueue(bucket *TaskBucket, maxQueued, maxWorkers int, db *storage.DB) *TaskQueue {
	return &TaskQueue{
		Queued:     db,
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
	}
}

func New(db *storage.DB, options ...*TaskOption) *BlackHole {
	q := map[string]*TaskQueue{}
	for _, opt := range options {
		bucket := newBucket(opt.Capacity, opt.Tokens, opt.FillInterval)
		q[opt.Name] = newQueue(bucket, opt.MaxQueued, opt.MaxWorkers, db)
	}

	return &BlackHole{
		Queues: q,
	}
}

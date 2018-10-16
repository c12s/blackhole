package db

import (
	"context"
	pb "github.com/c12s/blackhole/pb"
)

type DB interface {
	// Put tasks in the queue (user submit). Take n tasks from queue (done by workers)
	PutTasks(ctx context.Context, req *pb.PutReq) (*pb.Resp, error)
	TakeTasks(ctx context.Context, name, user_id string, tokens int64) (map[string]*pb.Task, error)

	// Add and Remove queues
	AddQueue(ctx context.Context, name, user_id string) error
	RemoveQueue(ctx context.Context, name, user_id string) error

	// Close connection to the persistent storage
	Close()
}

package db

import (
	"context"
	pb "github.com/c12s/blackhole/pb"
)

type DB interface {
	Put(ctx context.Context, req *pb.PutReq) (*pb.Resp, error)
	Get(ctx context.Context, req *pb.GetReq) (*pb.Resp, error)
	Take(ctx context.Context, tokens int) ([]*pb.Task, error)
}

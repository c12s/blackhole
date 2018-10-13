package service

import (
	"context"
	"fmt"
	pb "github.com/c12s/blackhole/pb"
	"github.com/c12s/blackhole/queue"
	"github.com/c12s/blackhole/storage"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
)

type Server struct {
	Queue *queue.BlackHole
}

func (s *Server) getTK(ctx context.Context, req *req.PutReq) (*queue.TaskQueue, error) {
	if req.ForceNSQueue {
		tk, err := s.Queue.GetTK(req.Mtdata.Namespace)
		if err != nil {
			return nil, err
		}
		return tk, nil
	}
	tk, err := s.Queue.GetTK(req.Mtdata.TaskName)
	if err != nil {
		return nil, err
	}
	return rk, nil
}

func (s *Server) Put(ctx context.Context, req *pb.PutReq) (*pb.Resp, error) {
	tk, err := s.getTK(ctx, teq)
	if err != nil {
		return nil, err
	}

	go func() {
		// Call celestial api and put into DB and set status as Waiting or something
	}()

	go func() {
		// Put into queue and wait for run time. after job is done, update celestial entry as Running or something
	}()

	// return to client that task is accepted!
	return &pb.Resp{Msg: "Accepted"}, nil
}

func Run(ctx context.Context, db storage.DB, address string) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to initializa TCP listen: %v", err)
	}
	defer lis.Close()

	server := grpc.NewServer()
	blackholeServer := &Server{
		Queue: queue.New(ctx, db, opts),
	}

	fmt.Println("BlackHoleService RPC Started")
	pb.RegisterBlackHoleServiceServer(server, blackholeServer)
	server.Serve(lis)
}

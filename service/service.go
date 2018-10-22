package service

import (
	"fmt"
	"github.com/c12s/blackhole/model"
	"github.com/c12s/blackhole/queue"
	storage "github.com/c12s/blackhole/storage"
	pb "github.com/c12s/scheme/blackhole"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
)

type Server struct {
	Queue *queue.BlackHole
}

func (s *Server) getTK(ctx context.Context, req *pb.PutReq) (*queue.TaskQueue, error) {
	if req.Mtdata.ForceNamespaceQueue {
		tk, err := s.Queue.GetTK(req.Mtdata.Namespace)
		if err != nil {
			return nil, err
		}
		return tk, nil
	}
	tk, err := s.Queue.GetTK(req.Mtdata.Queue)
	if err != nil {
		return nil, err
	}
	return tk, nil
}

func (s *Server) Put(ctx context.Context, req *pb.PutReq) (*pb.Resp, error) {
	tk, err := s.getTK(ctx, req)
	if err != nil {
		return nil, err
	}

	pResp, err := tk.PutTasks(ctx, req)
	if err != nil {
		log.Println(err)
	}

	// return to client that task is accepted!
	return &pb.Resp{Msg: pResp.Msg}, nil
}

func Run(ctx context.Context, db storage.DB, address, celestial string, opts []*model.TaskOption) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to initializa TCP listen: %v", err)
	}
	defer lis.Close()

	server := grpc.NewServer()
	blackholeServer := &Server{
		Queue: queue.New(ctx, db, opts, celestial),
	}

	fmt.Println("BlackHoleService RPC Started")
	pb.RegisterBlackHoleServiceServer(server, blackholeServer)
	server.Serve(lis)
}

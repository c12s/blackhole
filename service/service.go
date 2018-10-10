package service

import (
	"fmt"
	"github.com/c12s/blackhole/model"
	pb "github.com/c12s/blackhole/pb"
	"github.com/c12s/blackhole/storage"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
)

type Server struct {
	Queue *model.BlackHole
}

func (s *Server) Put(ctx context.Context, req *pb.PutReq) (*pb.Resp, error) {
	return nil, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetReq) (*pb.Resp, error) {
	return nil, nil
}

func Run(db *storage.DB, address string, opts ...*model.TaskQueue) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to initializa TCP listen: %v", err)
	}
	defer lis.Close()

	server := grpc.NewServer()
	blackholeServer := &Server{
		Queue: model.New(db, opts),
	}

	fmt.Println("BlackHoleService RPC Started")
	pb.RegisterBlackHoleServiceServer(server, blackholeServer)
	server.Serve(lis)
}

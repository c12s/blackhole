package service

import (
	"fmt"
	"github.com/c12s/blackhole/db"
	pb "github.com/c12s/blackhole/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
)

type Server struct {
	db db.DB
}

func (s *Server) Put(ctx context.Context, req *pb.PutReq) (*pb.Resp, error) {
	return nil, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetReq) (*pb.Resp, error) {
	return nil, nil
}

func Run(storage db.DB, address string) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to initializa TCP listen: %v", err)
	}
	defer lis.Close()

	server := grpc.NewServer()
	blackholeServer := &Server{
		db: storage,
	}

	fmt.Println("BlackHoleService RPC Started")
	pb.RegisterBlackHoleServiceServer(server, blackholeServer)
	server.Serve(lis)
}

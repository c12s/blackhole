package service

import (
	"fmt"
	"github.com/c12s/blackhole/model"
	"github.com/c12s/blackhole/queue"
	storage "github.com/c12s/blackhole/storage"
	aPb "github.com/c12s/scheme/apollo"
	pb "github.com/c12s/scheme/blackhole"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

type Server struct {
	Queue  *queue.BlackHole
	Apollo string
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
	client := NewApolloClient(s.Apollo)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Auth(ctx, &aPb.AuthOpt{})
	if err != nil {
		return &pb.Resp{Msg: err.Error()}, nil
	}

	if !resp.Value {
		return &pb.Resp{Msg: "You do not have access for this service"}, nil
	}

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

func Run(ctx context.Context, db storage.DB, address, celestial, apollo string, opts []*model.TaskOption) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to initializa TCP listen: %v", err)
	}
	defer lis.Close()

	server := grpc.NewServer()
	blackholeServer := &Server{
		Queue:  queue.New(ctx, db, opts, celestial),
		Apollo: apollo,
	}

	fmt.Println("BlackHoleService RPC Started")
	pb.RegisterBlackHoleServiceServer(server, blackholeServer)
	server.Serve(lis)
}

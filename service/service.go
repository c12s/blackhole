package service

import (
	"fmt"
	"github.com/c12s/blackhole/model"
	pb "github.com/c12s/blackhole/pb"
	"github.com/c12s/blackhole/queue"
	storage "github.com/c12s/blackhole/storage"
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

	go func() {
		// Call celestial api and put into DB and set status as Waiting or something
		// This i manuly so that user know that his task is submitted! After
		// the task is taken from queue and executed that key will be updated
		// with some real value!
	}()

	go func() {
		pResp, err := tk.PutTasks(ctx, req)
		if err != nil {
			fmt.Println(err) //TODO: This should go in some log system! And should update celestial api that there was an error!
			return
		}
		fmt.Println(pResp) //TODO: This should go in some log system
	}()

	// return to client that task is accepted!
	return &pb.Resp{Msg: "Accepted"}, nil
}

func Run(ctx context.Context, db storage.DB, address string, opts []*model.TaskOption) {
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

package api_server

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/c12s/blackhole/pkg/models"
	"github.com/c12s/blackhole/pkg/models/tasks"
	pb "github.com/c12s/blackhole/proto/blackhole"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type grpc_server struct {
	pb.UnimplementedBlackholeServer
	db           *gorm.DB
	queueManager models.TaskQueueManager
}

func (s *grpc_server) AddTask(ctx context.Context, in *pb.Task) (*pb.Task, error) {
	log.Println("[AddTask]: Endpoint execution.")
	queueId, err := uuid.Parse(in.QueueId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}
	task := &tasks.Task{
		Name:    in.Name,
		QueueID: queueId,
		WorkerDestination: tasks.WorkerDestination{
			Url:    in.WorkerDestination.Url,
			Method: in.WorkerDestination.Method,
		},
		Payload:    in.Payload,
		MaxRetries: int(in.MaxRetries),
		RetryDelay: time.Duration(in.RetryDelay) * time.Millisecond,
		Timeout:    time.Duration(in.Timeout) * time.Millisecond,
	}
	task, err = s.queueManager.NewTask(task)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, err.Error())
	}
	in.State = pb.Task_State(task.State)
	in.Id = task.ID.String()
	return in, nil
}

func (s *grpc_server) CreateTaskQueue(ctx context.Context, in *pb.TaskQueue) (*pb.TaskQueue, error) {
	log.Println("[CreateQueue]: Endpoint execution.")
	taskManager := s.queueManager.CreateQueue(in.Name, in.BucketSize, in.RefreshRate)
	in.Id = taskManager.ID.String()
	return in, nil
}

func Serve(db *gorm.DB) {
	log.Println("Starting grpc server...")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", os.Getenv("GRPC_PORT")))
	if err != nil {
		panic(err)
	}

	in_mem_queue_manager, err := models.InitTaskQueueManager(3, db)
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()
	pb.RegisterBlackholeServer(server, &grpc_server{db: db, queueManager: *in_mem_queue_manager})
	reflection.Register(server)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to start grpc server: %v", err)
	} else {
		log.Println("Blackhole server listening for requests at port 50051")
	}
}

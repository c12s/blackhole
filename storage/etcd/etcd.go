package etcd

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/c12s/blackhole/pb"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
)

func (s *StorageEtcd) PutTasks(ctx context.Context, req *pb.PutReq) (*pb.Resp, error) {
	for num, task := range req.Tasks {
		qt := &pb.Task{
			UserId:    req.UserId,
			Kind:      req.Kind,
			Timestamp: req.Mtdata.Timestamp,
			Namespace: req.Mtdata.Namespace,
			Task:      task,
		}
		data, err := proto.Marshal(qt)
		if err != nil {
			fmt.Println(err) //TODO: this should go to some log system!!
		}

		var key = ""
		if req.Mtdata.ForceNamespaceQueue {
			key = TaskKey(req.UserId, req.Mtdata.Namespace, req.Mtdata.TaskName, req.Mtdata.Timestamp, num)
		} else {
			key = TaskKey(req.UserId, req.Mtdata.Queue, req.Mtdata.TaskName, req.Mtdata.Timestamp, num)
		}

		_, err = s.Kv.Put(ctx, key, string(data))
		if err != nil {
			fmt.Println(err) //TODO: this should go to some log system!!
		}
	}
	return nil, nil
}

func (s *StorageEtcd) TakeTasks(ctx context.Context, name, user_id string, tokens int64) (map[string]*pb.Task, error) {
	retTasks := map[string]*pb.Task{}
	key := QueueKey(user_id, name)
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(tokens),
	}
	gresp, err := s.Kv.Get(ctx, key, opts...)
	if err != nil {
		return nil, err
	}

	dresp, err2 := s.Kv.Delete(ctx, key, opts...)
	if err2 != nil {
		return nil, err2
	}

	if int64(len(gresp.Kvs)) == dresp.Deleted {
		for _, item := range gresp.Kvs {
			newTask := &pb.Task{}
			err = proto.Unmarshal(item.Value, newTask)
			if err != nil {
				fmt.Println(err) // TODO: this should go to some log system!!
				continue
			}
			retTasks[string(item.Key)] = newTask
		}
	}

	return retTasks, nil
}

func (s *StorageEtcd) AddQueue(ctx context.Context, name, user_id string) error {
	key := QueueKey(user_id, name)
	opts := []clientv3.OpOption{
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	}
	gresp, err := s.Kv.Get(ctx, key, opts...)
	if err != nil {
		return err
	}

	if len(gresp.Kvs) > 0 {
		return errors.New("Queue already exists!")
	}

	_, err = s.Kv.Put(ctx, key, fmt.Sprintf("%s_queue", name))
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageEtcd) RemoveQueue(ctx context.Context, name, user_id string) error {
	key := QueueKey(user_id, name)
	_, err := s.Kv.Delete(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageEtcd) Close() {
	s.Client.Close()
}

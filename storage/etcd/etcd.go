package etcd

import (
	"context"
	"fmt"
	"github.com/c12s/blackhole/model"
	bPb "github.com/c12s/scheme/blackhole"
	cPb "github.com/c12s/scheme/core"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
)

func (s *StorageEtcd) put(ctx context.Context, req *bPb.PutReq, num int, task *bPb.PutTask) {
	qt := &cPb.Task{
		UserId:    req.UserId,
		Kind:      req.Kind,
		Timestamp: req.Mtdata.Timestamp,
		Namespace: req.Mtdata.Namespace,
		Extras:    req.Extras,
	}

	if task != nil {
		qt.Task = task
	}

	data, err := proto.Marshal(qt)
	if err != nil {
		fmt.Println(err) //TODO: this should go to some log system!!
	}

	var key = ""
	if req.Mtdata.Namespace == "" && req.Mtdata.Queue == "" {
		key = TaskKey(qdefault, qdefault, req.Mtdata.TaskName, req.Mtdata.Timestamp, num)
	} else if req.Mtdata.ForceNamespaceQueue {
		key = TaskKey(req.Mtdata.Namespace, req.Mtdata.Namespace, req.Mtdata.TaskName, req.Mtdata.Timestamp, num)
	} else {
		key = TaskKey(req.Mtdata.Namespace, req.Mtdata.Queue, req.Mtdata.TaskName, req.Mtdata.Timestamp, num)
	}

	_, err = s.Kv.Put(ctx, key, string(data))
	if err != nil {
		fmt.Println(err) //TODO: this should go to some log system!!
	}
}

func (s *StorageEtcd) PutTasks(ctx context.Context, req *bPb.PutReq) (*bPb.Resp, error) {
	if len(req.Tasks) > 0 {
		for num, task := range req.Tasks {
			s.put(ctx, req, num, task)
		}
	} else {
		s.put(ctx, req, 0, nil)
	}
	return &bPb.Resp{Msg: "Task accepted"}, nil
}

func (s *StorageEtcd) TakeTasks(ctx context.Context, name, namespace string, tokens int64) (map[string]*cPb.Task, error) {
	retTasks := map[string]*cPb.Task{}
	key := QueueKey(namespace, name)
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(tokens),
	}
	gresp, err := s.Kv.Get(ctx, key, opts...)
	if err != nil {
		return nil, err
	}

	if int64(len(gresp.Kvs)) <= tokens {
		for _, item := range gresp.Kvs {
			newTask := &cPb.Task{}
			err = proto.Unmarshal(item.Value, newTask)
			if err != nil {
				return nil, err
			}
			retTasks[string(item.Key)] = newTask
			_, err = s.Kv.Delete(ctx, string(item.Key))
			if err != nil {
				return nil, err
			}
		}
	}

	return retTasks, nil
}

func (s *StorageEtcd) AddQueue(ctx context.Context, opt *model.TaskOption) error {
	key := NewQueueKey(opt.Namespace, opt.Name)
	err, value := toString(opt)
	if err != nil {
		return err
	}
	_, err = s.Kv.Put(ctx, key, value)
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageEtcd) RemoveQueue(ctx context.Context, name, namespace string) error {
	key := RemoveQueueKey(namespace, name)
	_, err := s.Kv.Delete(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageEtcd) Close() {
	s.Client.Close()
}

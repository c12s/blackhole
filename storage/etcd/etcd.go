package etcd

import (
	"context"
	"fmt"
	pb "github.com/c12s/blackhole/pb"
	"github.com/coreos/etcd/clientv3"
	"time"
)

type StorageEtcd struct {
	Kv     clientv3.KV
	Client *clientv3.Client
}

func (s *StorageEtcd) Put(ctx context.Context, req *pb.PutReq) (*pb.Resp, error) {
	key := QueueKey(req.Name, req.UserId)
	for id, task := range req.Tasks {

	}
	return nil, nil
}

func (s *StorageEtcd) Take(ctx context.Context, name, user_id string, tokens int) (*pb.Task, error) {
	key := QueueKey(name, user_id)
	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(tokens),
	}
	resp, err := s.Kv.Get(ctx, key, opts)
	if err != nil {
		return nil, err
	}

	for _, item := range resp.Kvs {
		fmt.Println(string(item.Key), string(item.Value))
	}
	return nil, nil
}

func (s *StorageEtcd) Add(ctx context.Context, name, user_id string) error {
	key := QueueKey(user_id, name)
	_, err = s.Kv.Put(ctx, key, fmt.Sprintf("%s_queue_%s", name, user_id))
	if err != nil {
		return err
	}
	return nil
}

func (s *StorageEtcd) Remove(ctx context.Context, name, user_id string) error {
	key := QueueKey(user_id, name)
	_, err := s.Kv.Delete(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	return nil
}

func New(ctx context.Context, addr []string, timeout time.Duration) (*StorageEtcd, error) {
	cli, err := clientv3.New(clientv3.Config{
		DialTimeout: timeout,
		Endpoints:   addr,
	})
	if err != nil {
		return nil, err
	}

	return &StorageEtcd{
		Kv:     clientv3.NewKV(cli),
		Client: cli,
	}, nil
}

func (s *StorageEtcd) Close() {
	s.Client.Close()
}

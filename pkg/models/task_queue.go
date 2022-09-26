package models

import (
	"fmt"
	"log"
	"time"

	"github.com/c12s/blackhole/pkg/models/tasks"
	store "github.com/c12s/blackhole/pkg/store/queue_store"
	"github.com/c12s/blackhole/pkg/workers"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type TaskQueueManager struct {
	taskQueues      map[uuid.UUID]*TaskQueue
	workerPool      *workers.Pool
	taskStateUpdate chan *tasks.Task
	shutdownManager chan struct{}
	db              *gorm.DB
}

type TaskQueue struct {
	ID            uuid.UUID
	Name          string
	TokenBucketID uuid.UUID
	TokenBucket   TokenBucket `gorm:"foreignKey:TokenBucketID;references:ID"`
	store         store.QueueStore
}

func InitTaskQueueManager(noOfPoolWorkers int, db *gorm.DB) (*TaskQueueManager, error) {
	taskStateUpdate := make(chan *tasks.Task)
	workerPool, err := workers.Init(noOfPoolWorkers, taskStateUpdate)
	if err != nil {
		return nil, err
	}
	manager := &TaskQueueManager{
		taskQueues:      make(map[uuid.UUID]*TaskQueue),
		workerPool:      workerPool,
		taskStateUpdate: taskStateUpdate,
		shutdownManager: make(chan struct{}),
		db:              db,
	}
	go taskStateUpdatedListener(manager.taskStateUpdate, manager.shutdownManager)
	return manager, nil
}

func (manager *TaskQueueManager) CreateQueue(name string, bucketSize, refreshRate int32) *TaskQueue {
	id := uuid.New()
	tokenBucket := TokenBucket{
		ID:                 uuid.New(),
		NoOfTokens:         int(bucketSize),
		MaxTokens:          int(bucketSize),
		RefreshRate:        time.Duration(refreshRate) * time.Millisecond,
		LastTokenInsertion: time.Now(),
		NotifyWorker:       make(chan bool),
	}
	taskQueue := &TaskQueue{
		ID:            id,
		Name:          name,
		TokenBucketID: tokenBucket.ID,
		TokenBucket:   tokenBucket,
		store:         store.NewInMemQueue(),
	}
	manager.db.Create(taskQueue)
	manager.taskQueues[id] = taskQueue
	return taskQueue
}

func (manager *TaskQueueManager) NewTask(t *tasks.Task) (*tasks.Task, error) {
	if queue, queueExists := manager.taskQueues[t.QueueID]; queueExists {
		t.State = tasks.RECEIVED
		t.NoOfRetries = 0
		t.ID = uuid.New()
		queue.store.Enqueue(t)

		go func(wp *workers.Pool) {
			log.Println("Usap u novu rutinu")
			for {
				if !queue.TokenBucket.TakeToken() {
					<-queue.TokenBucket.NotifyWorker
				} else {
					wp.ExecuteTask(t)
					break
				}
			}
		}(manager.workerPool)

		log.Printf("Returning task with id %s and state %s", t.ID, t.State)
		return t, nil
	}
	return nil, fmt.Errorf("Queue with id %s does not exist", t.QueueID.String())
}

func (manager *TaskQueueManager) Shutdown() {
	manager.workerPool.ShutdownPool()
	close(manager.shutdownManager)
}

func taskStateUpdatedListener(updated <-chan *tasks.Task, shutdown <-chan struct{}) {
	for {
		select {
		case task := <-updated:
			fmt.Printf("Task %s updated to state %d\n", task.ID, task.State)
		case <-shutdown:
			fmt.Printf("Shutting down task state update listener")
			break
		}
	}
}

package workers

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/c12s/blackhole/pkg/models/tasks"
)

type Pool struct {
	noOfWorkers     int
	activeWorkers   int32
	jobsWaiting     int32
	killWorker      chan struct{}
	tasks           chan *tasks.Task
	updateTaskState chan *tasks.Task
}

func Init(noOfWorkers int, updateStateChannel chan *tasks.Task) (*Pool, error) {
	pool := &Pool{
		noOfWorkers:     noOfWorkers,
		activeWorkers:   0,
		jobsWaiting:     0,
		killWorker:      make(chan struct{}),
		tasks:           make(chan *tasks.Task),
		updateTaskState: updateStateChannel,
	}
	//Starting worker routines
	for i := 0; i < noOfWorkers; i++ {
		go pool.worker(i)
	}
	//Giving time for worker goroutines to actually start
	time.Sleep(100 * time.Millisecond)
	return pool, nil
}

func (pool *Pool) ExecuteTask(t *tasks.Task) {
	log.Printf("[ExecuteTask]: Task %s waiting for worker\n", t.ID.String())
	atomic.AddInt32(&pool.jobsWaiting, 1)
	select {
	case pool.tasks <- t:
		atomic.AddInt32(&pool.jobsWaiting, -1)
		break
	case <-pool.killWorker:
		log.Printf("[ExecuteTask]: Shutting down while task %s was waiting for worker.\n", t.ID.String())
		break
	}
}

func (pool *Pool) ShutdownPool() {
	pool.noOfWorkers = 0
	close(pool.killWorker)
}

func (pool *Pool) worker(wID int) {
	log.Printf("[Worker %d]: Starting worker \n", wID)
	for {
		log.Printf("[Worker %d]: Waiting for a task.\n", wID)
		break_from_loop := false
		select {
		case task := <-pool.tasks:
			log.Printf("[Worker %d]: Executing task %s\n", wID, task.ID)
			atomic.AddInt32(&pool.activeWorkers, 1)
			task.State = tasks.PROCESSING
			pool.updateTaskState <- task
			pool.sendTaskToEndpoint(wID, task)
			atomic.AddInt32(&pool.activeWorkers, -1)
			log.Printf("[Worker %d]: Finished with executing %s\n", wID, task.ID)
		case <-pool.killWorker:
			break_from_loop = true
		}
		if break_from_loop {
			break
		}
	}
	log.Printf("[Worker %d]: Shutting down\n", wID)
}

func (pool *Pool) sendTaskToEndpoint(wID int, task *tasks.Task) {
	req, err := createHttpReqFromTask(task)

	if err != nil {
		// No need for retries if url can't be constructed
		task.State = tasks.FAILED
		pool.updateTaskState <- task
	}

	//Create http client with custom timeout for each request
	client := http.Client{
		Timeout: task.Timeout,
	}

	resp, err := client.Do(req)

	if err == nil && resp.StatusCode < 300 {
		log.Printf("[Worker %d]: Task %s finished.", wID, task.ID)
		task.State = tasks.FINISHED
		pool.updateTaskState <- task
		return
	}

	if err != nil {
		log.Printf("[Worker %d]: Task with id %s failed with error %s\n", wID, task.ID.String(), err.Error())
	} else {
		log.Printf("[Worker %d]: Task with id %s failed with response status code %s\n", wID, task.ID.String(), resp.Status)
	}

	if task.NoOfRetries == task.MaxRetries {
		task.State = tasks.FAILED
		pool.updateTaskState <- task
		return
	}

	task.NoOfRetries += 1
	task.State = tasks.RETRY_TIMEOUT
	pool.updateTaskState <- task
	go func(task *tasks.Task) {
		log.Printf("[Worker %d]: Entering retry delay phase for task %s\n", wID, task.ID)
		time.Sleep(task.RetryDelay)
		pool.ExecuteTask(task)
	}(task)
}

func createHttpReqFromTask(task *tasks.Task) (*http.Request, error) {
	url, err := url.Parse(task.WorkerDestination.Url)
	if err != nil {
		return nil, err
	}
	for key, value := range task.WorkerDestination.Params {
		url.Query().Set(key, value)
	}
	req := http.Request{
		Method: task.WorkerDestination.Method,
		URL:    url,
	}
	if req.Method == http.MethodPost {
		req.Body = ioutil.NopCloser(strings.NewReader(task.Payload))
	}
	return &req, nil
}

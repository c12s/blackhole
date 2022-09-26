package workers

import (
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/c12s/blackhole/pkg/models/tasks"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	os.Exit(m.Run())
}
func Test_InitPool(t *testing.T) {
	t.Run("Initalization and Shutdown dont leave workers behind", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		pool, err := Init(3, make(chan *tasks.Task))
		assert.Nil(t, err)
		assert.Equal(t, 3, pool.noOfWorkers)
		assert.Equal(t, int32(0), pool.jobsWaiting)
		assert.Equal(t, int32(0), pool.activeWorkers)

		pool.ShutdownPool()
		time.Sleep(time.Second)
	})
}

func Test_StartingJobs(t *testing.T) {
	t.Run("When there are no free workers, dont start any jobs", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		pool, _ := Init(0, make(chan *tasks.Task))
		defer pool.ShutdownPool()
		task := &tasks.Task{ID: uuid.New(), Name: "test_task"}
		go pool.ExecuteTask(task)
		go pool.ExecuteTask(task)
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, int32(2), pool.jobsWaiting)
		assert.Equal(t, int32(0), pool.activeWorkers)
	})

	t.Run("First 3 jobs should be active, next 2 wait", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		//Following channel wont be buffered, this is just for
		//testing, so that writing to it doesn't become blocking
		pool, _ := Init(2, make(chan *tasks.Task, 6))
		defer pool.ShutdownPool()

		serverExecutinTime := 100 * time.Millisecond
		mockTaskDestinationServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(serverExecutinTime)
			w.WriteHeader(200)
		}))
		defer mockTaskDestinationServer.Close()

		task := &tasks.Task{
			ID: uuid.New(),
			WorkerDestination: tasks.WorkerDestination{
				Url:    mockTaskDestinationServer.URL,
				Method: http.MethodGet,
			},
		}
		go pool.ExecuteTask(task)
		go pool.ExecuteTask(task)
		go pool.ExecuteTask(task)
		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, int32(1), pool.jobsWaiting)
		assert.Equal(t, int32(2), pool.activeWorkers)
		// After this we expect server to finish handling first 3 tasks, and begin next 2
		time.Sleep(serverExecutinTime)
		assert.Equal(t, int32(0), pool.jobsWaiting)
		assert.Equal(t, int32(1), pool.activeWorkers)
	})
}

func Test_Workers(t *testing.T) {
	t.Run("When endpoint returns status 2xx, mark task as finished.", func(t *testing.T) {
		defer goleak.VerifyNone(t)
		updateTaskState := make(chan *tasks.Task)
		// Checking that task states are changed correctly
		go func(t *testing.T, updateTaskState chan *tasks.Task) {
			task := <-updateTaskState
			assert.Equal(t, tasks.PROCESSING, task.State)
			task = <-updateTaskState
			assert.Equal(t, tasks.FINISHED, task.State)
		}(t, updateTaskState)

		pool, _ := Init(1, updateTaskState)
		defer pool.ShutdownPool()

		serverExecutinTime := 10 * time.Millisecond
		mockTaskDestinationServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(serverExecutinTime)
			w.WriteHeader(200)
		}))
		defer mockTaskDestinationServer.Close()

		task := generateTask(mockTaskDestinationServer.URL, http.MethodGet, 1, time.Microsecond)
		go pool.ExecuteTask(task)

		time.Sleep(serverExecutinTime)
	})

	t.Run("Testing retries when enrpoint returns 3xx", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		updateTaskState := make(chan *tasks.Task)
		pool, _ := Init(1, updateTaskState)
		defer pool.ShutdownPool()

		// Not checking statuses here, this is just for non-blocking purposes
		go func(t *testing.T, updateTaskState chan *tasks.Task) {
			// In this example, 4 state changes are expected
			for i := 0; i < 4; i++ {
				<-updateTaskState
			}
		}(t, updateTaskState)

		serverExecutinTime := time.Microsecond
		noOfServerCalls := 0
		mockTaskDestinationServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(serverExecutinTime)
			*&noOfServerCalls++
			w.WriteHeader(300)
		}))
		defer mockTaskDestinationServer.Close()

		task := generateTask(mockTaskDestinationServer.URL, http.MethodGet, 1, time.Microsecond)
		go pool.ExecuteTask(task)

		// Extra 10 milliseconds is to allow for a mock server to respond to request
		time.Sleep(time.Duration(task.MaxRetries)*task.RetryDelay + 10*time.Millisecond)
		assert.Equal(t, task.MaxRetries+1, noOfServerCalls)
	})

	// Check that delay between running the task first and second time is as specified in the task
	t.Run("Retry delays work, and task states change correctly", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		updateTaskState := make(chan *tasks.Task)
		pool, _ := Init(1, updateTaskState)
		defer pool.ShutdownPool()

		// For testing purposes its important that something is reading from this channel,
		// otherwise writing to it would be a blocking operation, which would create deadlocks
		go func(updateTaskState chan *tasks.Task) {
			// In this test 4 state changes are expected
			for i := 0; i < 4; i++ {
				<-updateTaskState
			}
		}(updateTaskState)

		serverExecutinTime := 100 * time.Millisecond
		noOfServerCalls := 0
		mockTaskDestinationServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			*&noOfServerCalls += 1
			time.Sleep(serverExecutinTime)
			if *&noOfServerCalls == 1 {
				w.WriteHeader(500)
			} else {
				w.WriteHeader(200)
			}
		}))
		defer mockTaskDestinationServer.Close()

		retryDelay := 100 * time.Millisecond
		task := generateTask(mockTaskDestinationServer.URL, http.MethodGet, 1, retryDelay)
		go pool.ExecuteTask(task)

		time.Sleep(serverExecutinTime / 2)
		assert.Equal(t, task.State, tasks.PROCESSING)
		time.Sleep(serverExecutinTime)
		assert.Equal(t, task.State, tasks.RETRY_TIMEOUT)
		time.Sleep(retryDelay)
		assert.Equal(t, task.State, tasks.PROCESSING)
		time.Sleep(serverExecutinTime)
		assert.Equal(t, task.State, tasks.FINISHED)
	})

	//Since delay between retries should not block worker, 2 failing tasks
	//should be executed in the same time as one
	t.Run("Retries should not block workers", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		updateTaskState := make(chan *tasks.Task)
		pool, _ := Init(1, updateTaskState)
		defer pool.ShutdownPool()

		go func(updateTaskState chan *tasks.Task) {
			// In this test 8 state changes are expected
			for i := 0; i < 8; i++ {
				t := <-updateTaskState
				log.Printf("[Test]: Task %s has state %s\n", t.ID.String(), t.State.String())
			}
			log.Printf("Finished test routine")
		}(updateTaskState)

		serverExecutinTime := time.Microsecond
		noOfServerCalls := 0
		mockTaskDestinationServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(serverExecutinTime)
			*&noOfServerCalls++
			w.WriteHeader(300)
		}))
		defer mockTaskDestinationServer.Close()

		maxRetries := 1
		retryDelay := 100 * time.Millisecond
		task1 := generateTask(mockTaskDestinationServer.URL, http.MethodGet, maxRetries, retryDelay)
		task2 := generateTask(mockTaskDestinationServer.URL, http.MethodGet, maxRetries, retryDelay)
		go pool.ExecuteTask(task1)
		go pool.ExecuteTask(task2)

		time.Sleep(time.Duration(maxRetries)*retryDelay + 10*time.Millisecond)
		assert.Equal(t, 4, noOfServerCalls)
	})

	t.Run("Task payload is correctly being sent", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		updateTaskState := make(chan *tasks.Task)
		pool, _ := Init(1, updateTaskState)
		defer pool.ShutdownPool()

		go func(updateTaskState chan *tasks.Task) {
			// In this test 2 state changes are expected
			for i := 0; i < 2; i++ {
				<-updateTaskState
			}
		}(updateTaskState)

		type ReqBody struct {
			Field1 string
			Field2 []int
		}

		payload := ReqBody{
			Field1: "test",
			Field2: []int{1, 2, 3},
		}

		mockTaskDestinationServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Println("[Test]: Request received")
			decoder := json.NewDecoder(r.Body)
			var receivedPayload ReqBody
			err := decoder.Decode(&receivedPayload)
			assert.Nil(t, err)
			assert.Equal(t, payload, receivedPayload)
			w.WriteHeader(200)
		}))
		defer mockTaskDestinationServer.Close()

		task := generateTask(mockTaskDestinationServer.URL, http.MethodPost, 0, time.Microsecond)
		stringPayload, _ := json.Marshal(payload)
		task.Payload = string(stringPayload)
		go pool.ExecuteTask(task)

		time.Sleep(time.Millisecond)
	})

	t.Run("Check timeout for task endpoint to response", func(t *testing.T) {
		defer goleak.VerifyNone(t)

		updateTaskState := make(chan *tasks.Task)
		pool, _ := Init(1, updateTaskState)
		defer pool.ShutdownPool()

		go func(updateTaskState chan *tasks.Task) {
			// In this test 2 state changes are expected
			for i := 0; i < 2; i++ {
				<-updateTaskState
			}
		}(updateTaskState)

		serverExecutinTime := time.Second
		mockTaskDestinationServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(serverExecutinTime)
			w.WriteHeader(200)
		}))
		defer mockTaskDestinationServer.Close()

		task := generateTask(mockTaskDestinationServer.URL, http.MethodGet, 0, 0)
		task.Timeout = 100 * time.Millisecond
		go pool.ExecuteTask(task)

		time.Sleep(task.Timeout + 10*time.Millisecond)
		// Server takes one second to respond. Only case where task has
		// FAILED state after the timeout is if timeout ended the connection
		assert.Equal(t, tasks.FAILED, task.State)
	})

}

//
// Helper functions
//

func generateTask(url, method string, maxRetries int, retryDelay time.Duration) *tasks.Task {
	return &tasks.Task{
		ID:         uuid.New(),
		MaxRetries: maxRetries,
		RetryDelay: retryDelay,
		WorkerDestination: tasks.WorkerDestination{
			Url:    url,
			Method: method,
		},
		State: tasks.RECEIVED,
	}

}

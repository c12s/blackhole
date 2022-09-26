package store

import (
	"os"
	"testing"

	"github.com/c12s/blackhole/pkg/models/tasks"
	"github.com/stretchr/testify/assert"
)

var in_mem_q *inMemQueue

func TestMain(m *testing.M) {
	in_mem_q = NewInMemQueue()
	os.Exit(m.Run())
}
func Test_Create(t *testing.T) {
	t.Run("Queue successfully created", func(t *testing.T) {
		assert.Equal(t, 0, len(in_mem_q.queue))
	})
}

func Test_Enqueue(t *testing.T) {
	t.Run("Successful enqueue", func(t *testing.T) {
		err := in_mem_q.Enqueue(&tasks.Task{Name: "test_task"})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(in_mem_q.queue))
		assert.Equal(t, "test_task", in_mem_q.queue[0].Name)
	})

}

func Test_Dequeue(t *testing.T) {
	t.Run("Successful dequeue", func(t *testing.T) {
		in_mem_q.queue = []*tasks.Task{{Name: "test_task"}}
		task, _ := in_mem_q.Dequeue()
		assert.Equal(t, "test_task", task.Name)
		assert.Equal(t, 0, len(in_mem_q.queue))
	})
}

func Test_Size(t *testing.T) {
	t.Run("Successful dequeue", func(t *testing.T) {
		in_mem_q.queue = []*tasks.Task{{Name: "test_task"}}
		size := in_mem_q.Size()
		assert.Equal(t, 1, size)
	})
}

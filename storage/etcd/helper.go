package etcd

import (
	"fmt"
	"strings"
)

const (
	queues = "queues"
	tasks  = "tasks"
)

// queues/queue_name/tasks -> will be stored in db
// queue_name -> user_id:[queue_name|namespace]
// ex: queues/user_123:myqueue1/tasks/ | queues/user_123:default/tasks
func QueueKey(user_id, queueName string) string {
	mid := fmt.Sprintf("%s:%s", user_id, queueName)
	s := []string{queues, mid, tasks}
	return strings.Join(s, "/")
}

// queues/queue_name/tasks/task_group/task_name -> will be stored in db
// queue_name -> user_id:[queue_name|namespace]
// task_group -> user provided name of the task
// task_name -> task_name_id_in_group_timestamp
// ex: queues/user_123:myqueue1/tasks/mytask/mytask_1_1234567890 | queues/user_123:default/tasks/mytask/mytask_1_1234567890
func TaskKey(user_id, queueName, taskName string, timestamp int64, id int) string {
	prefix := TaskGroupKey(user_id, queueName, taskName)
	fTaskName := fmt.Sprintf("%s_%d_%d", taskName, timestamp)
	s := []string{prefix, fTaskName}
	return strings.Join(s, "/")
}

// queues/queue_name/tasks/task_group
// queue_name -> user_id:[queue_name|namespace]
// task_group -> user provided name of the task that holds other task in that group
func TaskGroupKey(user_id, queueName, task_group_name string) string {
	prefix := QueueKey(user_id, queueName)
	s := []string{prefix, task_group_name}
	return strings.Join(s, "/")

}

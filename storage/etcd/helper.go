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
// ex: queues/user_123:myqueue1/tasks | queues/user_123:default/tasks
func QueueKey(user_id, name string) string {
	mid := fmt.Sprintf("%s:%s", user_id, name)
	s := []strings{queues, mid, tasks}
	return strings.Join(s, "/")
}

// queues/queue_name/tasks/task_name -> will be stored in db
// queue_name -> user_id:[queue_name|namespace]
// task_name -> some_user_provided_name_timestamp
// ex: queues/user_123:myqueue1/tasks/task_1_1234567890 | queues/user_123:default/tasks/task_1
func TaskKey(user_id, name, task_name string, timestamp int64) string {
	prefix := QueueKey(user_id, name)
	taskName := fmt.Sprintf("%s_%d", task_name, timestamp)
	s := []strings{prefix, taskName}
	return strings.Join(s, "/")
}

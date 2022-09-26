package tasks

import (
	"time"

	"github.com/google/uuid"
)

type TaskState int

const (
	RECEIVED TaskState = iota
	PROCESSING
	RETRY_TIMEOUT
	FINISHED
	FAILED
	DELETED
)

func (ts TaskState) String() string {
	switch ts {
	case RECEIVED:
		return "RECEIVED"
	case PROCESSING:
		return "PROCESSING"
	case RETRY_TIMEOUT:
		return "RETRY_TIMEOUT"
	case FINISHED:
		return "FINISHED"
	case FAILED:
		return "FAILED"
	case DELETED:
		return "DELETED"
	default:
		return "Unknown state"
	}
}

type WorkerDestination struct {
	Url    string
	Params map[string]string
	Method string
}
type Task struct {
	ID      uuid.UUID
	Name    string
	QueueID uuid.UUID
	// Where should this Task be sent
	WorkerDestination WorkerDestination
	Payload           string
	MaxRetries        int
	NoOfRetries       int
	RetryDelay        time.Duration
	Timeout           time.Duration
	State             TaskState
}

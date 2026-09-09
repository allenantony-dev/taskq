package queue

// Lower runs first. PriorityHigh is a ceiling: nothing outranks it.
const (
	PriorityHigh   = 0
	PriorityNormal = 5
	PriorityLow    = 10
)

type Task struct {
	ID           int64
	Type         string
	Payload      map[string]any
	FencingToken int64
	Attempts     int64
	MaxAttempts  int64
}

// TaskStatus is what Get returns: every field is populated, unlike a Task,
// which only ever describes a job a worker has claimed.
type TaskStatus struct {
	ID          int64
	Type        string
	State       string
	Attempts    int64
	MaxAttempts int64
	LastError   *string
}

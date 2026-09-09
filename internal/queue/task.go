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

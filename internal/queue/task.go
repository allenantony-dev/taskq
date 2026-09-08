package queue

type Task struct {
	ID           int64
	Type         string
	Payload      map[string]any
	FencingToken int64
	Attempts     int64
	MaxAttempts  int64
}

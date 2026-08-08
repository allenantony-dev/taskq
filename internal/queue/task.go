package queue

type Task struct {
	ID      int64
	Type    string
	Payload map[string]any
}

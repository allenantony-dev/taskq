package queue

type Task struct {
	ID      int
	Type    string
	Payload map[string]any
}

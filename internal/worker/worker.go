package worker

import (
	"fmt"
	"taskq/internal/queue"
	"time"
)

type Handler func(queue.Task) error

type Worker struct {
	q        *queue.Queue
	handlers map[string]Handler
}

func NewWorker(q *queue.Queue, handlers map[string]Handler) *Worker {
	return &Worker{
		q:        q,
		handlers: handlers,
	}
}

func (w *Worker) Run() {
	for {
		task, ok := w.q.Dequeue()
		if !ok {
			fmt.Println("Waiting ...")
			time.Sleep(5 * time.Minute)
			continue
		}

		handler, ok := w.handlers[task.Type]
		if !ok {
			fmt.Printf("No handler for task type: %s\n", task.Type)
			continue
		}

		if err := handler(task); err != nil {
			fmt.Printf("Task %d failed: %v\n", task.ID, err)
			continue
		}

		fmt.Printf("Task %d completed\n", task.ID)
	}
}

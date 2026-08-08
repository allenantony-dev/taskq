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
		task, ok, err := w.q.Dequeue()
		if err != nil {
			fmt.Printf("Failed to dequeue task: %v\n", err)
			continue
		}
		if !ok {
			fmt.Println("Waiting ...")
			time.Sleep(2 * time.Minute)
			continue
		}

		fmt.Printf("Dequeued task %d (%s)\n", task.ID, task.Type)

		handler, ok := w.handlers[task.Type]
		if !ok {
			fmt.Printf("No handler for task type: %s\n", task.Type)
			continue
		}

		if err := handler(task); err != nil {
			fmt.Printf("Task %d failed: %v\n", task.ID, err)
			continue
		}

		if err := w.q.Complete(task.ID); err != nil {
			fmt.Printf("Failed to complete task: %d: %v\n", task.ID, err)
		}
		fmt.Printf("Task %d completed\n", task.ID)
	}
}

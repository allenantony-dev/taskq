package worker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"taskq/internal/queue"
	"time"

	"github.com/google/uuid"
)

var backoff = []time.Duration{time.Second, 10 * time.Second, time.Minute, 5 * time.Minute}

// ErrPermanent marks a failure that retrying cannot fix. Handlers wrap it with
// %w to send the task straight to the dead state.
var ErrPermanent = errors.New("permanent failure")

type Handler func(context.Context, queue.Task) error

type Worker struct {
	id       string
	lease    time.Duration
	q        *queue.Queue
	handlers map[string]Handler
}

func NewWorker(q *queue.Queue, handlers map[string]Handler) *Worker {
	return &Worker{
		id:       uuid.NewString(),
		lease:    30 * time.Second,
		q:        q,
		handlers: handlers,
	}
}

func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		task, ok, err := w.q.Dequeue(w.id, w.lease)
		if err != nil {
			fmt.Printf("Failed to dequeue task: %v\n", err)
			continue
		}
		if !ok {
			fmt.Println("Waiting ...")
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
			continue
		}

		fmt.Printf("Dequeued task %d (%s)\n", task.ID, task.Type)

		w.process(ctx, task)
	}
}

func (w *Worker) process(shutdown context.Context, task queue.Task) {
	handler, ok := w.handlers[task.Type]
	if !ok {
		fmt.Printf("No handler for task type: %s\n", task.Type)
		return
	}

	ctx, cancel := context.WithCancel(shutdown)
	defer cancel()

	go w.heartbeat(ctx, cancel, task.ID)

	if err := handler(ctx, task); err != nil {
		if shutdown.Err() != nil {
			released, err := w.q.Release(w.id, task.ID)
			if err != nil {
				fmt.Printf("Failed to release task %d: %v\n", task.ID, err)
				return
			}
			if !released {
				fmt.Printf("Worker %s was evicted from task: %d\n", w.id, task.ID)
				return
			}
			fmt.Printf("Task %d released for another worker\n", task.ID)
			return
		}

		fmt.Printf("Task %d failed: %v\n", task.ID, err)
		w.reschedule(task, err)
		return
	}

	completed, err := w.q.Complete(w.id, task.ID)
	if err != nil {
		fmt.Printf("Failed to complete task: %d: %v\n", task.ID, err)
		return
	}
	if !completed {
		fmt.Printf("Worker %s was evicted from task: %d\n", w.id, task.ID)
		return
	}
	fmt.Printf("Task %d completed\n", task.ID)
}

func (w *Worker) reschedule(task queue.Task, handlerErr error) {
	if errors.Is(handlerErr, ErrPermanent) || task.Attempts >= task.MaxAttempts {
		dead, err := w.q.Dead(w.id, task.ID, handlerErr.Error())
		if err != nil {
			fmt.Printf("Failed to mark task %d dead: %v\n", task.ID, err)
			return
		}
		if !dead {
			fmt.Printf("Worker %s was evicted from task: %d\n", w.id, task.ID)
			return
		}
		fmt.Printf("Task %d dead after %d attempts\n", task.ID, task.Attempts)
		return
	}

	delay := rand.N(backoff[min(int(task.Attempts)-1, len(backoff)-1)])

	retried, err := w.q.Retry(w.id, task.ID, delay, handlerErr.Error())
	if err != nil {
		fmt.Printf("Failed to retry task %d: %v\n", task.ID, err)
		return
	}
	if !retried {
		fmt.Printf("Worker %s was evicted from task: %d\n", w.id, task.ID)
		return
	}
	fmt.Printf("Task %d retrying in %s (attempt %d of %d)\n", task.ID, delay, task.Attempts, task.MaxAttempts)
}

func (w *Worker) heartbeat(ctx context.Context, cancel context.CancelFunc, taskID int64) {
	ticker := time.NewTicker(w.lease / 3)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			renewed, err := w.q.Heartbeat(ctx, taskID, w.id, w.lease)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				fmt.Printf("Failed to renew lease: %v\n", err)
				continue
			}
			if !renewed {
				fmt.Printf("Lease couldn't be renewed for worker %s working on task: %d\n", w.id, taskID)
				cancel()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

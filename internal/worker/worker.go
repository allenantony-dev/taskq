package worker

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"taskq/internal/queue"
	"time"

	"github.com/google/uuid"
)

var backoff = []time.Duration{time.Second, 10 * time.Second, time.Minute, 5 * time.Minute}

// ErrPermanent marks a failure that retrying cannot fix. Handlers wrap it with
// %w to send the task straight to the dead state.
var ErrPermanent = errors.New("permanent failure")

// backoffFor returns the ceiling for this attempt, which the caller jitters
// within. Attempts start at 1, and the last delay repeats past the end.
func backoffFor(attempts int64) time.Duration {
	return backoff[min(int(attempts)-1, len(backoff)-1)]
}

// isFinal reports whether a failure ends the job rather than earning a retry.
func isFinal(task queue.Task, err error) bool {
	return errors.Is(err, ErrPermanent) || task.Attempts >= task.MaxAttempts
}

type Handler func(context.Context, queue.Task) error

type Worker struct {
	id       string
	lease    time.Duration
	q        *queue.Queue
	handlers map[string]Handler
	log      *slog.Logger
}

func NewWorker(q *queue.Queue, handlers map[string]Handler) *Worker {
	id := uuid.NewString()
	return &Worker{
		id:       id,
		lease:    30 * time.Second,
		q:        q,
		handlers: handlers,
		log:      slog.With("worker_id", id),
	}
}

func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		task, ok, err := w.q.Dequeue(ctx, w.id, w.lease)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				w.log.Error("dequeue failed", "err", err)
			}
			continue
		}
		if !ok {
			// Debug: N loops idling would otherwise log this N times every poll.
			w.log.Debug("queue empty")
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
			continue
		}

		w.log.Info("dequeued", "job_id", task.ID, "type", task.Type)

		w.process(ctx, task)
	}
}

func (w *Worker) process(shutdown context.Context, task queue.Task) {
	log := w.log.With("job_id", task.ID)

	handler, ok := w.handlers[task.Type]
	if !ok {
		log.Error("no handler for task type", "type", task.Type)
		return
	}

	ctx, cancel := context.WithCancel(shutdown)
	defer cancel()

	go w.heartbeat(ctx, cancel, task.ID)

	if err := handler(ctx, task); err != nil {
		if shutdown.Err() != nil {
			released, err := w.q.Release(w.id, task.ID)
			if err != nil {
				log.Error("release failed", "err", err)
				return
			}
			if !released {
				log.Warn("evicted")
				return
			}
			log.Info("released for another worker")
			return
		}

		log.Warn("handler failed", "err", err)
		w.reschedule(task, err)
		return
	}

	completed, err := w.q.Complete(w.id, task.ID)
	if err != nil {
		log.Error("complete failed", "err", err)
		return
	}
	if !completed {
		log.Warn("evicted")
		return
	}
	log.Info("completed")
}

func (w *Worker) reschedule(task queue.Task, handlerErr error) {
	log := w.log.With("job_id", task.ID)

	if isFinal(task, handlerErr) {
		dead, err := w.q.Dead(w.id, task.ID, handlerErr.Error())
		if err != nil {
			log.Error("marking dead failed", "err", err)
			return
		}
		if !dead {
			log.Warn("evicted")
			return
		}
		log.Error("dead", "attempts", task.Attempts)
		return
	}

	delay := rand.N(backoffFor(task.Attempts))

	retried, err := w.q.Retry(w.id, task.ID, delay, handlerErr.Error())
	if err != nil {
		log.Error("retry failed", "err", err)
		return
	}
	if !retried {
		log.Warn("evicted")
		return
	}
	log.Info("retrying", "delay", delay, "attempt", task.Attempts, "max_attempts", task.MaxAttempts)
}

func (w *Worker) heartbeat(ctx context.Context, cancel context.CancelFunc, taskID int64) {
	log := w.log.With("job_id", taskID)

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
				log.Error("heartbeat failed", "err", err)
				continue
			}
			if !renewed {
				log.Warn("lease lost")
				cancel()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Queue struct {
	db *pgxpool.Pool
}

func NewQueue(pool *pgxpool.Pool) *Queue {
	return &Queue{
		db: pool,
	}
}

func (q *Queue) Enqueue(taskType string, payload map[string]any) (int64, error) {
	id, _, err := q.EnqueueAt(taskType, payload, time.Now(), "")
	return id, err
}

// EnqueueAt schedules a task for a given time. A non-empty key deduplicates:
// the insert is skipped and false returned if a job already holds that key.
func (q *Queue) EnqueueAt(taskType string, payload map[string]any, at time.Time, key string) (int64, bool, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return 0, false, err
	}

	var taskID int64

	err = q.db.QueryRow(
		context.Background(),
		`
		INSERT INTO jobs (type, payload, state, available_at, idempotency_key)
		VALUES ($1, $2, 'pending', $3, NULLIF($4, ''))
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id
		`,
		taskType,
		encoded,
		at,
		key,
	).Scan(&taskID)

	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	return taskID, true, nil
}

func (q *Queue) Dequeue(workerID string, leaseDuration time.Duration) (Task, bool, error) {
	var task Task
	var payload []byte

	tx, err := q.db.Begin(context.Background())
	if err != nil {
		return Task{}, false, err
	}
	defer tx.Rollback(context.Background())

	err = tx.QueryRow(
		context.Background(),
		`
		SELECT id, type, payload, max_attempts
		FROM jobs
		WHERE state = 'pending'
			AND NOW() >= available_at
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED;
		`,
	).Scan(
		&task.ID,
		&task.Type,
		&payload,
		&task.MaxAttempts,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			fmt.Println("Queue empty")
			return Task{}, false, nil
		}
		return Task{}, false, err
	}

	if err := json.Unmarshal(payload, &task.Payload); err != nil {
		return Task{}, false, err
	}

	leaseExpiry := time.Now().Add(leaseDuration)

	err = tx.QueryRow(
		context.Background(),
		`
		UPDATE jobs
		SET state = 'running',
			current_worker = $2,
			lease_expiry = $3,
			fencing_token = fencing_token + 1,
			attempts = attempts + 1
		WHERE id = $1
		RETURNING fencing_token, attempts;
		`,
		task.ID,
		workerID,
		leaseExpiry,
	).Scan(&task.FencingToken, &task.Attempts)
	if err != nil {
		return Task{}, false, err
	}

	if err := tx.Commit(context.Background()); err != nil {
		return Task{}, false, err
	}

	return task, true, nil
}

func (q *Queue) Complete(workerID string, taskID int64) (bool, error) {
	result, err := q.db.Exec(
		context.Background(),
		`
		UPDATE jobs
		SET state = 'done',
		    current_worker = NULL,
			lease_expiry = NULL
		WHERE id = $1
			AND current_worker = $2;
		`,
		taskID,
		workerID,
	)

	if err != nil {
		return false, err
	}

	return result.RowsAffected() == 1, nil
}

func (q *Queue) Retry(workerID string, taskID int64, delay time.Duration, lastError string) (bool, error) {
	result, err := q.db.Exec(
		context.Background(),
		`
		UPDATE jobs
		SET state = 'pending',
			current_worker = NULL,
			lease_expiry = NULL,
			available_at = NOW() + $3,
			last_error = $4
		WHERE id = $1
			AND current_worker = $2;
		`,
		taskID,
		workerID,
		delay,
		lastError,
	)
	if err != nil {
		return false, err
	}

	return result.RowsAffected() == 1, nil
}

func (q *Queue) Dead(workerID string, taskID int64, lastError string) (bool, error) {
	result, err := q.db.Exec(
		context.Background(),
		`
		UPDATE jobs
		SET state = 'dead',
			current_worker = NULL,
			lease_expiry = NULL,
			last_error = $3
		WHERE id = $1
			AND current_worker = $2;
		`,
		taskID,
		workerID,
		lastError,
	)
	if err != nil {
		return false, err
	}

	return result.RowsAffected() == 1, nil
}

func (q *Queue) ReapExpired() (int64, error) {
	result, err := q.db.Exec(
		context.Background(),
		`
		UPDATE jobs
		SET state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
			current_worker = NULL,
			lease_expiry = NULL,
			last_error = 'lease expired'
		WHERE state = 'running'
			AND lease_expiry < NOW();
		`,
	)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected(), nil
}

func (q *Queue) Heartbeat(ctx context.Context, taskID int64, workerID string, leaseExtension time.Duration) (bool, error) {
	extension := time.Now().Add(leaseExtension)

	result, err := q.db.Exec(
		ctx,
		`
		UPDATE jobs
		SET lease_expiry = $1
		WHERE
			id = $2 AND current_worker = $3
		`,
		extension,
		taskID,
		workerID,
	)
	if err != nil {
		return false, err
	}

	return result.RowsAffected() == 1, nil
}

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

// The completion writes below are deliberately not cancellable: they record
// work that already happened, so abandoning one would lose the record and let
// the job run again. This timeout exists only so a hung database cannot block
// a worker indefinitely.
const completionTimeout = 5 * time.Second

type Queue struct {
	db *pgxpool.Pool
}

func NewQueue(pool *pgxpool.Pool) *Queue {
	return &Queue{
		db: pool,
	}
}

func (q *Queue) Enqueue(ctx context.Context, taskType string, payload map[string]any) (int64, error) {
	id, _, err := q.EnqueueAt(ctx, taskType, payload, time.Now(), "", PriorityNormal)
	return id, err
}

// ErrKeyReused means an idempotency key was sent with different content than
// the job that already holds it. That is a caller mistake, not a retry.
var ErrKeyReused = errors.New("idempotency key reused with different content")

// EnqueueAt schedules a task for a given time. A non-empty key deduplicates:
// if a job already holds that key, its id is returned with false and nothing
// is inserted.
func (q *Queue) EnqueueAt(ctx context.Context, taskType string, payload map[string]any, at time.Time, key string, priority int) (int64, bool, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return 0, false, err
	}

	var taskID int64

	err = q.db.QueryRow(
		ctx,
		`
		INSERT INTO jobs (type, payload, state, available_at, idempotency_key, priority)
		VALUES ($1, $2, 'pending', $3, NULLIF($4, ''), $5)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id
		`,
		taskType,
		encoded,
		at,
		key,
		priority,
	).Scan(&taskID)

	if errors.Is(err, pgx.ErrNoRows) {
		var sameContent bool

		// Compared in Postgres so jsonb equality handles key order and
		// whitespace; the stored bytes never match a fresh json.Marshal.
		err = q.db.QueryRow(
			ctx,
			`
			SELECT id, type = $2 AND payload = $3
			FROM jobs
			WHERE idempotency_key = $1
			`,
			key,
			taskType,
			encoded,
		).Scan(&taskID, &sameContent)
		if err != nil {
			return 0, false, err
		}
		if !sameContent {
			return 0, false, ErrKeyReused
		}

		return taskID, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	return taskID, true, nil
}

func (q *Queue) Get(ctx context.Context, taskID int64) (TaskStatus, bool, error) {
	var status TaskStatus

	err := q.db.QueryRow(
		ctx,
		`
		SELECT id, type, state, attempts, max_attempts, last_error
		FROM jobs
		WHERE id = $1
		`,
		taskID,
	).Scan(
		&status.ID,
		&status.Type,
		&status.State,
		&status.Attempts,
		&status.MaxAttempts,
		&status.LastError,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return TaskStatus{}, false, nil
	}
	if err != nil {
		return TaskStatus{}, false, err
	}

	return status, true, nil
}

func (q *Queue) Dequeue(ctx context.Context, workerID string, leaseDuration time.Duration) (Task, bool, error) {
	var task Task
	var payload []byte

	tx, err := q.db.Begin(ctx)
	if err != nil {
		return Task{}, false, err
	}
	// Rolling back is cleanup, not work: on a cancelled ctx pgx cannot send it
	// and discards the connection instead of returning it to the pool.
	defer tx.Rollback(context.Background())

	err = tx.QueryRow(
		ctx,
		`
		SELECT id, type, payload, max_attempts
		FROM jobs
		WHERE state = 'pending'
			AND NOW() >= available_at
		ORDER BY priority, id
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
		ctx,
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

	if err := tx.Commit(ctx); err != nil {
		return Task{}, false, err
	}

	return task, true, nil
}

func (q *Queue) Complete(workerID string, taskID int64) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), completionTimeout)
	defer cancel()

	result, err := q.db.Exec(
		ctx,
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
	ctx, cancel := context.WithTimeout(context.Background(), completionTimeout)
	defer cancel()

	result, err := q.db.Exec(
		ctx,
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
	ctx, cancel := context.WithTimeout(context.Background(), completionTimeout)
	defer cancel()

	result, err := q.db.Exec(
		ctx,
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

// Release hands a claimed job back on shutdown. attempts is deliberately not
// decremented, and available_at is not pushed out.
func (q *Queue) Release(workerID string, taskID int64) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), completionTimeout)
	defer cancel()

	result, err := q.db.Exec(
		ctx,
		`
		UPDATE jobs
		SET state = 'pending',
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

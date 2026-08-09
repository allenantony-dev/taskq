package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type Queue struct {
	db *pgx.Conn
}

func NewQueue(conn *pgx.Conn) *Queue {
	return &Queue{
		db: conn,
	}
}

func (q *Queue) Enqueue(task Task) (int64, error) {
	payload, err := json.Marshal(task.Payload)
	if err != nil {
		return 0, err
	}

	var id int64

	err = q.db.QueryRow(
		context.Background(),
		`
		INSERT INTO jobs (type, payload, state)
		VALUES ($1, $2, 'pending')
		RETURNING id
		`,
		task.Type,
		payload,
	).Scan(&id)

	if err != nil {
		return 0, err
	}

	return id, nil
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
		SELECT id, type, payload
		FROM jobs
		WHERE state = 'pending'
		ORDER BY id
		LIMIT 1
		FOR UPDATE SKIP LOCKED;
		`,
	).Scan(
		&task.ID,
		&task.Type,
		&payload,
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

	_, err = tx.Exec(
		context.Background(),
		`
		UPDATE jobs
		SET state = 'running',
			current_worker = $2,
			lease_expiry = $3
		WHERE id = $1;
		`,
		task.ID,
		workerID,
		leaseExpiry,
	)
	if err != nil {
		return Task{}, false, err
	}

	if err := tx.Commit(context.Background()); err != nil {
		return Task{}, false, err
	}

	return task, true, nil
}

func (q *Queue) Complete(id int64) error {
	_, err := q.db.Exec(
		context.Background(),
		`
		UPDATE jobs
		SET state = 'done',
		    current_worker = NULL,
			lease_expiry = NULL
		WHERE id = $1
		`,
		id,
	)

	return err
}

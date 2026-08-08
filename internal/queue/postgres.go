package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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

func (q *Queue) Dequeue() (Task, bool, error) {
	var task Task
	var payload []byte

	err := q.db.QueryRow(
		context.Background(),
		`UPDATE jobs
		SET state = 'running'
		WHERE id = (
			SELECT id
			FROM jobs
			WHERE state = 'pending'
			ORDER BY id
			LIMIT 1
		)
		RETURNING id, type, payload;`,
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

	return task, true, nil
}

func (q *Queue) Complete(id int64) error {
	_, err := q.db.Exec(
		context.Background(),
		`
		UPDATE jobs
		SET state = 'done'
		WHERE id = $1
		`,
		id,
	)

	return err
}

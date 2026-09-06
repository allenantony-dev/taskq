package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"taskq/internal/queue"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	q := queue.NewQueue(pool)

	tasks := []queue.Task{
		{
			Type:    "email",
			Payload: map[string]any{"userID": 123},
		},
		{
			Type:    "image",
			Payload: map[string]any{"imageID": 123},
		},
		{
			Type:    "report",
			Payload: map[string]any{"reportID": 123},
		},
	}

	for range 10 {
		for _, task := range tasks {
			id, err := q.Enqueue(task)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Enqueued task: #%d\n", id)
		}
	}
}

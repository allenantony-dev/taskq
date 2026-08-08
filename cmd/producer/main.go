package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"taskq/internal/queue"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	q := queue.NewQueue(conn)

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

	for _, task := range tasks {
		id, err := q.Enqueue(task)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Enqueued task: #%d\n", id)
	}
}

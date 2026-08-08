package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"taskq/internal/queue"
	"taskq/internal/worker"

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

	handlers := map[string]worker.Handler{
		"email": func(task queue.Task) error {
			fmt.Println("Sending email ...")
			return nil
		},
		"image": func(task queue.Task) error {
			fmt.Println("Resizing image ...")
			return nil
		},
		"report": func(task queue.Task) error {
			fmt.Println("Generating report ...")
			return nil
		},
	}

	w := worker.NewWorker(q, handlers)
	w.Run()

}

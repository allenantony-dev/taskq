package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"taskq/internal/queue"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()

	dbUrl := os.Getenv("DATABASE_URL")
	if dbUrl == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	conn, err := pgx.Connect(ctx, dbUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	q := queue.NewQueue(conn)

	for {
		recovered, err := q.ReapExpired()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Reaper: recovered %d tasks\n", recovered)
		time.Sleep(5 * time.Second)
	}
}

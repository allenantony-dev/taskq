package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"taskq/internal/queue"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()

	dbUrl := os.Getenv("DATABASE_URL")
	if dbUrl == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	pool, err := pgxpool.New(ctx, dbUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	q := queue.NewQueue(pool)

	for {
		recovered, err := q.ReapExpired()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Reaper: recovered %d tasks\n", recovered)
		time.Sleep(5 * time.Second)
	}
}

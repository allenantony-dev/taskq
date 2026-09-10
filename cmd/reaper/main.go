package main

import (
	"context"
	"log"
	"log/slog"
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

		slog.Info("reaped", "count", recovered)
		time.Sleep(5 * time.Second)
	}
}

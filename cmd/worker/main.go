package main

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"taskq/internal/queue"
	"taskq/internal/worker"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	if v := os.Getenv("LOG_LEVEL"); v != "" {
		var level slog.Level
		if err := level.UnmarshalText([]byte(v)); err != nil {
			log.Fatal(err)
		}
		slog.SetLogLoggerLevel(level)
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL is not set")
	}

	reportsURL := os.Getenv("REPORTS_DATABASE_URL")
	if reportsURL == "" {
		log.Fatal("REPORTS_DATABASE_URL is not set")
	}

	reportDelay, err := time.ParseDuration(cmp.Or(os.Getenv("REPORT_DELAY"), "0s"))
	if err != nil {
		log.Fatal(err)
	}

	concurrency, err := strconv.Atoi(cmp.Or(os.Getenv("WORKER_CONCURRENCY"), "1"))
	if err != nil {
		log.Fatal(err)
	}
	if concurrency < 1 {
		log.Fatal("WORKER_CONCURRENCY must be at least 1")
	}

	queuePool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer queuePool.Close()

	reportsPool, err := pgxpool.New(ctx, reportsURL)
	if err != nil {
		log.Fatal(err)
	}
	defer reportsPool.Close()

	q := queue.NewQueue(queuePool)

	handlers := map[string]worker.Handler{
		"email": func(ctx context.Context, task queue.Task) error {
			slog.Info("sending email", "job_id", task.ID)
			return nil
		},
		"image": func(ctx context.Context, task queue.Task) error {
			slog.Info("resizing image", "job_id", task.ID)
			return nil
		},
		"report": func(ctx context.Context, task queue.Task) error {
			slog.Info("generating report", "job_id", task.ID, "fencing_token", task.FencingToken)

			select {
			case <-time.After(reportDelay):
			case <-ctx.Done():
				return ctx.Err()
			}

			result, err := reportsPool.Exec(
				ctx,
				`
				INSERT INTO reports (job_id, content, fencing_token)
				VALUES ($1, $2, $3)
				ON CONFLICT (job_id) DO UPDATE
				SET content = EXCLUDED.content,
					fencing_token = EXCLUDED.fencing_token
				WHERE reports.fencing_token < EXCLUDED.fencing_token
				`,
				task.ID,
				fmt.Sprintf("report for job %d", task.ID),
				task.FencingToken,
			)
			if err != nil {
				return err
			}

			if result.RowsAffected() == 0 {
				return fmt.Errorf("write rejected, stale token %d", task.FencingToken)
			}

			slog.Info("report written", "job_id", task.ID, "fencing_token", task.FencingToken)
			return nil
		},
	}

	slog.Info("starting", "concurrency", concurrency)

	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each loop gets its own worker id, so current_worker names one
			// unit of execution rather than the whole process.
			worker.NewWorker(q, handlers).Run(ctx)
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	<-ctx.Done()
	// Unregister so a second signal kills the process instead of being swallowed.
	stop()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		// Past the lease the reaper can reclaim the job anyway, so waiting
		// longer buys nothing. Exit hard: a handler ignoring its context may
		// still be holding a pool connection that Close would wait on.
		slog.Error("shutdown deadline exceeded, abandoning in-flight jobs")
		os.Exit(1)
	}
}

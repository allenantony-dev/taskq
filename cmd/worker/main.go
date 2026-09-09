package main

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"taskq/internal/queue"
	"taskq/internal/worker"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

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
			fmt.Println("Sending email ...")
			return nil
		},
		"image": func(ctx context.Context, task queue.Task) error {
			fmt.Println("Resizing image ...")
			return nil
		},
		"report": func(ctx context.Context, task queue.Task) error {
			fmt.Printf("Generating report ... (fencing token %d)\n", task.FencingToken)

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

			fmt.Printf("task %d: report written, token %d\n", task.ID, task.FencingToken)
			return nil
		},
	}

	w := worker.NewWorker(q, handlers)

	done := make(chan struct{})
	go func() {
		w.Run(ctx)
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
		fmt.Println("Shutdown deadline exceeded, abandoning in-flight job")
		os.Exit(1)
	}
}

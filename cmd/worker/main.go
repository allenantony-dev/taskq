package main

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"os"
	"taskq/internal/queue"
	"taskq/internal/worker"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()

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
		"email": func(task queue.Task) error {
			fmt.Println("Sending email ...")
			return nil
		},
		"image": func(task queue.Task) error {
			fmt.Println("Resizing image ...")
			return nil
		},
		"report": func(task queue.Task) error {
			fmt.Printf("Generating report ... (fencing token %d)\n", task.FencingToken)
			time.Sleep(reportDelay)

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
	w.Run()

}

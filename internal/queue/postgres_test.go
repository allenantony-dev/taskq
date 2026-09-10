package queue

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		os.Exit(m.Run()) // every test skips itself
	}

	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		panic(err)
	}
	testPool = pool

	code := m.Run()
	pool.Close()
	os.Exit(code)
}

// newTestQueue empties the jobs table so each test starts from nothing.
func newTestQueue(t *testing.T) *Queue {
	t.Helper()
	if testPool == nil {
		t.Skip("TEST_DATABASE_URL not set")
	}
	if _, err := testPool.Exec(context.Background(), "DELETE FROM jobs"); err != nil {
		t.Fatal(err)
	}
	return NewQueue(testPool)
}

func claim(t *testing.T, q *Queue, workerID string) Task {
	t.Helper()
	ctx := context.Background()
	if _, err := q.Enqueue(ctx, "test", map[string]any{"n": 1}); err != nil {
		t.Fatal(err)
	}
	task, ok, err := q.Dequeue(ctx, workerID, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("nothing to dequeue")
	}
	return task
}

func state(t *testing.T, q *Queue, taskID int64) string {
	t.Helper()
	status, ok, err := q.Get(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("job %d not found", taskID)
	}
	return status.State
}

// Every write to a claimed job must refuse a worker that no longer owns it.
// This is what stops a thawed zombie from finishing someone else's job.
func TestWritesRejectNonOwner(t *testing.T) {
	ctx := context.Background()

	ops := map[string]func(*Queue, int64) (bool, error){
		"Complete":  func(q *Queue, id int64) (bool, error) { return q.Complete("intruder", id) },
		"Retry":     func(q *Queue, id int64) (bool, error) { return q.Retry("intruder", id, time.Second, "boom") },
		"Dead":      func(q *Queue, id int64) (bool, error) { return q.Dead("intruder", id, "boom") },
		"Release":   func(q *Queue, id int64) (bool, error) { return q.Release("intruder", id) },
		"Heartbeat": func(q *Queue, id int64) (bool, error) { return q.Heartbeat(ctx, id, "intruder", time.Minute) },
	}

	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			q := newTestQueue(t)
			task := claim(t, q, "owner")

			ok, err := op(q, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Error("reported success for a worker that does not own the job")
			}
			if got := state(t, q, task.ID); got != "running" {
				t.Errorf("state = %q, want running: a non-owner changed the row", got)
			}
		})
	}
}

func TestOwnerCanComplete(t *testing.T) {
	q := newTestQueue(t)
	task := claim(t, q, "owner")

	ok, err := q.Complete("owner", task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("owner could not complete its own job")
	}
	if got := state(t, q, task.ID); got != "done" {
		t.Errorf("state = %q, want done", got)
	}
}

// A row already locked by another transaction must be stepped over, not waited
// on, so one slow claim cannot stall every other worker.
func TestDequeueSkipsLockedRows(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	first, err := q.Enqueue(ctx, "test", map[string]any{"n": 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := q.Enqueue(ctx, "test", map[string]any{"n": 2})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SELECT id FROM jobs WHERE id = $1 FOR UPDATE", first); err != nil {
		t.Fatal(err)
	}

	task, ok, err := q.Dequeue(ctx, "worker", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Without SKIP LOCKED this call blocks on the held lock and the test hangs
	// rather than reaching either assertion below.
	if !ok {
		t.Fatal("found no claimable row")
	}
	if task.ID != second {
		t.Errorf("claimed job %d, want %d (the unlocked one)", task.ID, second)
	}
}

func TestDequeueWaitsForAvailableAt(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	id, _, err := q.EnqueueAt(ctx, "test", map[string]any{}, time.Now().Add(time.Hour), "", PriorityNormal)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok, err := q.Dequeue(ctx, "worker", 30*time.Second); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("claimed a job that is not due yet")
	}

	if _, err := testPool.Exec(ctx, "UPDATE jobs SET available_at = NOW() WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := q.Dequeue(ctx, "worker", 30*time.Second); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("did not claim a job that is due")
	}
}

func TestDequeueTakesHighestPriorityFirst(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	// Enqueued least-urgent first, so insertion order disagrees with priority.
	low, _, err := q.EnqueueAt(ctx, "test", map[string]any{}, time.Now(), "", PriorityLow)
	if err != nil {
		t.Fatal(err)
	}
	high, _, err := q.EnqueueAt(ctx, "test", map[string]any{}, time.Now(), "", PriorityHigh)
	if err != nil {
		t.Fatal(err)
	}

	task, _, err := q.Dequeue(ctx, "worker", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if task.ID != high {
		t.Errorf("claimed job %d, want %d: priority did not beat insertion order (low was %d)", task.ID, high, low)
	}
}

func TestClaimIncrementsAttemptsAndFencingToken(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	task := claim(t, q, "first")
	if task.Attempts != 1 || task.FencingToken != 1 {
		t.Fatalf("first claim: attempts = %d, token = %d, want 1 and 1", task.Attempts, task.FencingToken)
	}

	if ok, err := q.Release("first", task.ID); err != nil || !ok {
		t.Fatalf("release: ok = %v, err = %v", ok, err)
	}

	again, _, err := q.Dequeue(ctx, "second", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if again.Attempts != 2 || again.FencingToken != 2 {
		t.Errorf("second claim: attempts = %d, token = %d, want 2 and 2", again.Attempts, again.FencingToken)
	}
}

// A job whose worker keeps dying must not cycle forever: the reaper is the only
// thing that sees those failures, so it has to apply max_attempts itself.
func TestReapExpiredKillsExhaustedJobs(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	var live, exhausted int64
	err := testPool.QueryRow(ctx, `
		INSERT INTO jobs (type, payload, state, current_worker, lease_expiry, attempts, max_attempts)
		VALUES ('test', '{}', 'running', 'ghost', NOW() - interval '1 minute', 2, 5)
		RETURNING id`).Scan(&live)
	if err != nil {
		t.Fatal(err)
	}
	err = testPool.QueryRow(ctx, `
		INSERT INTO jobs (type, payload, state, current_worker, lease_expiry, attempts, max_attempts)
		VALUES ('test', '{}', 'running', 'ghost', NOW() - interval '1 minute', 5, 5)
		RETURNING id`).Scan(&exhausted)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := q.ReapExpired(); err != nil {
		t.Fatal(err)
	}

	if got := state(t, q, live); got != "pending" {
		t.Errorf("job with attempts left: state = %q, want pending", got)
	}
	if got := state(t, q, exhausted); got != "dead" {
		t.Errorf("job past max_attempts: state = %q, want dead", got)
	}
}

// A retry must be held back, otherwise the delay is whatever the lease happens
// to be and the backoff schedule does nothing.
func TestRetryDelaysAndRecordsTheError(t *testing.T) {
	q := newTestQueue(t)
	task := claim(t, q, "owner")

	if ok, err := q.Retry("owner", task.ID, time.Minute, "upstream down"); err != nil || !ok {
		t.Fatalf("retry: ok = %v, err = %v", ok, err)
	}

	var seconds float64
	err := testPool.QueryRow(context.Background(),
		"SELECT EXTRACT(EPOCH FROM available_at - NOW()) FROM jobs WHERE id = $1",
		task.ID).Scan(&seconds)
	if err != nil {
		t.Fatal(err)
	}
	if seconds <= 0 {
		t.Errorf("available_at is %.1fs in the past, so the retry is eligible immediately", -seconds)
	}

	status, _, err := q.Get(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status.LastError == nil || *status.LastError != "upstream down" {
		t.Errorf("last_error = %v, want \"upstream down\"", status.LastError)
	}
}

// Shutdown is not a failure: the job goes back immediately, and the attempt it
// used is not refunded, so restarts cannot let a job cycle forever.
func TestReleaseIsImmediateAndKeepsAttempts(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()
	task := claim(t, q, "owner")

	if ok, err := q.Release("owner", task.ID); err != nil || !ok {
		t.Fatalf("release: ok = %v, err = %v", ok, err)
	}

	status, _, err := q.Get(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Attempts != task.Attempts {
		t.Errorf("attempts = %d, want %d: releasing must not refund the attempt", status.Attempts, task.Attempts)
	}

	if _, ok, err := q.Dequeue(ctx, "other", time.Minute); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Error("released job was not immediately claimable")
	}
}

func TestEnqueueAtDeduplicatesByKey(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()
	payload := map[string]any{"to": "alice"}

	first, created, err := q.EnqueueAt(ctx, "email", payload, time.Now(), "digest-1", PriorityNormal)
	if err != nil || !created {
		t.Fatalf("first enqueue: created = %v, err = %v", created, err)
	}

	second, created, err := q.EnqueueAt(ctx, "email", payload, time.Now(), "digest-1", PriorityNormal)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Error("created a second job for a key already in use")
	}
	if second != first {
		t.Errorf("returned id %d, want %d: a retry must be able to find the original job", second, first)
	}
}

// jsonb equality, not byte equality: the stored payload never matches a fresh
// json.Marshal, which differs in key order and whitespace.
func TestEnqueueAtIgnoresJSONFormatting(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	first, _, err := q.EnqueueAt(ctx, "email", map[string]any{"bb": 1, "a": 2}, time.Now(), "k", PriorityNormal)
	if err != nil {
		t.Fatal(err)
	}

	// Same content, different key order, and 2 written as a float.
	second, created, err := q.EnqueueAt(ctx, "email", map[string]any{"a": 2.0, "bb": 1}, time.Now(), "k", PriorityNormal)
	if err != nil {
		t.Fatalf("same content was treated as a conflict: %v", err)
	}
	if created || second != first {
		t.Errorf("created = %v, id = %d, want false and %d", created, second, first)
	}
}

func TestEnqueueAtRejectsReusedKey(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	if _, _, err := q.EnqueueAt(ctx, "email", map[string]any{"to": "alice"}, time.Now(), "k", PriorityNormal); err != nil {
		t.Fatal(err)
	}

	_, _, err := q.EnqueueAt(ctx, "email", map[string]any{"to": "bob"}, time.Now(), "k", PriorityNormal)
	if !errors.Is(err, ErrKeyReused) {
		t.Fatalf("err = %v, want ErrKeyReused: bob was silently dropped", err)
	}
}

// The key column is UNIQUE and "" is not NULL, so a plain empty string would
// make the second keyless enqueue fail forever.
func TestKeylessEnqueuesDoNotCollide(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	for i := range 3 {
		if _, err := q.Enqueue(ctx, "test", map[string]any{"n": i}); err != nil {
			t.Fatalf("keyless enqueue %d failed: %v", i+1, err)
		}
	}
}

func TestGetReportsMissingJob(t *testing.T) {
	q := newTestQueue(t)

	_, ok, err := q.Get(context.Background(), 999999)
	if err != nil {
		t.Fatalf("missing job returned an error rather than not-found: %v", err)
	}
	if ok {
		t.Error("reported a job that does not exist")
	}
}

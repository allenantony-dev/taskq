# taskq

A background job queue built on Postgres, written in Go. I built it to learn how job queues actually work.

The basic idea is simple. Each job is a row in a table, and worker processes take a job, run it, and mark it done. The hard part is everything that can go wrong in between, when workers crash, freeze, or fail halfway through.

For example, a worker takes a job and then freezes mid-run. The queue can't tell a frozen worker from a dead one, so it hands the job to another worker, which finishes it. Then the first worker unfreezes, finishes too, and overwrites the newer result with its stale one.

Each section below is one failure like that and its fix, in the order I fixed them, with a link to the commit. The frozen worker is [section 4](#a-frozen-worker-wakes-up).

**The failures, in order:**

1. [Multiple workers claiming the same job](#multiple-workers-claiming-the-same-job)
2. [A worker crashes holding a job](#a-worker-crashes-holding-a-job)
3. [A job outlives its lease](#a-job-outlives-its-lease)
4. [A frozen worker wakes up](#a-frozen-worker-wakes-up)
5. [A handler fails](#a-handler-fails)
6. [Running a job later](#running-a-job-later)
7. [Urgent work stuck behind trivial work](#urgent-work-stuck-behind-trivial-work)
8. [Shutting a worker down](#shutting-a-worker-down)
9. [Which calls should be cancellable](#which-calls-should-be-cancellable)

## How it works

A job is a row in `jobs`. It moves through four states:

```
pending ──claim──▶ running ──success──▶ done
   ▲                  │
   │                  ├──failure, attempts left──▶ pending (later)
   └──lease expired───┘
                      └──failure, attempts exhausted──▶ dead
```

Three processes:

| Process | What it does |
|---|---|
| `cmd/worker` | Claims jobs, runs handlers, renews leases, reports results |
| `cmd/reaper` | Recovers jobs whose worker died |
| `cmd/api` | HTTP endpoints to enqueue and check jobs |

Plus `cmd/producer`, a small script that enqueues sample jobs.

## The design, problem by problem

### Multiple workers claiming the same job

[`7336d4e`](https://github.com/allenantony-dev/taskq/commit/7336d4e)

Two workers running `SELECT ... WHERE state = 'pending'` at the same time both get the same row. The claim runs in a transaction with `FOR UPDATE SKIP LOCKED`, so a row being claimed by one worker is invisible to the others rather than blocking them.

### A worker crashes holding a job

[`ecb5331`](https://github.com/allenantony-dev/taskq/commit/ecb5331) · [`565bf95`](https://github.com/allenantony-dev/taskq/commit/565bf95)

The job stays `running` forever. Each claim sets `lease_expiry`, and the reaper resets any job whose lease has passed back to `pending`.

### A job outlives its lease

[`01d4a1e`](https://github.com/allenantony-dev/taskq/commit/01d4a1e) · [`9e12b3c`](https://github.com/allenantony-dev/taskq/commit/9e12b3c)

A lease is a *prediction*: "this should be done in 30s". The reaper was reading it as an *observation*: "this worker is alive". A 45-second job on a 30-second lease got reaped and run twice while the first run was still going.

A goroutine now extends the lease every 10 seconds while the handler runs. Recovery time is bounded by the **lease duration**, not the job duration. A three-hour job and a two-second job both recover in ~30s after a crash.

`lease = tick × 3`, so two consecutive failed heartbeats are absorbed and the third gives up.

> I kept a single `pgx.Conn` here longer than I should have, because I couldn't see what was wrong with it. What convinced me was `failed to deallocate cached statement(s): conn closed`. The heartbeat goroutine and the main loop had desynced the protocol, pgx closed the connection, and `pgx.Conn` doesn't reconnect. One collision bricked the worker for the life of the process. Hence `pgxpool`.

### A frozen worker wakes up

[`1815845`](https://github.com/allenantony-dev/taskq/commit/1815845)

A worker frozen by a suspended VM, a CPU-throttled container, or a laptop going to sleep stops heartbeating, gets reaped, and its job is claimed by someone else. Then it wakes up and finishes, writing stale results over newer ones.

Two defences:

**Inside the queue**, every write to a claimed job carries `AND current_worker = $x` and checks rows affected. An evicted worker cannot mark a job done, retry it, or extend its lease.

**Outside the queue**, each claim increments a `fencing_token`. Handlers pass it to whatever they write to, and that system rejects anything carrying a lower token than it has already seen. The `report` handler does this with an upsert:

```sql
INSERT INTO reports (job_id, content, fencing_token)
VALUES ($1, $2, $3)
ON CONFLICT (job_id) DO UPDATE
SET content = EXCLUDED.content, fencing_token = EXCLUDED.fencing_token
WHERE reports.fencing_token < EXCLUDED.fencing_token
```

Fencing only works if the receiving system supports a conditional write. Databases do. Object stores partly. A send-email API does not. For those, the answer is an idempotency key, not a fencing token.

`scripts/eviction-test.sh` reproduces the whole thing: it `SIGSTOP`s a worker mid-handler, waits for the reaper and a second worker to take over, then thaws the first one and asserts it never wrote.

![eviction test output](docs/eviction-test.svg)

Real output. A claims with token 1 and freezes; the reaper requeues 31s later; B claims with token 2 and writes; A thaws to find its lease gone. Two things can stop A on thaw: its heartbeat noticing the lost lease and cancelling the handler's context, or the fencing token rejecting the write. Which one wins is a race, so the test asserts only what holds either way. A never writes, and B's row survives.

> Two mistakes worth recording. First, I wrote the `AND current_worker = $2` guard and didn't check `RowsAffected`. An `UPDATE` matching zero rows isn't an error in Postgres, so for one commit the guard silently did nothing and the worker never learned it had been evicted. Second, my first attempt at this test used `^C`, which kills the worker instead of freezing it. A dead worker never wakes up to write anything, so the test passed without testing the thing it was named after.

### A handler fails

[`6a52175`](https://github.com/allenantony-dev/taskq/commit/6a52175)

Before retries existed, a failing handler left the job `running` and the reaper swept it back to `pending`. That worked, but badly: the retry delay was whatever the lease happened to be, a healthy failure was indistinguishable from a crash, and a malformed payload retried forever.

- `attempts` increments on every claim; `max_attempts` (default 5) caps it.
- Exhausted jobs go to `dead`, a state the reaper never touches.
- `available_at` holds a job back until a chosen time, so retry delay is independent of the lease.
- Backoff is `1s, 10s, 1m, 5m`, wrapped in full jitter (`rand.N(delay)`) so a thousand jobs failing together don't retry in lockstep and knock over whatever they were waiting on.
- `last_error` records why, on every failure. Without it, `state = 'dead'` tells you nothing at 3am.
- A handler that wraps `worker.ErrPermanent` skips retries entirely. Retrying a malformed payload five times is five guaranteed failures.

`attempts` only ever goes up. Nothing decrements it, including graceful shutdown, because a counter with two directions can't guarantee termination.

### Running a job later

[`73175f3`](https://github.com/allenantony-dev/taskq/commit/73175f3)

`available_at` was added for retry backoff, but nothing about it is retry-specific, so `EnqueueAt` exposes it directly. Recurrence (every Monday at 9am) is deliberately left to system cron rather than built in.

That creates a new problem: cron on three hosts fires three times. `idempotency_key` is `UNIQUE`, so the first insert wins and the others are told they lost. The key is compared against the existing job's type and payload. The same key with different content is a caller mistake, and returns `409`.

> The column is nullable for a reason. The key is a plain `string` parameter, so every keyless enqueue sends `""`, and `""` is not `NULL`. Postgres treats multiple `NULL`s as distinct in a unique index, but two empty strings collide. The second keyless job in the system would have failed forever. `NULLIF($4, '')` in the insert is the fix.

### Urgent work stuck behind trivial work

[`650f7a7`](https://github.com/allenantony-dev/taskq/commit/650f7a7)

`priority` orders the claim ahead of insertion order. Lower is more urgent, with named tiers spaced apart so a new tier can be slotted in later:

```go
PriorityHigh   = 0
PriorityNormal = 5
PriorityLow    = 10
```

### Shutting a worker down

[`d1ad0f3`](https://github.com/allenantony-dev/taskq/commit/d1ad0f3)

Killing a worker mid-job means waiting for the lease to lapse before anything can pick the job up. That's 30 seconds of nothing, times however many jobs were in flight, on every deploy. And it burns an attempt, so a shutdown you chose looks exactly like a crash.

On `SIGINT`/`SIGTERM` the worker stops claiming new jobs and lets the current one finish. Handlers receive a `context.Context` and can stop early; ones that ignore it can't be helped, because Go has no way to kill a goroutine from outside. After 30 seconds the process exits anyway, and `signal.NotifyContext` is unregistered first so a second Ctrl-C actually kills rather than being swallowed.

If a handler stops early because its context was cancelled, the job is `Release`d: back to `pending`, immediately eligible, no backoff, `attempts` untouched.

### Which calls should be cancellable

[`5b59eca`](https://github.com/allenantony-dev/taskq/commit/5b59eca)

Not every database call should be. `Complete`, `Retry`, `Dead` and `Release` all record work that already happened. Cancelling one loses the record and the job runs again. They use their own 5-second timeout instead of the caller's context, so a hung database can't block a worker forever.

`Enqueue`, `Dequeue` and `Get` start work rather than record it, so they take the caller's context. Transaction rollback is cleanup, so it runs on `context.Background()`.

> The reflex on a context pass is to thread it everywhere. Doing that here would have made the completion writes cancellable, which is a bug that only shows up during shutdown.

## Schema

```sql
CREATE TABLE jobs (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type            VARCHAR     NOT NULL,
    payload         JSONB       NOT NULL,
    state           VARCHAR     NOT NULL,  -- pending | running | done | dead
    current_worker  VARCHAR,               -- who holds it now
    lease_expiry    TIMESTAMPTZ,           -- when the claim goes stale
    fencing_token   BIGINT      NOT NULL DEFAULT 0,      -- +1 per claim, never reset
    attempts        BIGINT      NOT NULL DEFAULT 0,      -- +1 per claim, never decremented
    max_attempts    INTEGER     NOT NULL DEFAULT 5,
    available_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- not eligible before this
    last_error      TEXT,
    idempotency_key TEXT UNIQUE,
    priority        INTEGER     NOT NULL DEFAULT 5       -- lower runs first
);
```

`current_worker` and `lease_expiry` describe the *current claim* and are cleared when it ends. `fencing_token` and `attempts` are *history* and are never cleared. Mixing those two up breaks the guarantees.

## Running it

Postgres, with two databases: one for the queue, one for the `report` handler's output. They are separate on purpose. A handler might write anywhere, so the queue can't assume it owns the handler's storage. The cost of that choice is that the work and the `Complete` can't share a transaction, which is why delivery is at-least-once.

```bash
docker compose up -d
```

`database/init.sh` runs on first start: it applies `database/migrations/queue/` to the queue database, creates `reports`, and applies `database/migrations/reports/` to that. It also creates `taskq_test`, which the tests wipe between cases.

It runs only when the data directory is empty, which with a named volume means the first `up` and never again. A migration added later is silently skipped: `docker compose up` reports success, then the app fails with a column-does-not-exist error. Apply it by hand, or `docker compose down -v` to recreate the volume.

```bash
export DATABASE_URL="postgres://taskq:taskq@localhost:5432/taskq?sslmode=disable"
export REPORTS_DATABASE_URL="postgres://taskq:taskq@localhost:5432/reports?sslmode=disable"

go run ./cmd/reaper &
go run ./cmd/worker &
go run ./cmd/producer
```

`LOG_LEVEL` (`debug`, `info`, `warn`, `error`; default `info`) sets the level for all three services. `debug` shows the per-poll `queue empty` line, and `warn` silences the reaper's per-tick line.

`WORKER_CONCURRENCY` (default `1`) runs that many claim loops in one process, each with its own worker id. `FOR UPDATE SKIP LOCKED` already makes concurrent claiming safe, so they share the pool and the handler map and nothing else.

`REPORT_DELAY` (e.g. `90s`) makes the `report` handler sleep, which is how the eviction test keeps a job in flight long enough to freeze the worker holding it.

### Tests

```bash
export TEST_DATABASE_URL="postgres://taskq:taskq@localhost:5432/taskq_test?sslmode=disable"
go test ./...
```

Most of what is worth testing here is SQL — the ownership guards, `SKIP LOCKED`, the `ON CONFLICT` dedup — so those tests run against a real Postgres rather than a mock.

**Without `TEST_DATABASE_URL` every test skips and `go test ./...` still prints `ok`.** That is the right default for a fresh clone, but it means a CI job that forgets the variable is green because it ran nothing. Set it there, and check the run reports the tests it actually executed.

### API

```bash
go run ./cmd/api      # :8080
```

```bash
curl -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: digest-2026-09-14' \
  -d '{"type": "email", "payload": {"to": "alice@example.com"}}'
# 201 {"id": 42}
# same key again:              200 {"id": 42}
# same key, different payload: 409

curl localhost:8080/jobs/42
# {"id":42,"type":"email","state":"done","attempts":1,"max_attempts":5,"last_error":null}
```

`current_worker`, `lease_expiry` and `fencing_token` are deliberately not exposed, since anything returned becomes something callers depend on.

### Writing a handler

```go
handlers := map[string]worker.Handler{
    "email": func(ctx context.Context, task queue.Task) error {
        to, ok := task.Payload["to"].(string)
        if !ok {
            // No amount of retrying fixes a bad payload.
            return fmt.Errorf("missing 'to': %w", worker.ErrPermanent)
        }
        return send(ctx, to, task.FencingToken)
    },
}
```

Handlers get a context (cancelled on shutdown or eviction) and a fencing token. Both are optional to use and neither can be enforced from the outside.

## Known gaps

Deliberately deferred, with the trigger for each.

**When there's real load**
- No index on `(state, available_at)`, so `Dequeue` sequential-scans. One line, and the first thing to do.
- Nothing prunes old `done`/`dead` rows. Note that deleting a row frees its idempotency key.
- No priority aging, so sustained high-priority load would starve low-priority jobs.

**Before exposing the API publicly**
- No auth on `POST /jobs`.
- No read/write timeouts on the HTTP server.
- No graceful shutdown for `cmd/api`.

**Small blast radius**
- No backoff on the reap path: a job that crashes its worker retries as fast as the lease allows, bounded by `max_attempts`.
- The duplicate-key lookup in `EnqueueAt` is a second query, not atomic. It breaks once pruning exists.
- `last_error` is unbounded `TEXT`.
- `EnqueueAt` has six positional parameters and wants an options struct.

**Not yet built**
- No migration runner. `init.sh` only runs on an empty volume, so a migration added later needs `docker compose down -v` or applying by hand. Bites as soon as there is a second environment.

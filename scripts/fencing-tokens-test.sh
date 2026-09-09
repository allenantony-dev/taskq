#!/bin/bash
# Proves the fencing token rejects a zombie worker's stale write.
#
# Worker A claims the job, then is SIGSTOPped mid-handler (frozen, not killed --
# ^C would end it and there'd be no stale write to reject). Its lease expires,
# the reaper requeues the job, worker B claims it with a higher token and writes
# the report. A is then thawed and tries to write with its old token.
#
# REPORT_DELAY holds the handler open long enough for that to happen. Every wait
# polls the database rather than sleeping a fixed interval, so the script does
# not depend on the exact delay -- raising it only makes the run longer.
#
# For the control, comment out the WHERE clause in the report handler's upsert
# and run again: A's stale write lands, reports ends at token 1, and this FAILs.

set -u
cd "$(dirname "$0")/.." || exit 1

PSQL_CONTAINER=${PSQL_CONTAINER:-taskq-postgres}
export DATABASE_URL=${DATABASE_URL:-postgres://taskq:taskq@localhost:5432/taskq?sslmode=disable}
export REPORTS_DATABASE_URL=${REPORTS_DATABASE_URL:-postgres://taskq:taskq@localhost:5432/reports?sslmode=disable}
export REPORT_DELAY=${REPORT_DELAY:-30s}

qdb() { docker exec "$PSQL_CONTAINER" psql -U taskq -d taskq -tAc "$1"; }
rdb() { docker exec "$PSQL_CONTAINER" psql -U taskq -d reports -tAc "$1"; }

RUN=$(mktemp -d)
APID= BPID= RPID=
cleanup() { kill -CONT $APID 2>/dev/null; kill $APID $BPID $RPID 2>/dev/null; wait 2>/dev/null; rm -rf "$RUN"; }
trap cleanup EXIT

# wait_for <sql> <expected> <timeout seconds>
wait_for() {
	local deadline=$((SECONDS + $3))
	while [ $SECONDS -lt $deadline ]; do
		[ "$(qdb "$1")" = "$2" ] && return 0
		sleep 1
	done
	echo "TIMEOUT waiting for: $1 = $2" >&2
	return 1
}

say() { printf '\n[t=%3ds] %s\n' "$SECONDS" "$1"; }

go build -o "$RUN" ./... || exit 1

qdb "DELETE FROM jobs;" >/dev/null
rdb "TRUNCATE reports;" >/dev/null
ID=$(qdb "INSERT INTO jobs (type, payload, state) VALUES ('report', '{}', 'pending') RETURNING id;" | head -1)
say "enqueued job $ID (REPORT_DELAY=$REPORT_DELAY)"

"$RUN/reaper" >"$RUN/reaper.log" 2>&1 &	RPID=$!
"$RUN/worker" >"$RUN/A.log" 2>&1 &	APID=$!

wait_for "SELECT state FROM jobs WHERE id=$ID;" running 30 || exit 1
say "worker A claimed it, token $(qdb "SELECT fencing_token FROM jobs WHERE id=$ID;")"

kill -STOP $APID
say "SIGSTOP worker A (pid $APID) -- its heartbeat is frozen too, so the lease will lapse"

wait_for "SELECT state FROM jobs WHERE id=$ID;" pending 90 || exit 1
say "reaper requeued it"

"$RUN/worker" >"$RUN/B.log" 2>&1 &	BPID=$!
wait_for "SELECT state FROM jobs WHERE id=$ID;" done 300 || exit 1
say "worker B finished it, token $(qdb "SELECT fencing_token FROM jobs WHERE id=$ID;")"
echo "        reports holds: $(rdb "SELECT content||' (token '||fencing_token||')' FROM reports WHERE job_id=$ID;")"

kill -CONT $APID
say "SIGCONT worker A -- it wakes with a stale token and tries to write"

for _ in $(seq 1 60); do grep -q "write rejected" "$RUN/A.log" && break; sleep 1; done

echo
echo "===== worker A ====="; grep -v "Lease couldn't be renewed" "$RUN/A.log"
echo "      ($(grep -c "Lease couldn't be renewed" "$RUN/A.log") suppressed 'Lease couldn't be renewed' lines)"
echo "===== worker B ====="; grep -v "Lease couldn't be renewed" "$RUN/B.log"
echo "===== reports ====="; docker exec "$PSQL_CONTAINER" psql -U taskq -d reports -c "SELECT * FROM reports;"

echo "===== verdict ====="
TOKEN=$(rdb "SELECT fencing_token FROM reports WHERE job_id=$ID;")
if grep -q "Task $ID failed: write rejected, stale token 1" "$RUN/A.log" && [ "$TOKEN" = "2" ]; then
	echo "PASS: A's stale write was rejected; reports still holds B's row at token 2"
else
	echo "FAIL: expected A rejected and reports at token 2, got token '$TOKEN'"
	exit 1
fi

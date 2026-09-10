#!/bin/bash
# Proves an evicted worker cannot corrupt the report the new owner wrote.
#
# Worker A claims the job, then is SIGSTOPped mid-handler (frozen, not killed --
# ^C would end it and it would never wake to do damage). Its lease expires, the
# reaper requeues the job, worker B claims it with a higher token and writes the
# report. A is then thawed and tries to carry on.
#
# Two defences can stop it, and which one wins is a genuine race: on SIGCONT both
# A's handler and its heartbeat are runnable, so either the heartbeat cancels the
# context first (A gives up) or the handler reaches its write first (the fencing
# token rejects it). Both are correct, so this asserts what holds either way --
# A never writes successfully, and B's row survives.
#
# REPORT_DELAY holds the handler open long enough for that to happen. Every wait
# polls the database rather than sleeping a fixed interval, so the script does
# not depend on the exact delay -- raising it only makes the run longer.
#
# For the control, remove BOTH defences -- the cancel() in the heartbeat's
# lost-lease branch and the WHERE clause in the report handler's upsert -- and
# run again: A's write lands, reports drops to token 1, and this FAILs.

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
say "SIGCONT worker A -- it should be stopped by cancellation or by the fencing token"

for _ in $(seq 1 60); do grep -qE "context canceled|write rejected" "$RUN/A.log" && break; sleep 1; done

echo
echo "===== worker A ====="; grep -v "queue empty" "$RUN/A.log"
echo "===== worker B ====="; grep -v "queue empty" "$RUN/B.log"
echo "===== reports ====="; docker exec "$PSQL_CONTAINER" psql -U taskq -d reports -c "SELECT * FROM reports;"

echo "===== verdict ====="
TOKEN=$(rdb "SELECT fencing_token FROM reports WHERE job_id=$ID;")
if grep -qE "handler failed .*job_id=$ID .*err=\"(context canceled|write rejected)" "$RUN/A.log" &&
	! grep -q "report written" "$RUN/A.log" &&
	[ "$TOKEN" = "2" ]; then
	echo "PASS: evicted worker A never wrote ($(grep -oE "context canceled|write rejected" "$RUN/A.log" | head -1)); reports holds B's row at token 2"
else
	echo "FAIL: A was expected to be stopped without writing, with reports at token 2 (got '$TOKEN')"
	exit 1
fi

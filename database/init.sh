#!/bin/bash
set -e

for f in /migrations/queue/*.sql; do
	psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$f"
done

createdb -U "$POSTGRES_USER" reports
for f in /migrations/reports/*.sql; do
	psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d reports -f "$f"
done

# Tests wipe the jobs table between cases, so they get their own database.
createdb -U "$POSTGRES_USER" taskq_test
for f in /migrations/queue/*.sql; do
	psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d taskq_test -f "$f"
done

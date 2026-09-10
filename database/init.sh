#!/bin/bash
set -e

for f in /migrations/queue/*.sql; do
	psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$f"
done

createdb -U "$POSTGRES_USER" reports
for f in /migrations/reports/*.sql; do
	psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d reports -f "$f"
done

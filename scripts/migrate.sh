#!/usr/bin/env bash
set -euo pipefail

MIGRATIONS_DIR="${MIGRATIONS_DIR:-supabase/migrations}"

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is not set; skipping migrations."
  exit 0
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "psql is required to run migrations."
  exit 1
fi

shopt -s nullglob
files=("$MIGRATIONS_DIR"/*.sql)

if [[ ${#files[@]} -eq 0 ]]; then
  echo "No SQL migration files found in $MIGRATIONS_DIR"
  exit 0
fi

for file in "${files[@]}"; do
  echo "Applying migration: $file"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$file"
done

echo "Migrations complete."

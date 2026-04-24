# ProBuy Product Intelligence (Initial Scaffold)

This repository contains the minimal, deployable scaffold for ProBuy Product Intelligence database/search work.

## Scope in this phase

- FastAPI service with health/version endpoints.
- Supabase migration directory and initial SQL placeholder.
- Render-friendly startup and migration scripts.
- No Meilisearch yet.
- No B2/R2 ingestion yet.

## Project structure

- `api/` — FastAPI app code
- `scripts/` — startup and migration shell scripts
- `supabase/migrations/` — SQL migrations for Supabase Postgres

## Endpoints

- `GET /health` → `{ "status": "ok" }`
- `GET /version` → `{ "version": "0.1.0" }`

## Local run

1. Create and activate a virtual environment.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Start the app:
   ```bash
   ./scripts/start.sh
   ```
4. Visit `http://localhost:10000/health` and `http://localhost:10000/version`.

## Migrations

- Put SQL migrations in `supabase/migrations/`.
- Run migrations:
  ```bash
  DATABASE_URL='postgresql://...' ./scripts/migrate.sh
  ```

## Render deployment notes

- **Start command:** `./scripts/start.sh`
- **Optional pre-deploy command:** `./scripts/migrate.sh`

## Next phase (not implemented here)

- Add initial schema/tables for product intelligence.
- Add seeded demo data (20 SCN products).
- Add search/query functionality on Supabase Postgres.

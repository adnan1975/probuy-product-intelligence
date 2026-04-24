# ProBuy Product Intelligence (Phase 1: Supabase/Postgres Schema)

This repository contains the deployable Phase 1 scaffold for ProBuy Product Intelligence using FastAPI + Supabase Postgres.

## Phase 1 scope

- FastAPI service with health/version endpoints.
- Postgres migration set for normalized product intelligence data.
- Source traceability via JSONB row payloads (for current/demo rows).
- Supabase/Postgres full-text search document table.
- Multi-source-ready design (SCN International as first source).

## Database design highlights

All Phase 1 tables live under the `probuy` schema and are designed to support:

- multiple primary sources
- source products and source locations
- source price and inventory by location
- attribute dimensions + product attribute values
- search documents for Supabase full-text search
- raw source row traceability (`JSONB`) without storing full raw source files in Postgres

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

## Running migrations locally

1. Ensure your Postgres/Supabase database is reachable.
2. Set `DATABASE_URL`.
3. Run:
   ```bash
   DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/DBNAME?sslmode=require' ./scripts/migrate.sh
   ```

Notes:
- Migrations run in lexicographic order from `supabase/migrations/`.
- Keep new migrations additive and forward-only.

## Running migrations on Render

Use one of these patterns:

1. **Pre-deploy command (recommended)**
   - In Render service settings, set pre-deploy command:
     ```bash
     ./scripts/migrate.sh
     ```
   - Ensure `DATABASE_URL` is configured in environment variables.

2. **Manual one-off migration job**
   - Run a one-off command in Render shell:
     ```bash
     ./scripts/migrate.sh
     ```

3. **Service startup (only if safe for your deployment flow)**
   - Keep start command as:
     ```bash
     ./scripts/start.sh
     ```

## Current migration set

1. `0001_init.sql` (placeholder)
2. `0002_pricing_schema.sql`
3. `0003_primary_sources.sql`
4. `0004_import_batches.sql`
5. `0005_source_products.sql`
6. `0006_source_locations.sql`
7. `0007_source_product_prices.sql`
8. `0008_source_product_inventory.sql`
9. `0009_attribute_definitions.sql`
10. `0010_product_attribute_values.sql`
11. `0011_product_search_documents.sql`

# ProBuy Product Intelligence (Phase 1 + Phase 2 Search)

This repository contains the deployable Phase 1 + Phase 2 scaffold for ProBuy Product Intelligence using FastAPI + Supabase Postgres as the system of record, with optional Meilisearch for search execution.

## Phase 1 scope

- FastAPI service with health/version endpoints plus Supabase/Postgres search APIs.
- Postgres migration set for normalized product intelligence data.
- Source traceability via JSONB row payloads (for current/demo rows).
- Supabase/Postgres full-text search document table.
- Multi-source-ready design (SCN International as first source).

## Phase 2 scope (Meilisearch optional)

- `SEARCH_ENGINE` feature flag to choose `supabase` or `meilisearch`.
- Meilisearch client module for search and health checks.
- `GET /api/search/health` endpoint for active search engine health.
- Graceful fallback to Supabase search when Meilisearch is selected but unavailable.
- Local `docker-compose.meilisearch.yml` option for running Meilisearch.

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
- `GET /api/search/products?q=` → product search using Postgres full-text search with trigram fuzzy fallback.
- `GET /api/search/health` → search subsystem health with configured engine + Meilisearch status (when enabled).
- `GET /api/products/{source_product_id}` → product detail by UUID.
- `GET /api/products/{source_product_id}/attributes` → attribute list for a product.

### Search API behavior

`GET /api/search/products` supports:

- `q` keyword search via `websearch_to_tsquery` + `tsvector`.
- fuzzy fallback via `pg_trgm` similarity when no FTS matches are found.
- optional `brand` filter.
- optional `source` filter (`SCN`, etc).
- attribute filters as additional query params (for example `color=black` or `size=large`).

Example:

```http
GET /api/search/products?q=3 inch blade&brand=3M&color=black
```

Search responses return:

- `source_product_id`
- `source_code`
- `title`
- `brand`
- `manufacturer`
- `model_number`
- `category`
- `primary_image`
- `list_price`
- `distributor_cost`
- `quantity_available`
- `matched_attributes` (attributes matching provided attribute filters)
- `engine_used` (`supabase` or `meilisearch`)
- `fallback_applied` (`true` when Meilisearch was selected but unavailable and Supabase fallback was used)

## Environment configuration

Use `.env.example` as your baseline.

Important settings:

- `DATABASE_URL` — required Supabase/Postgres connection string.
- `SEARCH_ENGINE` — `supabase` (default) or `meilisearch`.
- `MEILISEARCH_HOST` — Meilisearch base URL (default `http://localhost:7700`).
- `MEILISEARCH_API_KEY` — optional Meilisearch API key / master key.
- `MEILISEARCH_INDEX` — index to query (default `products`).
- `MEILISEARCH_TIMEOUT_SECONDS` — request timeout before fallback.

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
5. Query search endpoints:
   ```bash
   curl 'http://localhost:10000/api/search/products?q=respirator'
   curl 'http://localhost:10000/api/search/health'
   curl 'http://localhost:10000/api/search/products?q=3%20inch%20blade&brand=3M&source=SCN&color=black'
   curl 'http://localhost:10000/api/products/<SOURCE_PRODUCT_UUID>'
   curl 'http://localhost:10000/api/products/<SOURCE_PRODUCT_UUID>/attributes'
   ```

## Local Meilisearch option (Phase 2)

Run Meilisearch locally:

```bash
docker compose -f docker-compose.meilisearch.yml up -d
```

Then set:

```bash
SEARCH_ENGINE=meilisearch
MEILISEARCH_HOST=http://localhost:7700
```

Notes:
- Supabase/Postgres remains the system of record.
- Search results still hydrate product details from Supabase/Postgres.
- If Meilisearch is down/unreachable, the API automatically falls back to Supabase search.

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

## Demo seed dataset (SCN)

Phase 1 now includes a demo seed migration that loads SCN sample data for schema + search testing.

- Source files used:
  - `input/sample/contentLicensing_example.xlsx`
  - `input/sample/price_list_example.xlsx`
  - `input/sample/inventory_list_example.xlsx`
- Seeded records:
  - `SCN International` in `primary_sources`
  - 20 demo `source_products` (19 content+price rows plus 1 inventory-only demo row to keep seed size at 20)
  - related `source_product_prices` where available
  - related `source_product_inventory` where available
  - attribute definitions + product attribute values
  - `product_search_documents` for the seeded SCN products
- Original source-row payloads are preserved as `JSONB` on the seeded rows for traceability.

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
12. `0012_seed_scn_demo_products.sql`
13. `0013_search_pg_trgm.sql`

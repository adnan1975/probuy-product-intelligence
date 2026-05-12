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
- `frontend/src/theme/` — reusable React design tokens + pricing/search layout theme helpers

## React theme infrastructure (UI styling only)

To keep pricing/search UI styling consistent without touching business logic, reusable React tokens are available under `frontend/src/theme/`.

- `tokens.ts` exports typed design tokens for:
  - colors
  - border radius
  - shadows
  - spacing
  - typography
- `layout.ts` exports `pricingLayout` defaults that support:
  - full-width search bar
  - left-side facets rail
- `theme.css` provides CSS variables and optional utility classes for pricing pages.
- `index.ts` provides a clean single import surface.

Example import:

```ts
import { themeTokens, pricingLayout } from './theme';
import './theme/theme.css';
```

## Endpoints

- `GET /health` → `{ "status": "ok" }`
- `GET /version` → `{ "version": "0.1.0" }`
- `GET /api/search/products?q=` → product search using Postgres full-text search with trigram fuzzy fallback.
- `GET /api/search/health` → search subsystem health with configured engine + Meilisearch status (when enabled).
- `GET /api/products/{source_product_id}` → product detail by UUID.
- `GET /api/products/{source_product_id}/attributes` → attribute list for a product.
- `GET /api/categories?channel_code=SHOPIFY` → list channel category hierarchy records.
- `POST /api/categories` → create category for a channel (defaults to Shopify).
- `PATCH /api/categories/{category_id}` → update category metadata.
- `POST /api/categories/{category_id}/move` → move/reorder a category in the hierarchy.
- `DELETE /api/categories/{category_id}` → soft delete category (`is_active=false`, `deleted_at` set).
- `GET /api/categories/mappings` → list source product ↔ channel category mappings for a channel (**product-level assignment/override only**).
- `POST /api/categories/mappings` → map a source product to a channel category (**product-level assignment/override only**).
- `GET /api/categories/crosswalk-mappings` → list source category ↔ channel category crosswalk mappings.
- `POST /api/categories/crosswalk-mappings` → create/update source category ↔ channel category crosswalk mappings.
- `POST /api/categories/bootstrap/shopify` → bootstrap Shopify categories + mappings from export CSV.
- `POST /sync/start` → trigger full Meilisearch sync from Supabase product search documents.

### Search API behavior

`GET /api/search/products` supports:

- `q` keyword search via `websearch_to_tsquery` + `tsvector`.
- fuzzy fallback via `pg_trgm` similarity when no FTS matches are found.
- optional text filters: `brand`, `manufacturer`, `category`, `source`, `stock_status`.
- optional structured attribute filters: `color`, `size`, `material` (plus additional dynamic attribute query params).
- optional numeric range filters: `price_min`/`price_max`, `length_min`/`length_max`, `width_min`/`width_max`, `height_min`/`height_max`, `weight_min`/`weight_max`.

Example:

```http
GET /api/search/products?q=3 inch blade&brand=3M&manufacturer=3M&category=Disposable Respirators&source=SCN&color=black&price_min=10&price_max=100
```

Search responses return:

- `results` (paged product matches)
- `total_count` (total matches before pagination)
- `facetDistribution` (facet counts from search backend)
- `applied_filters` (normalized filters applied to this query)
- `source_product_id`
- `source_code`
- `title`
- `brand`
- `manufacturer`
- `model_number`
- `category`
- `primary_image`
  - generated from the product's main image filename as `https://f004.backblazeb2.com/file/probuy-images/{image_file_name}`
  - when no main image filename exists, API returns a stock fallback image URL: `https://placehold.co/600x600?text=No+Image`
- `list_price`
- `distributor_cost`
- `quantity_available`
- `stock_status`
- `matched_attributes` (attributes matching provided attribute filters)
- `engine_used` (`supabase` or `meilisearch`)
- `fallback_applied` (`true` when Meilisearch was selected but unavailable and Supabase fallback was used)

## Environment configuration

Use `.env.example` as your baseline.

Important settings:

- `DATABASE_URL` — required Supabase/Postgres connection string.
- `SEARCH_ENGINE` — `supabase` (default) or `meilisearch`.
- `CORS_ALLOWED_ORIGINS` — comma-separated explicit browser origins allowed by the API.
- `CORS_ALLOW_ORIGIN_REGEX` — optional regex for preview/staging origins (defaults allow localhost, `*.onrender.com`, `*.vercel.app`).
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


## Manual Meilisearch full sync

Use this when you need to rebuild the Meilisearch `products` index from Supabase/Postgres `product_search_documents`.

1. Ensure `DATABASE_URL` points to your Supabase Postgres instance.
2. Ensure Meilisearch is reachable (`MEILISEARCH_HOST`, optional `MEILISEARCH_API_KEY`, optional `MEILISEARCH_INDEX`).
3. Run the sync script:

```bash
python scripts/sync_meilisearch.py
```

What the script does:
- Reads all active records from `probuy.product_search_documents` joined to source products.
- Sends documents to Meilisearch with fields:
  - `source_product_id`
  - `source_code`
  - `title`
  - `brand`
  - `manufacturer`
  - `model_no`
  - `category`
  - `search_text`
  - `attributes`
  - `price`
  - `inventory_status`
- Configures filterable attributes:
  - `source_code`, `brand`, `manufacturer`, `category`
  - `attributes.color`, `attributes.size`, `attributes.material`, `attributes.length`
- Configures searchable fields:
  - `title`, `brand`, `manufacturer`, `model_no`, `search_text`

You can also trigger the same full sync through the API:

```bash
curl -X POST http://localhost:10000/sync/start
```

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


## Phase A: Channel publication tracking (Shopify-ready)

Phase A adds a channel-aware publication model so search and APIs can filter by publication status without coupling channel state to core product rows.

### New tables

- `probuy.sales_channels`
  - Channel registry (starts with `SHOPIFY`)
  - Fields: `code`, `name`, `is_active`, timestamps
- `probuy.product_channel_publications`
  - One row per `(source_product_id, channel_id)`
  - Fields include `publish_method`, `publication_status`, `is_published`, external IDs, sync timestamps, error/message metadata

### Supported values

- `publish_method`: `AUTO`, `MANUAL`, `API_SYNC`, `BULK_FEED`
- `publication_status`: `NOT_PUBLISHED`, `QUEUED`, `PUBLISHED`, `UNPUBLISHED`, `FAILED`

## Phase B: Shopify category management

Phase B adds a channel-specific category hierarchy plus two separate mapping layers:
- **Category crosswalk**: source categories (SCN first) mapped to channel categories.
- **Product category assignment/override**: per-product mappings that can override category-level defaults.

### New tables

- `probuy.channel_categories`
  - Category tree keyed by sales channel (`SHOPIFY` ready)
  - Supports `parent_id` for nesting and move operations
  - Stores `name`, `description`, `image_url`, `slug`, and optional `external_category_id`
  - Uses soft delete (`deleted_at`, `is_active=false`) for category deletion
- `probuy.channel_category_tags`
  - Normalized tags per category (`category_id`, `tag`)
  - Unique per category and tag
- `probuy.product_category_mappings`
  - Links `source_products` to channel categories (product-level assignment/override)
  - Supports a single primary mapping per product
  - Tracks mapping origin (`MANUAL`, `RULE`, `IMPORT`, `SYNC`)
  - Allows many mappings per product, with exactly one primary mapping
- `probuy.source_categories`
  - Normalized source-side categories (`SCN` first) independent of products
  - Supports hierarchy through `parent_id`
  - Keeps optional `external_category_key` + JSONB `metadata`
- `probuy.channel_category_source_category_mappings`
  - Crosswalk between `source_categories` and `channel_categories`
  - Supports mapping provenance (`MANUAL`, `RULE`, `IMPORT`, `SYNC`)
  - Allows many channel mappings per source category, with one primary mapping per source category

### Integrity protections

- Category slugs are unique per channel (`unique(channel_id, slug)`).
- A DB trigger blocks cyclic parent-child moves in the category tree.

### Category API payload examples

Create a Shopify category:

```bash
curl -X POST http://localhost:10000/api/categories \
  -H "Content-Type: application/json" \
  -d '{
    "channel_code": "SHOPIFY",
    "slug": "safety-gloves",
    "name": "Safety Gloves",
    "description": "Hand protection products",
    "image_url": "https://cdn.example.com/cat/safety-gloves.jpg",
    "tags": ["ppe", "gloves", "safety"],
    "sort_order": 10
  }'
```

Move a category under a new parent:

```bash
curl -X POST http://localhost:10000/api/categories/<CATEGORY_ID>/move \
  -H "Content-Type: application/json" \
  -d '{"parent_id":"<NEW_PARENT_ID>","sort_order":20}'
```

Map a product to a Shopify category (product-level assignment/override):

```bash
curl -X POST http://localhost:10000/api/categories/mappings \
  -H "Content-Type: application/json" \
  -d '{
    "source_product_id":"<SOURCE_PRODUCT_UUID>",
    "channel_category_id":"<CATEGORY_UUID>",
    "is_primary": true,
    "mapping_source": "MANUAL"
  }'
```

Map an SCN source category to a Shopify category (category crosswalk):

```bash
curl -X POST http://localhost:10000/api/categories/crosswalk-mappings \
  -H "Content-Type: application/json" \
  -d '{
    "source_category_id":"<SOURCE_CATEGORY_UUID>",
    "channel_category_id":"<CATEGORY_UUID>",
    "is_primary": true,
    "mapping_source": "MANUAL"
  }'
```

List category crosswalk mappings for SCN + Shopify:

```bash
curl "http://localhost:10000/api/categories/crosswalk-mappings?channel_code=SHOPIFY&source_code=SCN&is_primary=true&limit=50&offset=0"
```

Bootstrap categories and mappings from a Shopify export CSV:

```bash
curl -X POST http://localhost:10000/api/categories/bootstrap/shopify \
  -H "Content-Type: application/json" \
  -d '{
    "csv_path":"shopify_export/products_export_1(2).csv",
    "channel_code":"SHOPIFY"
  }'
```

The bootstrap process:
- reads `Product Category` path as hierarchy (`A > B > C`)
- upserts channel categories for Shopify
- maps each row's `Variant SKU` to `source_products.source_product_key`
- upserts primary product-category mapping (`mapping_source=IMPORT`)

Product mapping API vs category crosswalk API:
- `/api/categories/mappings` = **product-level assignment/override**
- `/api/categories/crosswalk-mappings` = **source-category to channel-category crosswalk**

### Migration

Run migrations as usual:

```bash
./scripts/migrate.sh
```

### Example query: show only products NOT published to Shopify

```sql
select sp.id, sp.source_product_key, sp.product_title_en
from probuy.source_products sp
left join probuy.sales_channels sc
  on sc.code = 'SHOPIFY'
left join probuy.product_channel_publications pcp
  on pcp.source_product_id = sp.id
 and pcp.channel_id = sc.id
where coalesce(pcp.publication_status, 'NOT_PUBLISHED') <> 'PUBLISHED';
```

### Example query: include Shopify flag in result set

```sql
select
  sp.id,
  sp.source_product_key,
  sp.product_title_en,
  sc.code as channel_code,
  coalesce(pcp.is_published, false) as is_published,
  coalesce(pcp.publication_status, 'NOT_PUBLISHED') as publication_status
from probuy.source_products sp
left join probuy.sales_channels sc
  on sc.code = 'SHOPIFY'
left join probuy.product_channel_publications pcp
  on pcp.source_product_id = sp.id
 and pcp.channel_id = sc.id;
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
12. `0013_search_pg_trgm.sql`
13. `0014_product_images.sql`
14. `0015_sales_channels_and_publications.sql`

## Full reconcile scripts (Render/background friendly)

You can run full reconcile as two separate Python scripts instead of tying this flow to migrations.

### 1) Purge product intelligence data

This script removes current data from `probuy` product intelligence tables.

```bash
python scripts/recon_purge.py
```

Output is structured JSON with deleted row counts and elapsed time.

### 2) Ingest full data from `input/data`

This script reads these required files:

- `input/data/contentlicensing.xlsx`
- `input/data/pricing.xlsx`
- `input/data/inventory.xlsx`

Run:

```bash
python scripts/recon_ingest.py
```

The script fails immediately if any required file is missing or renamed.

Output is structured JSON with:

- elapsed time
- per-file SHA-256 checksum
- row/upsert counters (`products`, `attributes`, `prices`, `inventory`, `search_docs`)

### Render usage pattern

Use two one-off/background jobs in sequence:

1. purge
   ```bash
   python scripts/recon_purge.py
   ```
2. ingest
   ```bash
   python scripts/recon_ingest.py
   ```

Both scripts require `DATABASE_URL` in environment variables.

## Shopify metafield updater (`shipping_time`)

Use `scripts/update_shopify_product_metafield.py` to update exactly one product metafield per CSV row by product handle, without changing core product fields.

### Required Shopify app scopes

- `read_products`
- `write_products`

### Inputs

The script reads a UTF-8 CSV (default `shopify_import.csv`) and expects:

- `Handle`
- `Shipping Time (product.metafields.custom.shipping_time)`

If you pass a different `--key`, the script maps to:

- `Shipping Time (product.metafields.custom.<key>)`

Rows with empty `Handle` or empty metafield value are skipped and logged in the output report.

### Environment variables

- `SHOPIFY_SHOP_DOMAIN` (for `--shop-domain`)
- `SHOPIFY_ADMIN_ACCESS_TOKEN` (for `--access-token`)

### CLI usage

```bash
python scripts/update_shopify_product_metafield.py \
  --csv shopify_import.csv \
  --shop-domain your-store.myshopify.com \
  --access-token shpat_xxx \
  --namespace custom \
  --key shipping_time \
  --type single_line_text_field
```

Optional flags:

- `--dry-run` (lookup only, no metafield writes)
- `--limit 25` (safe rollout cap)
- `--api-version 2025-10`

### Dry-run workflow

1. Run with `--dry-run --limit 10` to validate handle lookup and mapped values.
2. Review `output/shopify_metafield_update_report.csv` for skipped/fail reasons.
3. Run without `--dry-run` once validation is clean.

### Rollback guidance

If a metafield value is incorrect, fix the CSV values and re-run the script for affected rows (optionally with `--limit` for staged correction). Because updates are done via `metafieldsSet`, rerunning with corrected values overwrites only that target metafield key.

## Bootstrap source-to-channel category crosswalk mappings

Use this bootstrap job to derive SCN source-category mappings from existing product-level Shopify category mappings.

Run:

```bash
python scripts/bootstrap_source_channel_category_mappings.py --verbose
```

Behavior:

- Resolves Shopify `channel_id` from `probuy.sales_channels.code = 'SHOPIFY'`.
- Reads candidate products from `probuy.source_products` joined with Shopify `probuy.product_channel_publications` and `probuy.product_category_mappings`.
- Normalizes `source_products.category_en` by trimming, collapsing duplicate whitespace, and lowercasing for dedupe keying.
- Upserts deduped categories into `probuy.source_categories` using `external_category_key` as normalized key.
- When a legacy SCN category row already exists by `name`, bootstrap updates that row first to avoid violating the `(source_id, name)` unique key.
- `source_categories` upsert key is `ON CONFLICT (source_id, external_category_key)` and requires a matching non-partial unique index (added in migration `0020_source_categories_external_key_conflict_support.sql`).
- Upserts crosswalk rows into `probuy.channel_category_source_category_mappings` with `mapping_source='IMPORT'`.

Rerun/idempotency:

- The job is idempotent: reruns update existing `source_categories` and crosswalk rows in place instead of duplicating records.
- It relies on unique keys for `(source_id, external_category_key)` in `source_categories` and `(source_category_id, channel_category_id)` in crosswalk mappings.

Ambiguity handling:

- If one SCN category maps to multiple Shopify categories, all mappings are stored.
- The Shopify category with the highest product count is marked `is_primary=true`.
- All other mappings for that SCN category are set `is_primary=false`.

Report output includes:

- total candidate rows scanned
- total normalized SCN categories found
- mappings created/updated
- ambiguous SCN categories count
- skipped rows with missing Shopify category mapping
- skipped rows with missing SCN category text

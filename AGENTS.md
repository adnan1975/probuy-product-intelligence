# ProBuy product intelligence  Instructions

Work in small phases. Do not implement future phases unless explicitly asked.

Tech assumptions:
- Backend: Python/FastAPI
- Database: Supabase Postgres
- Migrations: SQL files under supabase/migrations
- Search Phase 1: Supabase/Postgres only
- Search Phase 2: Meilisearch
- Raw file storage B2/R2 is out of scope for now

Rules:
- Add clear README instructions for every phase.
- Add seed/demo data with 20 SCN products from provided sample files.
- Do not store full raw source files in Postgres.
- Preserve source row data as JSONB only for the 20 demo rows.
- Every phase must be independently deployable and testable.

The schema must support:
- multiple primary sources
- SCN as the first source
- source products
- source locations
- pricing
- inventory
- image records
- attribute definitions
- product attribute values
- product search documents
- JSONB source row traceability for demo/current rows

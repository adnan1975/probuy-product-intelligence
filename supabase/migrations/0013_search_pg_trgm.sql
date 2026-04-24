-- Enable trigram fuzzy fallback for Phase 1 Postgres search endpoints.
create extension if not exists pg_trgm;

create index if not exists idx_product_search_documents_search_text_trgm
    on probuy.product_search_documents using gin (search_text gin_trgm_ops);

create index if not exists idx_source_products_title_trgm
    on probuy.source_products using gin (product_title_en gin_trgm_ops);

create index if not exists idx_source_products_model_no_trgm
    on probuy.source_products using gin (source_model_no gin_trgm_ops);

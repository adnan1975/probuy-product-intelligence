create table if not exists probuy.source_products (
    id uuid primary key default gen_random_uuid(),
    source_id uuid not null references probuy.primary_sources(id) on delete cascade,
    import_batch_id uuid references probuy.import_batches(id) on delete set null,
    source_product_key text not null,
    source_model_no text,
    brand text,
    manufacturer text,
    product_title_en text,
    description_en text,
    category_en text,
    unit_description_en text,
    product_url text,
    image_url text,
    is_active boolean not null default true,
    raw_data jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_id, source_product_key)
);

create index if not exists idx_source_products_source_key
    on probuy.source_products (source_id, source_product_key);

create index if not exists idx_source_products_brand
    on probuy.source_products (brand);

create index if not exists idx_source_products_manufacturer
    on probuy.source_products (manufacturer);

create index if not exists idx_source_products_model_no
    on probuy.source_products (source_model_no);

create index if not exists idx_source_products_title_fts
    on probuy.source_products using gin (to_tsvector('simple', coalesce(product_title_en, '')));

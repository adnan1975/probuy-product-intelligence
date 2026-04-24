create table if not exists probuy.source_product_prices (
    id uuid primary key default gen_random_uuid(),
    source_product_id uuid not null references probuy.source_products(id) on delete cascade,
    location_id uuid not null references probuy.source_locations(id) on delete cascade,
    model_no text,
    list_price numeric(12, 2),
    distributor_cost numeric(12, 2),
    msrp numeric(12, 2),
    currency_code text not null default 'CAD',
    pricing_update_date timestamptz,
    effective_at timestamptz,
    expires_at timestamptz,
    raw_data jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_product_id, location_id)
);

create index if not exists idx_source_product_prices_product_location
    on probuy.source_product_prices (source_product_id, location_id);

create index if not exists idx_source_product_prices_model_no
    on probuy.source_product_prices (model_no);

create index if not exists idx_source_product_prices_effective_at
    on probuy.source_product_prices (effective_at desc);

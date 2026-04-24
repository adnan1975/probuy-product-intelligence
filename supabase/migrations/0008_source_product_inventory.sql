create table if not exists probuy.source_product_inventory (
    id uuid primary key default gen_random_uuid(),
    source_product_id uuid not null references probuy.source_products(id) on delete cascade,
    location_id uuid not null references probuy.source_locations(id) on delete cascade,
    model_no text,
    stock_status text,
    quantity_available numeric(14, 3),
    quantity_reserved numeric(14, 3),
    quantity_inbound numeric(14, 3),
    inventory_update_date timestamptz,
    raw_data jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_product_id, location_id)
);

create index if not exists idx_source_product_inventory_product_location
    on probuy.source_product_inventory (source_product_id, location_id);

create index if not exists idx_source_product_inventory_model_no
    on probuy.source_product_inventory (model_no);

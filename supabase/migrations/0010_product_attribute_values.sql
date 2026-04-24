create table if not exists probuy.product_attribute_values (
    id uuid primary key default gen_random_uuid(),
    source_product_id uuid not null references probuy.source_products(id) on delete cascade,
    attribute_id uuid not null references probuy.attribute_definitions(id) on delete cascade,
    value_text text,
    value_numeric numeric(18, 6),
    value_boolean boolean,
    unit text,
    raw_data jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_product_id, attribute_id)
);

create index if not exists idx_product_attribute_values_product
    on probuy.product_attribute_values (source_product_id);

create index if not exists idx_product_attribute_values_attribute
    on probuy.product_attribute_values (attribute_id);

create index if not exists idx_product_attribute_values_text
    on probuy.product_attribute_values (value_text);

create index if not exists idx_product_attribute_values_numeric
    on probuy.product_attribute_values (value_numeric);

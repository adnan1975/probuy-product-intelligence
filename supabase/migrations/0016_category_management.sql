create table if not exists probuy.channel_categories (
    id uuid primary key default gen_random_uuid(),
    channel_id uuid not null references probuy.sales_channels(id) on delete cascade,
    parent_id uuid references probuy.channel_categories(id) on delete set null,
    external_category_id text,
    slug text not null,
    name text not null,
    description text,
    image_url text,
    tags text[] not null default '{}'::text[],
    sort_order integer not null default 0,
    is_active boolean not null default true,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (channel_id, slug)
);

create index if not exists idx_channel_categories_channel_parent
    on probuy.channel_categories (channel_id, parent_id);

create index if not exists idx_channel_categories_active
    on probuy.channel_categories (channel_id, is_active);

create table if not exists probuy.product_category_mappings (
    id uuid primary key default gen_random_uuid(),
    source_product_id uuid not null references probuy.source_products(id) on delete cascade,
    channel_category_id uuid not null references probuy.channel_categories(id) on delete cascade,
    mapping_source text not null default 'MANUAL'
        check (mapping_source in ('MANUAL', 'RULE', 'IMPORT', 'SYNC')),
    is_primary boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_product_id, channel_category_id)
);

create unique index if not exists idx_product_category_primary_unique
    on probuy.product_category_mappings (source_product_id)
    where is_primary = true;

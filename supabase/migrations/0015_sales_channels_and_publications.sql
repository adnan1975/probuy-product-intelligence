create table if not exists probuy.sales_channels (
    id uuid primary key default gen_random_uuid(),
    code text not null unique,
    name text not null,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_sales_channels_active
    on probuy.sales_channels (is_active);

create table if not exists probuy.product_channel_publications (
    id uuid primary key default gen_random_uuid(),
    source_product_id uuid not null references probuy.source_products(id) on delete cascade,
    channel_id uuid not null references probuy.sales_channels(id) on delete cascade,
    publish_method text not null default 'MANUAL'
        check (publish_method in ('AUTO', 'MANUAL', 'API_SYNC', 'BULK_FEED')),
    publication_status text not null default 'NOT_PUBLISHED'
        check (publication_status in ('NOT_PUBLISHED', 'QUEUED', 'PUBLISHED', 'UNPUBLISHED', 'FAILED')),
    is_published boolean not null default false,
    external_product_id text,
    external_variant_id text,
    published_at timestamptz,
    last_sync_at timestamptz,
    last_error text,
    sync_metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_product_id, channel_id)
);

create index if not exists idx_product_channel_publications_channel_status
    on probuy.product_channel_publications (channel_id, publication_status);

create index if not exists idx_product_channel_publications_published
    on probuy.product_channel_publications (channel_id, is_published);

create index if not exists idx_product_channel_publications_last_sync
    on probuy.product_channel_publications (last_sync_at desc);

insert into probuy.sales_channels (code, name, is_active)
values ('SHOPIFY', 'Shopify', true)
on conflict (code) do update
set name = excluded.name,
    is_active = excluded.is_active,
    updated_at = now();

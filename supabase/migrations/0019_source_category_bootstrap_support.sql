create table if not exists probuy.source_categories (
    id uuid primary key default gen_random_uuid(),
    source_id uuid not null references probuy.primary_sources(id) on delete cascade,
    external_category_key text,
    name text not null,
    parent_id uuid references probuy.source_categories(id) on delete set null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_id, name)
);

create table if not exists probuy.channel_category_source_category_mappings (
    id uuid primary key default gen_random_uuid(),
    source_category_id uuid not null references probuy.source_categories(id) on delete cascade,
    channel_category_id uuid not null references probuy.channel_categories(id) on delete cascade,
    mapping_source text not null default 'MANUAL'
        check (mapping_source in ('MANUAL', 'RULE', 'IMPORT', 'SYNC')),
    is_primary boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_category_id, channel_category_id)
);

create unique index if not exists idx_source_categories_source_external_key_unique
    on probuy.source_categories (source_id, external_category_key)
    where external_category_key is not null;

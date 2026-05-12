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

create index if not exists idx_source_categories_source_parent
    on probuy.source_categories (source_id, parent_id);

create index if not exists idx_source_categories_external_key
    on probuy.source_categories (source_id, external_category_key)
    where external_category_key is not null;

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

create unique index if not exists idx_channel_source_category_primary_unique
    on probuy.channel_category_source_category_mappings (source_category_id)
    where is_primary = true;

insert into probuy.source_categories (source_id, name, metadata)
select
    sp.source_id,
    trim(sp.category_en) as name,
    jsonb_build_object('seeded_from', 'source_products.category_en')
from probuy.source_products sp
join probuy.primary_sources src on src.id = sp.source_id
where src.code = 'SCN'
  and sp.category_en is not null
  and trim(sp.category_en) <> ''
group by sp.source_id, trim(sp.category_en)
on conflict (source_id, name) do nothing;

create table if not exists probuy.source_locations (
    id uuid primary key default gen_random_uuid(),
    source_id uuid not null references probuy.primary_sources(id) on delete cascade,
    code text not null,
    name text not null,
    province text,
    country text not null default 'CA',
    is_active boolean not null default true,
    raw_data jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_id, code)
);

create index if not exists idx_source_locations_source_code
    on probuy.source_locations (source_id, code);

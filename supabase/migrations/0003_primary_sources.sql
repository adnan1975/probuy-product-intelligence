create table if not exists probuy.primary_sources (
    id uuid primary key default gen_random_uuid(),
    code text not null unique,
    name text not null,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_primary_sources_active
    on probuy.primary_sources (is_active);

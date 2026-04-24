create table if not exists probuy.attribute_definitions (
    id uuid primary key default gen_random_uuid(),
    canonical_name text not null unique,
    display_name text not null,
    data_type text not null,
    unit text,
    is_filterable boolean not null default true,
    is_searchable boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_attribute_definitions_display_name
    on probuy.attribute_definitions (display_name);

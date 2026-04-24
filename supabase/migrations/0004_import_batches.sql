create table if not exists probuy.import_batches (
    id uuid primary key default gen_random_uuid(),
    source_id uuid not null references probuy.primary_sources(id) on delete cascade,
    import_type text not null,
    file_name text,
    imported_at timestamptz not null default now(),
    row_count integer,
    status text not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_import_batches_source_imported_at
    on probuy.import_batches (source_id, imported_at desc);

create index if not exists idx_import_batches_status
    on probuy.import_batches (status);

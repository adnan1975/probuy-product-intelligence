create table if not exists probuy.product_search_documents (
    source_product_id uuid primary key references probuy.source_products(id) on delete cascade,
    search_text text not null,
    search_vector tsvector not null,
    brand text,
    manufacturer text,
    model_no text,
    category text,
    attributes jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_product_search_documents_vector
    on probuy.product_search_documents using gin (search_vector);

create index if not exists idx_product_search_documents_brand
    on probuy.product_search_documents (brand);

create index if not exists idx_product_search_documents_manufacturer
    on probuy.product_search_documents (manufacturer);

create index if not exists idx_product_search_documents_model_no
    on probuy.product_search_documents (model_no);

create index if not exists idx_product_search_documents_attributes
    on probuy.product_search_documents using gin (attributes);

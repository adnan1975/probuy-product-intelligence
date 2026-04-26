create table if not exists probuy.product_images (
    id uuid primary key default gen_random_uuid(),
    source_product_id uuid not null references probuy.source_products(id) on delete cascade,
    image_position integer,
    image_file_name text not null,
    is_main_image boolean not null default false,
    raw_data jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_product_id, image_file_name)
);

create unique index if not exists idx_product_images_one_main_per_product
    on probuy.product_images (source_product_id)
    where is_main_image = true;

create index if not exists idx_product_images_product_position
    on probuy.product_images (source_product_id, image_position);

with candidate_images as (
    select
        sp.id as source_product_id,
        nullif(
            coalesce(
                nullif(sp.raw_data->>'image_main', ''),
                nullif(substring(sp.image_url from '[^/]+$'), '')
            ),
            ''
        ) as image_file_name
    from probuy.source_products sp
)
insert into probuy.product_images (source_product_id, image_position, image_file_name, is_main_image, raw_data)
select
    ci.source_product_id,
    1,
    ci.image_file_name,
    true,
    jsonb_build_object('migrated_from', 'source_products.image_url')
from candidate_images ci
where ci.image_file_name is not null
on conflict (source_product_id, image_file_name) do update
set
    is_main_image = true,
    image_position = coalesce(probuy.product_images.image_position, excluded.image_position),
    updated_at = now();

alter table probuy.source_products
    drop column if exists image_url;

-- Ensure ON CONFLICT (source_id, external_category_key) used by
-- scripts/bootstrap_source_channel_category_mappings.py is backed by a matching
-- non-partial unique index.

-- 1) Deduplicate non-null external keys per source_id, keeping the newest row.
with ranked as (
    select
        id,
        row_number() over (
            partition by source_id, external_category_key
            order by updated_at desc nulls last, created_at desc nulls last, id desc
        ) as rn
    from probuy.source_categories
    where external_category_key is not null
), to_delete as (
    select id from ranked where rn > 1
)
delete from probuy.source_categories sc
using to_delete d
where sc.id = d.id;

-- 2) Replace the old partial unique index (if present) with a full unique index
-- so Postgres can infer the ON CONFLICT target without a predicate.
drop index if exists probuy.idx_source_categories_source_external_key_unique;

create unique index if not exists idx_source_categories_source_external_key_unique
    on probuy.source_categories (source_id, external_category_key);

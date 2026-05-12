alter table probuy.channel_categories
    add column if not exists deleted_at timestamptz;

create table if not exists probuy.channel_category_tags (
    id uuid primary key default gen_random_uuid(),
    category_id uuid not null references probuy.channel_categories(id) on delete cascade,
    tag text not null,
    created_at timestamptz not null default now(),
    unique (category_id, tag)
);

create index if not exists idx_channel_category_tags_category
    on probuy.channel_category_tags (category_id);

do $$
begin
    if exists (
        select 1
        from information_schema.columns
        where table_schema = 'probuy'
          and table_name = 'channel_categories'
          and column_name = 'tags'
    ) then
        insert into probuy.channel_category_tags (category_id, tag)
        select cc.id, trim(t.tag)
        from probuy.channel_categories cc
        cross join lateral unnest(coalesce(cc.tags, '{}'::text[])) as t(tag)
        where trim(t.tag) <> ''
        on conflict (category_id, tag) do nothing;
    end if;
end
$$;

alter table probuy.channel_categories
    drop column if exists tags;

create or replace function probuy.prevent_category_cycles()
returns trigger
language plpgsql
as $$
declare
    found_cycle boolean;
begin
    if new.parent_id is null then
        return new;
    end if;

    if new.parent_id = new.id then
        raise exception 'Category cannot be parent of itself';
    end if;

    with recursive ancestors as (
        select cc.parent_id
        from probuy.channel_categories cc
        where cc.id = new.parent_id
        union all
        select cc.parent_id
        from probuy.channel_categories cc
        join ancestors a on cc.id = a.parent_id
        where a.parent_id is not null
    )
    select exists(select 1 from ancestors where parent_id = new.id) into found_cycle;

    if found_cycle then
        raise exception 'Category hierarchy cycle detected';
    end if;

    return new;
end;
$$;

drop trigger if exists trg_prevent_category_cycles on probuy.channel_categories;
create trigger trg_prevent_category_cycles
before update of parent_id on probuy.channel_categories
for each row execute function probuy.prevent_category_cycles();

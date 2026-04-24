import json
import os
from decimal import Decimal
from typing import Any

import psycopg2
from fastapi import FastAPI, HTTPException, Query, Request
from psycopg2.extras import RealDictCursor

APP_VERSION = os.getenv("APP_VERSION", "0.1.0")
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="ProBuy Product Intelligence API", version=APP_VERSION)


def _get_connection():
    if not DATABASE_URL:
        raise HTTPException(
            status_code=500,
            detail="DATABASE_URL is not configured. Set DATABASE_URL before calling data endpoints.",
        )
    return psycopg2.connect(DATABASE_URL)


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _to_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_json_safe(v) for v in value]
    return value


KNOWN_SEARCH_PARAMS = {"q", "brand", "source", "limit", "offset"}


def _extract_attribute_filters(request: Request) -> dict[str, str]:
    attribute_filters: dict[str, str] = {}
    for key, value in request.query_params.multi_items():
        if key in KNOWN_SEARCH_PARAMS:
            continue
        if value:
            attribute_filters[key.strip().lower()] = value.strip()
    return attribute_filters


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/version")
def version() -> dict[str, str]:
    return {"version": APP_VERSION}


@app.get("/api/search/products")
def search_products(
    request: Request,
    q: str = Query("", description="Search query"),
    brand: str | None = Query(default=None),
    source: str | None = Query(default=None, description="Primary source code, e.g. SCN"),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    attribute_filters = _extract_attribute_filters(request)

    sql = """
    with has_fts as (
        select exists (
            select 1
            from probuy.product_search_documents psd
            join probuy.source_products sp on sp.id = psd.source_product_id and sp.is_active = true
            join probuy.primary_sources src on src.id = sp.source_id and src.is_active = true
            where (
                %(q)s = ''
                or psd.search_vector @@ websearch_to_tsquery('simple', %(q)s)
            )
            and (%(brand)s is null or sp.brand ilike %(brand_like)s)
            and (%(source)s is null or src.code = %(source)s)
            and (
                %(attribute_filters)s::jsonb = '{}'::jsonb
                or psd.attributes @> %(attribute_filters)s::jsonb
            )
        ) as present
    ),
    ranked as (
        select
            sp.id as source_product_id,
            src.code as source_code,
            sp.product_title_en as title,
            sp.brand,
            sp.manufacturer,
            sp.source_model_no as model_number,
            sp.category_en as category,
            coalesce(sp.image_url, '') as primary_image,
            price.list_price,
            price.distributor_cost,
            inv.quantity_available,
            psd.attributes,
            ts_rank(psd.search_vector, websearch_to_tsquery('simple', %(q)s)) as fts_rank,
            similarity(psd.search_text, %(q)s) as fuzzy_rank,
            false as used_fuzzy_fallback
        from probuy.product_search_documents psd
        join probuy.source_products sp on sp.id = psd.source_product_id and sp.is_active = true
        join probuy.primary_sources src on src.id = sp.source_id and src.is_active = true
        left join lateral (
            select spp.list_price, spp.distributor_cost
            from probuy.source_product_prices spp
            where spp.source_product_id = sp.id
            order by coalesce(spp.effective_at, spp.pricing_update_date, spp.updated_at) desc
            limit 1
        ) price on true
        left join lateral (
            select spi.quantity_available
            from probuy.source_product_inventory spi
            where spi.source_product_id = sp.id
            order by coalesce(spi.inventory_update_date, spi.updated_at) desc
            limit 1
        ) inv on true
        where (
            %(q)s = ''
            or psd.search_vector @@ websearch_to_tsquery('simple', %(q)s)
        )
        and (%(brand)s is null or sp.brand ilike %(brand_like)s)
        and (%(source)s is null or src.code = %(source)s)
        and (
            %(attribute_filters)s::jsonb = '{}'::jsonb
            or psd.attributes @> %(attribute_filters)s::jsonb
        )

        union all

        select
            sp.id as source_product_id,
            src.code as source_code,
            sp.product_title_en as title,
            sp.brand,
            sp.manufacturer,
            sp.source_model_no as model_number,
            sp.category_en as category,
            coalesce(sp.image_url, '') as primary_image,
            price.list_price,
            price.distributor_cost,
            inv.quantity_available,
            psd.attributes,
            0 as fts_rank,
            greatest(
                similarity(psd.search_text, %(q)s),
                similarity(sp.product_title_en, %(q)s),
                similarity(coalesce(sp.source_model_no, ''), %(q)s)
            ) as fuzzy_rank,
            true as used_fuzzy_fallback
        from probuy.product_search_documents psd
        join probuy.source_products sp on sp.id = psd.source_product_id and sp.is_active = true
        join probuy.primary_sources src on src.id = sp.source_id and src.is_active = true
        left join lateral (
            select spp.list_price, spp.distributor_cost
            from probuy.source_product_prices spp
            where spp.source_product_id = sp.id
            order by coalesce(spp.effective_at, spp.pricing_update_date, spp.updated_at) desc
            limit 1
        ) price on true
        left join lateral (
            select spi.quantity_available
            from probuy.source_product_inventory spi
            where spi.source_product_id = sp.id
            order by coalesce(spi.inventory_update_date, spi.updated_at) desc
            limit 1
        ) inv on true
        where %(q)s <> ''
        and not (select present from has_fts)
        and (
            psd.search_text %% %(q)s
            or sp.product_title_en %% %(q)s
            or coalesce(sp.source_model_no, '') %% %(q)s
        )
        and (%(brand)s is null or sp.brand ilike %(brand_like)s)
        and (%(source)s is null or src.code = %(source)s)
        and (
            %(attribute_filters)s::jsonb = '{}'::jsonb
            or psd.attributes @> %(attribute_filters)s::jsonb
        )
    )
    select *
    from ranked
    order by used_fuzzy_fallback asc, fts_rank desc, fuzzy_rank desc, title asc
    limit %(limit)s
    offset %(offset)s;
    """

    params = {
        "q": q.strip(),
        "brand": brand.strip() if brand else None,
        "brand_like": f"%{brand.strip()}%" if brand else None,
        "source": source.strip().upper() if source else None,
        "attribute_filters": json.dumps(attribute_filters),
        "limit": limit,
        "offset": offset,
    }

    with _get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        attrs = row.pop("attributes") or {}
        matched_attributes = {
            k: attrs.get(k)
            for k in attribute_filters.keys()
            if k in attrs
        }
        row["matched_attributes"] = matched_attributes
        results.append(_to_json_safe(dict(row)))

    return {
        "query": q,
        "brand": brand,
        "source": source,
        "attribute_filters": attribute_filters,
        "count": len(results),
        "results": results,
    }


@app.get("/api/products/{source_product_id}")
def get_product(source_product_id: str) -> dict[str, Any]:
    sql = """
    select
        sp.id as source_product_id,
        src.code as source_code,
        sp.product_title_en as title,
        sp.brand,
        sp.manufacturer,
        sp.source_model_no as model_number,
        sp.category_en as category,
        sp.description_en as description,
        sp.product_url,
        sp.image_url as primary_image,
        price.list_price,
        price.distributor_cost,
        inv.quantity_available
    from probuy.source_products sp
    join probuy.primary_sources src on src.id = sp.source_id
    left join lateral (
        select spp.list_price, spp.distributor_cost
        from probuy.source_product_prices spp
        where spp.source_product_id = sp.id
        order by coalesce(spp.effective_at, spp.pricing_update_date, spp.updated_at) desc
        limit 1
    ) price on true
    left join lateral (
        select spi.quantity_available
        from probuy.source_product_inventory spi
        where spi.source_product_id = sp.id
        order by coalesce(spi.inventory_update_date, spi.updated_at) desc
        limit 1
    ) inv on true
    where sp.id = %(source_product_id)s
    limit 1;
    """

    with _get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, {"source_product_id": source_product_id})
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Product not found")

    return _to_json_safe(dict(row))


@app.get("/api/products/{source_product_id}/attributes")
def get_product_attributes(source_product_id: str) -> dict[str, Any]:
    sql = """
    select
        ad.canonical_name,
        ad.display_name,
        ad.data_type,
        coalesce(pav.value_text, pav.value_numeric::text, pav.value_boolean::text) as value,
        coalesce(pav.unit, ad.unit) as unit
    from probuy.product_attribute_values pav
    join probuy.attribute_definitions ad on ad.id = pav.attribute_id
    where pav.source_product_id = %(source_product_id)s
    order by ad.display_name asc;
    """

    with _get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, {"source_product_id": source_product_id})
            rows = cur.fetchall()

    return {
        "source_product_id": source_product_id,
        "count": len(rows),
        "attributes": _to_json_safe([dict(row) for row in rows]),
    }

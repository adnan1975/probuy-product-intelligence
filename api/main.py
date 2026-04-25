import json
import os
from decimal import Decimal
from typing import Any

import psycopg2
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extras import RealDictCursor

from api.meilisearch_client import MeilisearchClient, MeilisearchUnavailableError
from api.search_sync import sync_meilisearch_index

APP_VERSION = os.getenv("APP_VERSION", "0.1.0")
DATABASE_URL = os.getenv("DATABASE_URL")
SEARCH_ENGINE = os.getenv("SEARCH_ENGINE", "supabase").strip().lower()

app = FastAPI(title="ProBuy Product Intelligence API", version=APP_VERSION)


def _to_list(value: str | None, fallback: list[str]) -> list[str]:
    if not value:
        return fallback
    parsed = [item.strip() for item in value.split(",") if item.strip()]
    return parsed or fallback


cors_allowed_origins: list[str] = _to_list(
    os.getenv("CORS_ALLOWED_ORIGINS"),
    [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "https://probuy-frontend.onrender.com",
    ],
)
cors_allow_origin_regex: str | None = os.getenv(
    "CORS_ALLOW_ORIGIN_REGEX",
    r"^http://(localhost|127\.0\.0\.1)(:\d+)?$|^https://.*\.onrender\.com$|^https://.*\.vercel\.app$",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_allowed_origins,
    allow_origin_regex=cors_allow_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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


KNOWN_SEARCH_PARAMS = {
    "q",
    "brand",
    "manufacturer",
    "category",
    "source",
    "color",
    "size",
    "material",
    "stock_status",
    "price_min",
    "price_max",
    "length_min",
    "length_max",
    "width_min",
    "width_max",
    "height_min",
    "height_max",
    "weight_min",
    "weight_max",
    "limit",
    "offset",
}



def _extract_attribute_filters(request: Request) -> dict[str, str]:
    attribute_filters: dict[str, str] = {}
    for key, value in request.query_params.multi_items():
        if key in KNOWN_SEARCH_PARAMS:
            continue
        if value:
            attribute_filters[key.strip().lower()] = value.strip()
    return attribute_filters


def _parse_range_filter(value: float | None) -> float | None:
    return float(value) if value is not None else None


def _build_applied_filters(
    brand: str | None,
    manufacturer: str | None,
    category: str | None,
    source: str | None,
    stock_status: str | None,
    attribute_filters: dict[str, str],
    range_filters: dict[str, float | None],
) -> dict[str, Any]:
    applied_filters: dict[str, Any] = {}
    if brand:
        applied_filters["brand"] = brand.strip()
    if manufacturer:
        applied_filters["manufacturer"] = manufacturer.strip()
    if category:
        applied_filters["category"] = category.strip()
    if source:
        applied_filters["source"] = source.strip().upper()
    if stock_status:
        applied_filters["stock_status"] = stock_status.strip()
    for key, value in attribute_filters.items():
        if value:
            applied_filters[key] = value
    for key, value in range_filters.items():
        if value is not None:
            applied_filters[key] = value
    return applied_filters


def _build_facet_distribution(results: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    facet_distribution: dict[str, dict[str, int]] = {
        "brand": {},
        "manufacturer": {},
        "category": {},
        "source_code": {},
    }

    for row in results:
        for facet in ("brand", "manufacturer", "category", "source_code"):
            value = row.get(facet)
            if value is None:
                continue
            text_value = str(value)
            facet_distribution[facet][text_value] = facet_distribution[facet].get(text_value, 0) + 1

        for key, value in (row.get("matched_attributes") or {}).items():
            if value is None:
                continue
            facet_distribution.setdefault(key, {})
            text_value = str(value)
            facet_distribution[key][text_value] = facet_distribution[key].get(text_value, 0) + 1

    return facet_distribution


def _build_search_response(
    q: str,
    applied_filters: dict[str, Any],
    results: list[dict[str, Any]],
    total_count: int,
    facet_distribution: dict[str, Any],
    engine_used: str,
    fallback_applied: bool,
) -> dict[str, Any]:
    return {
        "query": q,
        "applied_filters": applied_filters,
        "facetDistribution": facet_distribution,
        "total_count": total_count,
        "engine_used": engine_used,
        "fallback_applied": fallback_applied,
        "count": len(results),
        "results": results,
    }


def _search_products_supabase(
    q: str,
    brand: str | None,
    manufacturer: str | None,
    category: str | None,
    source: str | None,
    stock_status: str | None,
    attribute_filters: dict[str, str],
    range_filters: dict[str, float | None],
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    sql = """
    with has_fts as (
        select exists (
            select 1
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
                select spi.quantity_available, spi.stock_status
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
            and (%(manufacturer)s is null or sp.manufacturer ilike %(manufacturer_like)s)
            and (%(category)s is null or sp.category_en ilike %(category_like)s)
            and (%(source)s is null or src.code = %(source)s)
            and (%(stock_status)s is null or coalesce(inv.stock_status, '') ilike %(stock_status_like)s)
            and (
                %(attribute_filters)s::jsonb = '{}'::jsonb
                or psd.attributes @> %(attribute_filters)s::jsonb
            )
            and (%(price_min)s is null or price.list_price >= %(price_min)s)
            and (%(price_max)s is null or price.list_price <= %(price_max)s)
            and (%(length_min)s is null or nullif(substring(coalesce(psd.attributes->>'length', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric >= %(length_min)s)
            and (%(length_max)s is null or nullif(substring(coalesce(psd.attributes->>'length', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric <= %(length_max)s)
            and (%(width_min)s is null or nullif(substring(coalesce(psd.attributes->>'width', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric >= %(width_min)s)
            and (%(width_max)s is null or nullif(substring(coalesce(psd.attributes->>'width', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric <= %(width_max)s)
            and (%(height_min)s is null or nullif(substring(coalesce(psd.attributes->>'height', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric >= %(height_min)s)
            and (%(height_max)s is null or nullif(substring(coalesce(psd.attributes->>'height', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric <= %(height_max)s)
            and (%(weight_min)s is null or nullif(substring(coalesce(psd.attributes->>'weight', coalesce(psd.attributes->>'weight_per_square_yard', '')) from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric >= %(weight_min)s)
            and (%(weight_max)s is null or nullif(substring(coalesce(psd.attributes->>'weight', coalesce(psd.attributes->>'weight_per_square_yard', '')) from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric <= %(weight_max)s)
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
            inv.stock_status,
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
            select spi.quantity_available, spi.stock_status
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
        and (%(manufacturer)s is null or sp.manufacturer ilike %(manufacturer_like)s)
        and (%(category)s is null or sp.category_en ilike %(category_like)s)
        and (%(source)s is null or src.code = %(source)s)
        and (%(stock_status)s is null or coalesce(inv.stock_status, '') ilike %(stock_status_like)s)
        and (
            %(attribute_filters)s::jsonb = '{}'::jsonb
            or psd.attributes @> %(attribute_filters)s::jsonb
        )
        and (%(price_min)s is null or price.list_price >= %(price_min)s)
        and (%(price_max)s is null or price.list_price <= %(price_max)s)
        and (%(length_min)s is null or nullif(substring(coalesce(psd.attributes->>'length', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric >= %(length_min)s)
        and (%(length_max)s is null or nullif(substring(coalesce(psd.attributes->>'length', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric <= %(length_max)s)
        and (%(width_min)s is null or nullif(substring(coalesce(psd.attributes->>'width', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric >= %(width_min)s)
        and (%(width_max)s is null or nullif(substring(coalesce(psd.attributes->>'width', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric <= %(width_max)s)
        and (%(height_min)s is null or nullif(substring(coalesce(psd.attributes->>'height', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric >= %(height_min)s)
        and (%(height_max)s is null or nullif(substring(coalesce(psd.attributes->>'height', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric <= %(height_max)s)
        and (%(weight_min)s is null or nullif(substring(coalesce(psd.attributes->>'weight', coalesce(psd.attributes->>'weight_per_square_yard', '')) from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric >= %(weight_min)s)
        and (%(weight_max)s is null or nullif(substring(coalesce(psd.attributes->>'weight', coalesce(psd.attributes->>'weight_per_square_yard', '')) from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric <= %(weight_max)s)

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
            inv.stock_status,
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
            select spi.quantity_available, spi.stock_status
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
        and (%(manufacturer)s is null or sp.manufacturer ilike %(manufacturer_like)s)
        and (%(category)s is null or sp.category_en ilike %(category_like)s)
        and (%(source)s is null or src.code = %(source)s)
        and (%(stock_status)s is null or coalesce(inv.stock_status, '') ilike %(stock_status_like)s)
        and (
            %(attribute_filters)s::jsonb = '{}'::jsonb
            or psd.attributes @> %(attribute_filters)s::jsonb
        )
        and (%(price_min)s is null or price.list_price >= %(price_min)s)
        and (%(price_max)s is null or price.list_price <= %(price_max)s)
        and (%(length_min)s is null or nullif(substring(coalesce(psd.attributes->>'length', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric >= %(length_min)s)
        and (%(length_max)s is null or nullif(substring(coalesce(psd.attributes->>'length', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric <= %(length_max)s)
        and (%(width_min)s is null or nullif(substring(coalesce(psd.attributes->>'width', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric >= %(width_min)s)
        and (%(width_max)s is null or nullif(substring(coalesce(psd.attributes->>'width', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric <= %(width_max)s)
        and (%(height_min)s is null or nullif(substring(coalesce(psd.attributes->>'height', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric >= %(height_min)s)
        and (%(height_max)s is null or nullif(substring(coalesce(psd.attributes->>'height', '') from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric <= %(height_max)s)
        and (%(weight_min)s is null or nullif(substring(coalesce(psd.attributes->>'weight', coalesce(psd.attributes->>'weight_per_square_yard', '')) from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric >= %(weight_min)s)
        and (%(weight_max)s is null or nullif(substring(coalesce(psd.attributes->>'weight', coalesce(psd.attributes->>'weight_per_square_yard', '')) from '([0-9]+(?:\\.[0-9]+)?)'), '')::numeric <= %(weight_max)s)
    )
    select ranked.*, count(*) over() as total_count
    from ranked
    order by used_fuzzy_fallback asc, fts_rank desc, fuzzy_rank desc, title asc
    limit %(limit)s
    offset %(offset)s;
    """

    params = {
        "q": q.strip(),
        "brand": brand.strip() if brand else None,
        "brand_like": f"%{brand.strip()}%" if brand else None,
        "manufacturer": manufacturer.strip() if manufacturer else None,
        "manufacturer_like": f"%{manufacturer.strip()}%" if manufacturer else None,
        "category": category.strip() if category else None,
        "category_like": f"%{category.strip()}%" if category else None,
        "source": source.strip().upper() if source else None,
        "stock_status": stock_status.strip() if stock_status else None,
        "stock_status_like": f"%{stock_status.strip()}%" if stock_status else None,
        "attribute_filters": json.dumps(attribute_filters),
        "limit": limit,
        "offset": offset,
        **range_filters,
    }

    with _get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    results: list[dict[str, Any]] = []
    total_count = 0
    for row in rows:
        total_count = int(row.pop("total_count", 0) or 0)
        attrs = row.pop("attributes") or {}
        matched_attributes = {
            k: attrs.get(k)
            for k in attribute_filters.keys()
            if k in attrs
        }
        row["matched_attributes"] = matched_attributes
        results.append(_to_json_safe(dict(row)))

    return results, total_count


def _fetch_products_by_ids(
    source_product_ids: list[str],
    attribute_filters: dict[str, str],
) -> list[dict[str, Any]]:
    if not source_product_ids:
        return []

    sql = """
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
        inv.stock_status,
        psd.attributes
    from probuy.source_products sp
    join probuy.primary_sources src on src.id = sp.source_id and src.is_active = true
    left join probuy.product_search_documents psd on psd.source_product_id = sp.id
    left join lateral (
        select spp.list_price, spp.distributor_cost
        from probuy.source_product_prices spp
        where spp.source_product_id = sp.id
        order by coalesce(spp.effective_at, spp.pricing_update_date, spp.updated_at) desc
        limit 1
    ) price on true
    left join lateral (
        select spi.quantity_available, spi.stock_status
        from probuy.source_product_inventory spi
        where spi.source_product_id = sp.id
        order by coalesce(spi.inventory_update_date, spi.updated_at) desc
        limit 1
    ) inv on true
    where sp.is_active = true and sp.id = any(%(source_product_ids)s::uuid[]);
    """

    with _get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, {"source_product_ids": source_product_ids})
            rows = cur.fetchall()

    row_by_id = {str(row["source_product_id"]): row for row in rows}
    ordered_results: list[dict[str, Any]] = []

    for product_id in source_product_ids:
        row = row_by_id.get(product_id)
        if not row:
            continue
        attrs = row.pop("attributes") or {}
        matched_attributes = {
            k: attrs.get(k)
            for k in attribute_filters.keys()
            if k in attrs
        }
        row["matched_attributes"] = matched_attributes
        ordered_results.append(_to_json_safe(dict(row)))

    return ordered_results


def _search_products_meilisearch(
    q: str,
    brand: str | None,
    manufacturer: str | None,
    category: str | None,
    source: str | None,
    stock_status: str | None,
    attribute_filters: dict[str, str],
    range_filters: dict[str, float | None],
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
    client = MeilisearchClient.from_env()
    meili_response = client.search_products(
        query=q,
        brand=brand,
        manufacturer=manufacturer,
        category=category,
        source=source,
        stock_status=stock_status,
        attribute_filters=attribute_filters,
        range_filters=range_filters,
        limit=limit,
        offset=offset,
    )
    source_product_ids = [
        str(hit.get("source_product_id"))
        for hit in meili_response.get("hits", [])
        if hit.get("source_product_id")
    ]
    total_count = int(meili_response.get("estimatedTotalHits") or meili_response.get("totalHits") or 0)
    facet_distribution = meili_response.get("facetDistribution") or {}
    return _fetch_products_by_ids(source_product_ids, attribute_filters), total_count, facet_distribution


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/version")
def version() -> dict[str, str]:
    return {"version": APP_VERSION}


@app.get("/api/search/health")
def search_health() -> dict[str, Any]:
    engine = SEARCH_ENGINE if SEARCH_ENGINE in {"supabase", "meilisearch"} else "supabase"
    response: dict[str, Any] = {
        "status": "ok",
        "configured_engine": engine,
        "fallback_engine": "supabase",
    }

    if engine == "meilisearch":
        try:
            health_status = MeilisearchClient.from_env().health()
            response["meilisearch"] = {
                "status": "ok",
                "details": health_status,
            }
        except MeilisearchUnavailableError as exc:
            response["status"] = "degraded"
            response["meilisearch"] = {
                "status": "unavailable",
                "error": str(exc),
            }

    return response


@app.post("/sync/start")
def start_sync() -> dict[str, Any]:
    try:
        return {"status": "ok", "sync": sync_meilisearch_index()}
    except (MeilisearchUnavailableError, ValueError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/search/products")
def search_products(
    request: Request,
    q: str = Query("", description="Search query"),
    brand: str | None = Query(default=None),
    manufacturer: str | None = Query(default=None),
    category: str | None = Query(default=None),
    source: str | None = Query(default=None, description="Primary source code, e.g. SCN"),
    color: str | None = Query(default=None),
    size: str | None = Query(default=None),
    material: str | None = Query(default=None),
    stock_status: str | None = Query(default=None),
    price_min: float | None = Query(default=None),
    price_max: float | None = Query(default=None),
    length_min: float | None = Query(default=None),
    length_max: float | None = Query(default=None),
    width_min: float | None = Query(default=None),
    width_max: float | None = Query(default=None),
    height_min: float | None = Query(default=None),
    height_max: float | None = Query(default=None),
    weight_min: float | None = Query(default=None),
    weight_max: float | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    attribute_filters = _extract_attribute_filters(request)
    if color:
        attribute_filters["color"] = color.strip()
    if size:
        attribute_filters["size"] = size.strip()
    if material:
        attribute_filters["material"] = material.strip()

    range_filters = {
        "price_min": _parse_range_filter(price_min),
        "price_max": _parse_range_filter(price_max),
        "length_min": _parse_range_filter(length_min),
        "length_max": _parse_range_filter(length_max),
        "width_min": _parse_range_filter(width_min),
        "width_max": _parse_range_filter(width_max),
        "height_min": _parse_range_filter(height_min),
        "height_max": _parse_range_filter(height_max),
        "weight_min": _parse_range_filter(weight_min),
        "weight_max": _parse_range_filter(weight_max),
    }

    engine = SEARCH_ENGINE if SEARCH_ENGINE in {"supabase", "meilisearch"} else "supabase"
    fallback_applied = False
    applied_filters = _build_applied_filters(
        brand=brand,
        manufacturer=manufacturer,
        category=category,
        source=source,
        stock_status=stock_status,
        attribute_filters=attribute_filters,
        range_filters=range_filters,
    )

    meili_unsupported_filters = any(
        range_filters.get(key) is not None
        for key in ("length_min", "length_max", "width_min", "width_max", "height_min", "height_max", "weight_min", "weight_max")
    ) or (stock_status is not None)

    if engine == "meilisearch" and not meili_unsupported_filters:
        try:
            results, total_count, facet_distribution = _search_products_meilisearch(
                q=q,
                brand=brand,
                manufacturer=manufacturer,
                category=category,
                source=source,
                stock_status=stock_status,
                attribute_filters=attribute_filters,
                range_filters=range_filters,
                limit=limit,
                offset=offset,
            )
            return _build_search_response(
                q=q,
                applied_filters=applied_filters,
                results=results,
                total_count=total_count,
                facet_distribution=facet_distribution,
                engine_used="meilisearch",
                fallback_applied=False,
            )
        except MeilisearchUnavailableError:
            fallback_applied = True
    elif engine == "meilisearch" and meili_unsupported_filters:
        fallback_applied = True

    results, total_count = _search_products_supabase(
        q=q,
        brand=brand,
        manufacturer=manufacturer,
        category=category,
        source=source,
        stock_status=stock_status,
        attribute_filters=attribute_filters,
        range_filters=range_filters,
        limit=limit,
        offset=offset,
    )

    return _build_search_response(
        q=q,
        applied_filters=applied_filters,
        results=results,
        total_count=total_count,
        facet_distribution=_build_facet_distribution(results),
        engine_used="supabase",
        fallback_applied=fallback_applied,
    )


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
        inv.quantity_available,
        inv.stock_status
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
        select spi.quantity_available, spi.stock_status
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

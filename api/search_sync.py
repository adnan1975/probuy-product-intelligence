import logging
import os
import time
from decimal import Decimal
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from api.meilisearch_client import MeilisearchClient, MeilisearchUnavailableError

FILTERABLE_ATTRIBUTES = [
    "source_code",
    "brand",
    "manufacturer",
    "category",
    "stock_status",
    "price",
    "attributes.color",
    "attributes.size",
    "attributes.material",
    "attributes.length",
]

logger = logging.getLogger(__name__)

SEARCHABLE_ATTRIBUTES = [
    "title",
    "brand",
    "manufacturer",
    "model_no",
    "search_text",
]


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _to_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_json_safe(v) for v in value]
    return value


def _derive_inventory_status(quantity_available: Any) -> str:
    if quantity_available is None:
        return "unknown"
    try:
        quantity = int(quantity_available)
    except (TypeError, ValueError):
        return "unknown"
    return "in_stock" if quantity > 0 else "out_of_stock"


def _fetch_search_documents(database_url: str, statement_timeout_ms: int) -> list[dict[str, Any]]:
    sql = """
    select
        sp.id as source_product_id,
        src.code as source_code,
        sp.product_title_en as title,
        sp.brand,
        sp.manufacturer,
        sp.source_model_no as model_no,
        sp.category_en as category,
        psd.search_text,
        coalesce(psd.attributes, '{}'::jsonb) as attributes,
        price.list_price as price,
        inv.quantity_available,
        case
            when inv.quantity_available is null then 'unknown'
            when inv.quantity_available > 0 then 'in_stock'
            else 'out_of_stock'
        end as inventory_status
    from probuy.product_search_documents psd
    join probuy.source_products sp on sp.id = psd.source_product_id and sp.is_active = true
    join probuy.primary_sources src on src.id = sp.source_id and src.is_active = true
    left join lateral (
        select spp.list_price
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
    order by sp.id;
    """

    fetch_started = time.time()
    logger.info("search_sync.fetch_start statement_timeout_ms=%s", statement_timeout_ms)
    with psycopg2.connect(database_url) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("set local statement_timeout = %s", (statement_timeout_ms,))
            cur.execute(sql)
            rows = cur.fetchall()
    logger.info("search_sync.fetch_complete row_count=%s elapsed_seconds=%.3f", len(rows), time.time() - fetch_started)

    documents: list[dict[str, Any]] = []
    for row in rows:
        doc = _to_json_safe(dict(row))
        doc["source_product_id"] = str(doc["source_product_id"])
        doc["attributes"] = doc.get("attributes") or {}
        doc["inventory_status"] = doc.get("inventory_status") or _derive_inventory_status(doc.get("quantity_available"))
        doc["stock_status"] = doc.get("stock_status")
        doc.pop("quantity_available", None)
        documents.append(doc)

    return documents


def sync_meilisearch_index() -> dict[str, Any]:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not configured.")

    statement_timeout_ms = int(os.getenv("SYNC_DB_STATEMENT_TIMEOUT_MS", "300000"))
    if statement_timeout_ms <= 0:
        raise ValueError("SYNC_DB_STATEMENT_TIMEOUT_MS must be a positive integer.")

    sync_started = time.time()
    logger.info("search_sync.run_start")

    client = MeilisearchClient.from_env()
    documents = _fetch_search_documents(database_url, statement_timeout_ms)
    logger.info("search_sync.documents_prepared count=%s", len(documents))

    task = client.add_documents(documents, primary_key="source_product_id")
    logger.info("search_sync.meilisearch_documents_enqueued task=%s", task)
    filterable_task = client.update_filterable_attributes(FILTERABLE_ATTRIBUTES)
    searchable_task = client.update_searchable_attributes(SEARCHABLE_ATTRIBUTES)
    logger.info("search_sync.meilisearch_settings_updated filterable_task=%s searchable_task=%s", filterable_task, searchable_task)
    logger.info("search_sync.run_complete elapsed_seconds=%.3f", time.time() - sync_started)

    return {
        "index_name": client.index_name,
        "documents_indexed": len(documents),
        "filterable_attributes": FILTERABLE_ATTRIBUTES,
        "searchable_attributes": SEARCHABLE_ATTRIBUTES,
        "tasks": {
            "documents": task,
            "filterable_attributes": filterable_task,
            "searchable_attributes": searchable_task,
        },
    }


__all__ = ["sync_meilisearch_index", "MeilisearchUnavailableError"]

#!/usr/bin/env python3
import os
import time
import json
import hashlib
from typing import Dict

import psycopg2

TABLE_DELETE_ORDER = [
    "probuy.product_search_documents",
    "probuy.product_attribute_values",
    "probuy.source_product_inventory",
    "probuy.source_product_prices",
    "probuy.source_products",
    "probuy.source_locations",
    "probuy.attribute_definitions",
    "probuy.primary_sources",
]


def get_conn():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")
    return psycopg2.connect(database_url)


def main():
    started = time.time()
    counts: Dict[str, int] = {}

    with get_conn() as conn:
        with conn.cursor() as cur:
            for table in TABLE_DELETE_ORDER:
                cur.execute(f"DELETE FROM {table};")
                counts[table] = cur.rowcount

    elapsed = round(time.time() - started, 3)
    payload = {
        "event": "recon_purge_completed",
        "elapsed_seconds": elapsed,
        "deleted_counts": counts,
    }
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()

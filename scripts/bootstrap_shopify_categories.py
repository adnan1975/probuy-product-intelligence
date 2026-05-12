import csv
import os
from dataclasses import dataclass

import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")
DEFAULT_CHANNEL_CODE = "SHOPIFY"


@dataclass
class BootstrapStats:
    rows_read: int = 0
    category_paths_seen: int = 0
    categories_upserted: int = 0
    mappings_upserted: int = 0
    mappings_missing_product: int = 0


def _slugify(name: str) -> str:
    return "-".join(name.strip().lower().replace("&", "and").replace("/", " ").split())


def bootstrap_shopify_categories(csv_path: str, channel_code: str = DEFAULT_CHANNEL_CODE) -> dict:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL must be set")

    stats = BootstrapStats()
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("select id from probuy.sales_channels where code = %s", (channel_code.upper(),))
            channel = cur.fetchone()
            if not channel:
                raise RuntimeError(f"Sales channel not found: {channel_code}")
            channel_id = channel["id"]

            with open(csv_path, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    stats.rows_read += 1
                    category_path = (row.get("Product Category") or "").strip()
                    sku = (row.get("Variant SKU") or "").strip()
                    if not category_path:
                        continue
                    stats.category_paths_seen += 1

                    parent_id = None
                    leaf_category_id = None
                    for part in [p.strip() for p in category_path.split(">") if p.strip()]:
                        slug = _slugify(part)
                        cur.execute(
                            """
                            insert into probuy.channel_categories (
                                channel_id, parent_id, slug, name, is_active, deleted_at
                            ) values (%s, %s, %s, %s, true, null)
                            on conflict (channel_id, slug)
                            do update set
                                parent_id = excluded.parent_id,
                                name = excluded.name,
                                is_active = true,
                                deleted_at = null,
                                updated_at = now()
                            returning id;
                            """,
                            (channel_id, parent_id, slug, part),
                        )
                        leaf_category_id = cur.fetchone()["id"]
                        parent_id = leaf_category_id
                        stats.categories_upserted += 1

                    if sku and leaf_category_id:
                        cur.execute(
                            """
                            select sp.id
                            from probuy.source_products sp
                            where sp.source_product_key = %s
                            limit 1
                            """,
                            (sku,),
                        )
                        product = cur.fetchone()
                        if not product:
                            stats.mappings_missing_product += 1
                            continue
                        product_id = product["id"]
                        cur.execute(
                            "update probuy.product_category_mappings set is_primary = false, updated_at = now() where source_product_id = %s",
                            (product_id,),
                        )
                        cur.execute(
                            """
                            insert into probuy.product_category_mappings (
                                source_product_id, channel_category_id, mapping_source, is_primary
                            ) values (%s, %s, 'IMPORT', true)
                            on conflict (source_product_id, channel_category_id)
                            do update set is_primary = true, mapping_source = 'IMPORT', updated_at = now();
                            """,
                            (product_id, leaf_category_id),
                        )
                        stats.mappings_upserted += 1

    return stats.__dict__


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Bootstrap Shopify categories and mappings from export CSV.")
    parser.add_argument("--csv-path", required=True)
    parser.add_argument("--channel-code", default=DEFAULT_CHANNEL_CODE)
    args = parser.parse_args()
    print(bootstrap_shopify_categories(args.csv_path, args.channel_code))

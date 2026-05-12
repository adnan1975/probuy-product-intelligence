import logging
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass

import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")
SHOPIFY_CHANNEL_CODE = "SHOPIFY"
SCN_SOURCE_CODE = "SCN"
logger = logging.getLogger("bootstrap_source_channel_category_mappings")


@dataclass
class BootstrapReport:
    total_candidate_rows: int = 0
    total_scn_categories_found: int = 0
    mappings_created: int = 0
    mappings_updated: int = 0
    ambiguous_scn_categories: int = 0
    skipped_missing_shopify_mapping: int = 0
    skipped_missing_scn_category_text: int = 0


def _normalize_category(value: str) -> tuple[str, str]:
    display_name = re.sub(r"\s+", " ", value.strip())
    normalized_key = display_name.lower()
    return normalized_key, display_name


def bootstrap_source_channel_category_mappings() -> dict:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL must be set")

    report = BootstrapReport()

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                select sc.id as channel_id
                from probuy.sales_channels sc
                where sc.code = %s
                limit 1
                """,
                (SHOPIFY_CHANNEL_CODE,),
            )
            channel = cur.fetchone()
            if not channel:
                raise RuntimeError("SHOPIFY sales channel was not found")
            shopify_channel_id = channel["channel_id"]

            cur.execute(
                """
                select ps.id as source_id
                from probuy.primary_sources ps
                where ps.code = %s
                limit 1
                """,
                (SCN_SOURCE_CODE,),
            )
            source = cur.fetchone()
            if not source:
                raise RuntimeError("SCN primary source was not found")
            scn_source_id = source["source_id"]

            cur.execute(
                """
                select
                    sp.id as source_product_id,
                    sp.category_en,
                    pcm.channel_category_id
                from probuy.source_products sp
                join probuy.product_channel_publications pcp
                  on pcp.source_product_id = sp.id
                 and pcp.channel_id = %s
                left join probuy.product_category_mappings pcm
                  on pcm.source_product_id = sp.id
                where sp.source_id = %s
                """,
                (shopify_channel_id, scn_source_id),
            )
            candidate_rows = cur.fetchall()
            report.total_candidate_rows = len(candidate_rows)

            category_counts: dict[str, Counter] = defaultdict(Counter)
            normalized_categories: dict[str, str] = {}

            for row in candidate_rows:
                raw_category = row["category_en"]
                channel_category_id = row["channel_category_id"]

                if raw_category is None or raw_category.strip() == "":
                    report.skipped_missing_scn_category_text += 1
                    continue

                normalized_key, display_name = _normalize_category(raw_category)
                normalized_categories[normalized_key] = display_name

                if channel_category_id is None:
                    report.skipped_missing_shopify_mapping += 1
                    continue

                category_counts[normalized_key][channel_category_id] += 1

            report.total_scn_categories_found = len(normalized_categories)

            source_category_ids: dict[str, str] = {}
            for normalized_key, display_name in normalized_categories.items():
                cur.execute(
                    """
                    insert into probuy.source_categories (source_id, external_category_key, name, metadata)
                    values (%s, %s, %s, jsonb_build_object('normalized_from', 'source_products.category_en'))
                    on conflict (source_id, external_category_key)
                    do update set
                        name = excluded.name,
                        metadata = probuy.source_categories.metadata || excluded.metadata,
                        updated_at = now()
                    returning id
                    """,
                    (scn_source_id, normalized_key, display_name),
                )
                source_category_ids[normalized_key] = cur.fetchone()["id"]

            for normalized_key, channel_counter in category_counts.items():
                source_category_id = source_category_ids[normalized_key]
                ranked_categories = channel_counter.most_common()
                if len(ranked_categories) > 1:
                    report.ambiguous_scn_categories += 1

                for index, (channel_category_id, _) in enumerate(ranked_categories):
                    is_primary = index == 0
                    cur.execute(
                        """
                        insert into probuy.channel_category_source_category_mappings (
                            source_category_id,
                            channel_category_id,
                            mapping_source,
                            is_primary
                        ) values (%s, %s, 'IMPORT', %s)
                        on conflict (source_category_id, channel_category_id)
                        do update set
                            mapping_source = 'IMPORT',
                            is_primary = excluded.is_primary,
                            updated_at = now()
                        returning (xmax = 0) as inserted
                        """,
                        (source_category_id, channel_category_id, is_primary),
                    )
                    if cur.fetchone()["inserted"]:
                        report.mappings_created += 1
                    else:
                        report.mappings_updated += 1

            conn.commit()

    payload = report.__dict__
    logger.info("bootstrap_source_channel_category_mappings.report %s", payload)
    return payload


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Bootstrap source-to-channel category mappings from product-level mappings")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    print(bootstrap_source_channel_category_mappings())

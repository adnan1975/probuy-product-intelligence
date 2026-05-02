#!/usr/bin/env python3
"""
sync_channel_publications.py

Generic sales-channel publication sync for ProBuy.

Primary current use case:
- Read Shopify export CSV files from ./shopify_export:
    product_export_1.csv, product_export_2.csv, ...
    products_export_1.csv, products_export_2.csv, ...
- Match exported rows back to probuy.source_products.
- Upsert rows into probuy.product_channel_publications for a channel such as SHOPIFY.
- Write operational CSV reports into ./output.

Design:
- Generic by channel_code, so the same publication table can support SHOPIFY, METHOD, AMAZON, etc.
- For Shopify, the safest match is Variant SKU -> source_products.source_product_key.
- Shopify Handle is stored in sync_metadata, not treated as a true Shopify numeric product ID.

Expected tables:
- probuy.sales_channels
- probuy.product_channel_publications
- probuy.source_products
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

import pandas as pd
import psycopg2
from psycopg2.extras import Json, execute_values


LOG = logging.getLogger("channel_publication_sync")

VALID_PUBLISH_METHODS = {"AUTO", "MANUAL", "API_SYNC", "BULK_FEED"}
VALID_PUBLICATION_STATUSES = {"NOT_PUBLISHED", "QUEUED", "PUBLISHED", "UNPUBLISHED", "FAILED"}


def setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def clean_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def clean_sku(value: object) -> str:
    """Normalize common CSV/Excel SKU artifacts."""
    s = clean_text(value)
    s = re.sub(r"\.0$", "", s)
    if "e" in s.lower():
        try:
            return format(int(float(s)), "d")
        except Exception:
            return s
    return s


def slugify(value: object) -> str:
    s = clean_text(value).lower()
    s = re.sub(r"[^a-z0-9\s\-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"\-+", "-", s).strip("-")
    return s


def validate_choice(name: str, value: str, allowed: set[str]) -> str:
    value = clean_text(value).upper()
    if value not in allowed:
        raise ValueError(f"Invalid {name}={value}. Allowed: {sorted(allowed)}")
    return value


def find_export_files(export_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    for pattern in ("product_export_*.csv", "products_export_*.csv"):
        candidates.extend(export_dir.glob(pattern))

    def number_key(path: Path) -> tuple[int, str]:
        m = re.search(r"_(\d+)\.csv$", path.name)
        return (int(m.group(1)) if m else 999999, path.name.lower())

    return sorted(set(candidates), key=number_key)


@dataclass(frozen=True)
class ChannelExportRow:
    source_file: str
    row_number: int
    match_key: str
    handle: str
    title: str
    status: str
    published: str


def iter_shopify_rows(export_files: list[Path], chunksize: int = 5000) -> Iterator[ChannelExportRow]:
    """
    Shopify-specific reader.

    Match key is Variant SKU because the converter writes SCN Prod into Variant SKU.
    Handle is kept as metadata and optional fallback evidence.
    """
    required = {"Handle", "Variant SKU"}

    for path in export_files:
        LOG.info("Reading Shopify export: %s", path)
        for chunk_index, df in enumerate(
            pd.read_csv(path, dtype=str, chunksize=chunksize, keep_default_na=False),
            start=1,
        ):
            missing = required - set(df.columns)
            if missing:
                raise ValueError(f"{path} is missing required column(s): {sorted(missing)}")

            for i, row in df.iterrows():
                match_key = clean_sku(row.get("Variant SKU"))
                handle = clean_text(row.get("Handle"))

                # Shopify export may contain image/title-only continuation rows.
                # They are not useful for source product matching unless a Variant SKU exists.
                if not match_key:
                    continue

                yield ChannelExportRow(
                    source_file=path.name,
                    row_number=int(i) + 2,
                    match_key=match_key,
                    handle=handle,
                    title=clean_text(row.get("Title")),
                    status=clean_text(row.get("Status")),
                    published=clean_text(row.get("Published")),
                )

            LOG.debug("Processed chunk %s from %s", chunk_index, path.name)


def get_conn(args: argparse.Namespace):
    dsn = args.database_url or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is required. Set env var DATABASE_URL or pass --database-url.")
    return psycopg2.connect(dsn)


def table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
            """,
            (schema, table),
        )
        return cur.fetchone() is not None


def column_exists(conn, schema: str, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            """,
            (schema, table, column),
        )
        return cur.fetchone() is not None


def validate_schema(conn, schema: str, product_table: str, product_key_col: str) -> None:
    for table in ["sales_channels", "product_channel_publications", product_table]:
        if not table_exists(conn, schema, table):
            raise RuntimeError(f'Missing table "{schema}"."{table}".')

    if not column_exists(conn, schema, product_table, product_key_col):
        raise RuntimeError(f'Missing product key column "{schema}"."{product_table}"."{product_key_col}".')


def get_or_create_channel_id(conn, schema: str, code: str, name: str, create_missing: bool) -> str:
    code = clean_text(code).upper()
    name = clean_text(name) or code.title()

    with conn.cursor() as cur:
        cur.execute(f'SELECT id FROM "{schema}".sales_channels WHERE code = %s', (code,))
        row = cur.fetchone()
        if row:
            LOG.info("Using sales channel %s", code)
            return str(row[0])

        if not create_missing:
            raise RuntimeError(
                f"Sales channel {code!r} does not exist. Insert it first or pass --create-channel-if-missing."
            )

        cur.execute(
            f"""
            INSERT INTO "{schema}".sales_channels (code, name, is_active)
            VALUES (%s, %s, true)
            RETURNING id
            """,
            (code, name),
        )
        channel_id = str(cur.fetchone()[0])
        LOG.info("Created sales channel %s", code)
        return channel_id


def create_temp_export_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE tmp_channel_export (
                match_key text,
                handle text,
                title text,
                status text,
                published text,
                source_file text,
                row_number integer
            ) ON COMMIT DROP
            """
        )


def load_rows_to_temp(conn, rows: Iterable[ChannelExportRow], page_size: int = 5000) -> int:
    sql = """
        INSERT INTO tmp_channel_export
            (match_key, handle, title, status, published, source_file, row_number)
        VALUES %s
    """
    total = 0
    batch: list[tuple] = []

    with conn.cursor() as cur:
        for row in rows:
            batch.append((
                row.match_key,
                row.handle,
                row.title,
                row.status,
                row.published,
                row.source_file,
                row.row_number,
            ))
            if len(batch) >= page_size:
                execute_values(cur, sql, batch, page_size=page_size)
                total += len(batch)
                LOG.info("Loaded %s export rows into temp table...", total)
                batch.clear()

        if batch:
            execute_values(cur, sql, batch, page_size=page_size)
            total += len(batch)

        cur.execute("CREATE INDEX ON tmp_channel_export (lower(match_key))")

    LOG.info("Loaded total export rows into temp table: %s", total)
    return total


def upsert_publications(
    conn,
    schema: str,
    product_table: str,
    product_key_col: str,
    channel_id: str,
    channel_code: str,
    publish_method: str,
    publication_status: str,
    dry_run: bool,
) -> int:
    """Upsert matched products into product_channel_publications."""
    if dry_run:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(DISTINCT p.id)
                FROM "{schema}"."{product_table}" p
                JOIN tmp_channel_export e
                  ON lower(e.match_key) = lower(p."{product_key_col}")
                WHERE COALESCE(e.match_key, '') <> ''
                """
            )
            count = int(cur.fetchone()[0])
            LOG.info("[DRY RUN] Would upsert %s publication rows for %s.", count, channel_code)
            return count

    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH matched AS (
                SELECT DISTINCT ON (p.id)
                       p.id AS source_product_id,
                       e.match_key,
                       e.handle,
                       e.title,
                       e.status AS external_status,
                       e.published AS external_published,
                       e.source_file,
                       e.row_number
                  FROM "{schema}"."{product_table}" p
                  JOIN tmp_channel_export e
                    ON lower(e.match_key) = lower(p."{product_key_col}")
                 WHERE COALESCE(e.match_key, '') <> ''
                 ORDER BY p.id, e.source_file, e.row_number
            )
            INSERT INTO "{schema}".product_channel_publications (
                source_product_id,
                channel_id,
                publish_method,
                publication_status,
                is_published,
                external_product_id,
                external_variant_id,
                published_at,
                last_sync_at,
                last_error,
                sync_metadata,
                created_at,
                updated_at
            )
            SELECT
                m.source_product_id,
                %s::uuid AS channel_id,
                %s AS publish_method,
                %s AS publication_status,
                true AS is_published,
                NULLIF(m.handle, '') AS external_product_id,
                NULL AS external_variant_id,
                now() AS published_at,
                now() AS last_sync_at,
                NULL AS last_error,
                jsonb_build_object(
                    'source', 'csv_import',
                    'channel_code', %s,
                    'matched_by', 'variant_sku_to_source_product_key',
                    'match_key', m.match_key,
                    'handle', m.handle,
                    'title', m.title,
                    'external_status', m.external_status,
                    'external_published', m.external_published,
                    'source_file', m.source_file,
                    'row_number', m.row_number
                ) AS sync_metadata,
                now(),
                now()
            FROM matched m
            ON CONFLICT (source_product_id, channel_id)
            DO UPDATE SET
                publish_method = EXCLUDED.publish_method,
                publication_status = EXCLUDED.publication_status,
                is_published = EXCLUDED.is_published,
                external_product_id = COALESCE(EXCLUDED.external_product_id, "{schema}".product_channel_publications.external_product_id),
                external_variant_id = COALESCE(EXCLUDED.external_variant_id, "{schema}".product_channel_publications.external_variant_id),
                published_at = COALESCE("{schema}".product_channel_publications.published_at, EXCLUDED.published_at),
                last_sync_at = EXCLUDED.last_sync_at,
                last_error = NULL,
                sync_metadata = "{schema}".product_channel_publications.sync_metadata || EXCLUDED.sync_metadata,
                updated_at = now()
            """,
            (channel_id, publish_method, publication_status, channel_code),
        )
        count = cur.rowcount

    LOG.info("Upserted %s publication rows for %s.", count, channel_code)
    return count


def mark_missing_as_not_published(
    conn,
    schema: str,
    product_table: str,
    product_key_col: str,
    channel_id: str,
    publish_method: str,
    dry_run: bool,
) -> int:
    """
    Optional: create/update NOT_PUBLISHED rows for active source products missing from export.

    This makes filtering easier because every active product will have a publication row for the channel.
    """
    if dry_run:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM "{schema}"."{product_table}" p
                WHERE COALESCE(p.is_active, true) = true
                  AND NOT EXISTS (
                      SELECT 1 FROM tmp_channel_export e
                      WHERE lower(e.match_key) = lower(p."{product_key_col}")
                  )
                """
            )
            count = int(cur.fetchone()[0])
            LOG.info("[DRY RUN] Would mark %s active products as NOT_PUBLISHED.", count)
            return count

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO "{schema}".product_channel_publications (
                source_product_id,
                channel_id,
                publish_method,
                publication_status,
                is_published,
                published_at,
                last_sync_at,
                last_error,
                sync_metadata,
                created_at,
                updated_at
            )
            SELECT
                p.id,
                %s::uuid,
                %s,
                'NOT_PUBLISHED',
                false,
                NULL,
                now(),
                NULL,
                jsonb_build_object(
                    'source', 'csv_import',
                    'reason', 'not_found_in_latest_channel_export'
                ),
                now(),
                now()
            FROM "{schema}"."{product_table}" p
            WHERE COALESCE(p.is_active, true) = true
              AND NOT EXISTS (
                  SELECT 1 FROM tmp_channel_export e
                  WHERE lower(e.match_key) = lower(p."{product_key_col}")
              )
            ON CONFLICT (source_product_id, channel_id)
            DO UPDATE SET
                publication_status = CASE
                    WHEN "{schema}".product_channel_publications.is_published = true THEN 'UNPUBLISHED'
                    ELSE 'NOT_PUBLISHED'
                END,
                is_published = false,
                last_sync_at = now(),
                sync_metadata = "{schema}".product_channel_publications.sync_metadata || EXCLUDED.sync_metadata,
                updated_at = now()
            """,
            (channel_id, publish_method),
        )
        count = cur.rowcount

    LOG.info("Marked %s missing active products as not published/unpublished.", count)
    return count


def write_query_to_csv(conn, sql: str, params: tuple, out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with conn.cursor() as cur, out_path.open("w", newline="", encoding="utf-8-sig") as f:
        cur.execute(sql, params)
        writer = csv.writer(f)
        writer.writerow([desc[0] for desc in cur.description])
        count = 0
        while True:
            rows = cur.fetchmany(5000)
            if not rows:
                break
            writer.writerows(rows)
            count += len(rows)
    LOG.info("Wrote %s rows: %s", count, out_path)
    return count


def write_outputs(
    conn,
    output_dir: Path,
    schema: str,
    product_table: str,
    product_key_col: str,
    channel_id: str,
    channel_code: str,
) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}

    counts["matched_rows"] = write_query_to_csv(
        conn,
        f"""
        SELECT DISTINCT
               p.id AS source_product_id,
               p."{product_key_col}" AS source_product_key,
               p.source_model_no,
               p.brand,
               p.manufacturer,
               p.product_title_en,
               pub.publication_status,
               pub.is_published,
               pub.publish_method,
               pub.external_product_id,
               pub.published_at,
               pub.last_sync_at,
               e.match_key AS export_match_key,
               e.handle,
               e.status AS export_status,
               e.published AS export_published,
               e.source_file,
               e.row_number
          FROM "{schema}"."{product_table}" p
          JOIN tmp_channel_export e
            ON lower(e.match_key) = lower(p."{product_key_col}")
          LEFT JOIN "{schema}".product_channel_publications pub
            ON pub.source_product_id = p.id
           AND pub.channel_id = %s::uuid
         WHERE COALESCE(e.match_key, '') <> ''
         ORDER BY p."{product_key_col}", e.source_file, e.row_number
        """,
        (channel_id,),
        output_dir / f"{channel_code.lower()}_matched_rows.csv",
    )

    counts["export_rows_not_found_in_db"] = write_query_to_csv(
        conn,
        f"""
        SELECT DISTINCT
               e.match_key,
               e.handle,
               e.title,
               e.status,
               e.published,
               e.source_file,
               e.row_number
          FROM tmp_channel_export e
          LEFT JOIN "{schema}"."{product_table}" p
            ON lower(e.match_key) = lower(p."{product_key_col}")
         WHERE COALESCE(e.match_key, '') <> ''
           AND p.id IS NULL
         ORDER BY e.source_file, e.row_number
        """,
        (),
        output_dir / f"{channel_code.lower()}_rows_not_found_in_db.csv",
    )

    counts["not_published_candidates"] = write_query_to_csv(
        conn,
        f"""
        SELECT
               p.id AS source_product_id,
               p."{product_key_col}" AS source_product_key,
               p.source_model_no,
               p.brand,
               p.manufacturer,
               p.product_title_en,
               p.description_en,
               p.category_en,
               p.unit_description_en,
               p.product_url,
               COALESCE(pub.publication_status, 'NOT_PUBLISHED') AS publication_status,
               COALESCE(pub.is_published, false) AS is_published,
               pub.last_sync_at,
               p.is_active,
               p.created_at,
               p.updated_at
          FROM "{schema}"."{product_table}" p
          LEFT JOIN "{schema}".product_channel_publications pub
            ON pub.source_product_id = p.id
           AND pub.channel_id = %s::uuid
         WHERE COALESCE(p.is_active, true) = true
           AND COALESCE(pub.is_published, false) = false
         ORDER BY p.product_title_en NULLS LAST, p."{product_key_col}"
        """,
        (channel_id,),
        output_dir / f"{channel_code.lower()}_not_published_candidates.csv",
    )

    pd.DataFrame([{"metric": k, "count": v} for k, v in counts.items()]).to_csv(
        output_dir / f"{channel_code.lower()}_summary.csv",
        index=False,
        encoding="utf-8-sig",
    )
    LOG.info("Wrote summary: %s", output_dir / f"{channel_code.lower()}_summary.csv")
    return counts


def write_search_sql_examples(output_dir: Path, schema: str, product_table: str) -> None:
    sql = f"""-- Search query pattern with channel publication status
-- Replace :channel_code, :publication_status, :search with your app parameters.

SELECT
    p.id,
    p.source_product_key,
    p.source_model_no,
    p.brand,
    p.manufacturer,
    p.product_title_en,
    p.category_en,
    sc.code AS channel_code,
    COALESCE(pub.publication_status, 'NOT_PUBLISHED') AS publication_status,
    COALESCE(pub.is_published, false) AS is_published,
    pub.external_product_id,
    pub.published_at,
    pub.last_sync_at
FROM {schema}.{product_table} p
JOIN {schema}.product_search_documents psd
  ON psd.source_product_id = p.id
LEFT JOIN {schema}.sales_channels sc
  ON sc.code = :channel_code
LEFT JOIN {schema}.product_channel_publications pub
  ON pub.source_product_id = p.id
 AND pub.channel_id = sc.id
WHERE p.is_active = true
  AND (:publication_status IS NULL OR COALESCE(pub.publication_status, 'NOT_PUBLISHED') = :publication_status)
  AND (:is_published IS NULL OR COALESCE(pub.is_published, false) = :is_published)
  AND psd.search_vector @@ plainto_tsquery('english', :search)
ORDER BY p.product_title_en NULLS LAST
LIMIT 50;
"""
    path = output_dir / "search_publication_filter_example.sql"
    path.write_text(sql, encoding="utf-8")
    LOG.info("Wrote search SQL example: %s", path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--export-dir", default=os.getenv("EXPORT_DIR", "shopify_export"))
    parser.add_argument("--output-dir", default=os.getenv("OUTPUT_DIR", "output"))
    parser.add_argument("--schema", default=os.getenv("DB_SCHEMA", "probuy"))
    parser.add_argument("--product-table", default=os.getenv("PRODUCT_TABLE", "source_products"))
    parser.add_argument("--product-key-column", default=os.getenv("PRODUCT_KEY_COLUMN", "source_product_key"))
    parser.add_argument("--channel-code", default=os.getenv("CHANNEL_CODE", "SHOPIFY"))
    parser.add_argument("--channel-name", default=os.getenv("CHANNEL_NAME", "Shopify"))
    parser.add_argument("--create-channel-if-missing", action="store_true")
    parser.add_argument("--publish-method", default=os.getenv("PUBLISH_METHOD", "BULK_FEED"))
    parser.add_argument("--publication-status", default=os.getenv("PUBLICATION_STATUS", "PUBLISHED"))
    parser.add_argument("--mark-missing-not-published", action="store_true")
    parser.add_argument("--chunksize", type=int, default=int(os.getenv("CHUNKSIZE", "5000")))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    setup_logging(args.verbose)

    args.channel_code = clean_text(args.channel_code).upper()
    args.publish_method = validate_choice("publish_method", args.publish_method, VALID_PUBLISH_METHODS)
    args.publication_status = validate_choice("publication_status", args.publication_status, VALID_PUBLICATION_STATUSES)

    export_dir = Path(args.export_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    export_files = find_export_files(export_dir)
    if not export_files:
        raise FileNotFoundError(
            f"No export files found in {export_dir}. Expected product_export_1.csv or products_export_1.csv."
        )
    LOG.info("Found %s export file(s): %s", len(export_files), [p.name for p in export_files])

    conn = get_conn(args)
    try:
        validate_schema(conn, args.schema, args.product_table, args.product_key_column)
        channel_id = get_or_create_channel_id(
            conn,
            schema=args.schema,
            code=args.channel_code,
            name=args.channel_name,
            create_missing=args.create_channel_if_missing,
        )

        create_temp_export_table(conn)
        loaded = load_rows_to_temp(
            conn,
            iter_shopify_rows(export_files, chunksize=args.chunksize),
            page_size=args.chunksize,
        )
        if loaded == 0:
            raise RuntimeError("No usable export rows were loaded.")

        upserted = upsert_publications(
            conn,
            schema=args.schema,
            product_table=args.product_table,
            product_key_col=args.product_key_column,
            channel_id=channel_id,
            channel_code=args.channel_code,
            publish_method=args.publish_method,
            publication_status=args.publication_status,
            dry_run=args.dry_run,
        )

        missing_marked = 0
        if args.mark_missing_not_published:
            missing_marked = mark_missing_as_not_published(
                conn,
                schema=args.schema,
                product_table=args.product_table,
                product_key_col=args.product_key_column,
                channel_id=channel_id,
                publish_method=args.publish_method,
                dry_run=args.dry_run,
            )

        counts = write_outputs(
            conn,
            output_dir=output_dir,
            schema=args.schema,
            product_table=args.product_table,
            product_key_col=args.product_key_column,
            channel_id=channel_id,
            channel_code=args.channel_code,
        )
        write_search_sql_examples(output_dir, args.schema, args.product_table)

        if args.dry_run:
            conn.rollback()
            LOG.info("Dry run complete. No DB changes committed.")
        else:
            conn.commit()
            LOG.info("Committed DB updates.")

        LOG.info(
            "Done. channel=%s upserted=%s missing_marked=%s output_counts=%s",
            args.channel_code,
            upserted,
            missing_marked,
            counts,
        )
        return 0

    except Exception:
        conn.rollback()
        LOG.exception("Failed. Rolled back DB changes.")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

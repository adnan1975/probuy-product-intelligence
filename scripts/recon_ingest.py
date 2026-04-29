#!/usr/bin/env python3
import hashlib
import json
import os
import re
import time
import gc
from datetime import datetime
import logging
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg2
from openpyxl import load_workbook

LOG_EVERY = int(os.getenv("RECON_LOG_EVERY", "5000"))
COMMIT_EVERY = int(os.getenv("RECON_COMMIT_EVERY", "500"))


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def log_progress(stage: str, count: int):
    if count and count % LOG_EVERY == 0:
        logging.info("%s processed=%s", stage, count)

REQUIRED_FILES = {
    "content": "contentlicensing.xlsx",
    "pricing": "pricing.xlsx",
    "inventory": "inventory.xlsx",
}


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def checksum(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def normalize_key(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def row_dict(headers, row):
    out = {}
    for idx, cell in enumerate(row):
        key = headers[idx]
        if not key:
            continue
        out[key] = cell
    return out


def parse_ts(value):
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        v = value.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(v)
        except ValueError:
            return None
    return None


def parse_decimal(value):
    if value is None or value == "":
        return None
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))
    txt = str(value).replace("$", "").replace(",", "").strip()
    if txt == "":
        return None
    try:
        return Decimal(txt)
    except InvalidOperation:
        return None


def maybe_commit(conn, stage: str, processed_rows: int, pending_rows: int) -> int:
    if pending_rows >= COMMIT_EVERY:
        conn.commit()
        logging.info("%s committed processed=%s", stage, processed_rows)
        gc.collect()
        return 0
    return pending_rows


def get_conn():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")
    return psycopg2.connect(database_url)


def ensure_source_and_locations(cur):
    cur.execute(
        """
        insert into probuy.primary_sources (code, name, is_active)
        values ('SCN', 'SCN International', true)
        on conflict (code) do update set name = excluded.name, is_active = excluded.is_active, updated_at = now()
        returning id
        """
    )
    source_id = cur.fetchone()[0]

    locations = [
        ("SCN-CA", "SCN Canada Pricing", "National", "CA", {"source": "pricing.xlsx"}),
        ("MTL", "Montreal Distribution Centre", "QC", "CA", {"source": "inventory.xlsx", "sheet": "MTL"}),
        ("VAN", "Vancouver Distribution Centre", "BC", "CA", {"source": "inventory.xlsx", "sheet": "VAN"}),
        ("EDM", "Edmonton Distribution Centre", "AB", "CA", {"source": "inventory.xlsx", "sheet": "EDM"}),
    ]
    loc_ids = {}
    for code, name, province, country, raw_data in locations:
        cur.execute(
            """
            insert into probuy.source_locations (source_id, code, name, province, country, is_active, raw_data)
            values (%s, %s, %s, %s, %s, true, %s::jsonb)
            on conflict (source_id, code) do update
            set name=excluded.name, province=excluded.province, country=excluded.country, is_active=excluded.is_active, raw_data=excluded.raw_data, updated_at=now()
            returning id
            """,
            (source_id, code, name, province, country, json.dumps(raw_data)),
        )
        loc_ids[code] = cur.fetchone()[0]
    return source_id, loc_ids


def source_products_has_image_url(cur) -> bool:
    cur.execute(
        """
        select exists (
            select 1
            from information_schema.columns
            where table_schema = 'probuy'
              and table_name = 'source_products'
              and column_name = 'image_url'
        )
        """
    )
    return bool(cur.fetchone()[0])


def upsert_product(cur, source_id, key, model_no, brand, manufacturer, title, description, category, unit, product_url, image_url, raw_data, include_image_url: bool):
    if include_image_url:
        cur.execute(
            """
            insert into probuy.source_products
            (source_id, source_product_key, source_model_no, brand, manufacturer, product_title_en, description_en, category_en, unit_description_en, product_url, image_url, is_active, raw_data)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true,%s::jsonb)
            on conflict (source_id, source_product_key) do update
            set source_model_no=excluded.source_model_no,
                brand=excluded.brand,
                manufacturer=excluded.manufacturer,
                product_title_en=excluded.product_title_en,
                description_en=excluded.description_en,
                category_en=excluded.category_en,
                unit_description_en=excluded.unit_description_en,
                product_url=excluded.product_url,
                image_url=excluded.image_url,
                is_active=excluded.is_active,
                raw_data=excluded.raw_data,
                updated_at=now()
            returning id
            """,
            (source_id, key, model_no, brand, manufacturer, title, description, category, unit, product_url, image_url, json.dumps(raw_data, default=str)),
        )
    else:
        cur.execute(
            """
            insert into probuy.source_products
            (source_id, source_product_key, source_model_no, brand, manufacturer, product_title_en, description_en, category_en, unit_description_en, product_url, is_active, raw_data)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true,%s::jsonb)
            on conflict (source_id, source_product_key) do update
            set source_model_no=excluded.source_model_no,
                brand=excluded.brand,
                manufacturer=excluded.manufacturer,
                product_title_en=excluded.product_title_en,
                description_en=excluded.description_en,
                category_en=excluded.category_en,
                unit_description_en=excluded.unit_description_en,
                product_url=excluded.product_url,
                is_active=excluded.is_active,
                raw_data=excluded.raw_data,
                updated_at=now()
            returning id
            """,
            (source_id, key, model_no, brand, manufacturer, title, description, category, unit, product_url, json.dumps(raw_data, default=str)),
        )
    return cur.fetchone()[0]


def main():
    configure_logging()
    started = time.time()
    input_dir = Path("input/data")
    paths = {k: input_dir / v for k, v in REQUIRED_FILES.items()}

    missing = [str(p) for p in paths.values() if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required files: {', '.join(missing)}")

    logging.info(
        "Starting recon ingest; log cadence every %s rows; commit cadence every %s rows",
        LOG_EVERY,
        COMMIT_EVERY,
    )
    file_info = {k: {"path": str(v), "sha256": checksum(v)} for k, v in paths.items()}
    counts = {"products": 0, "attributes": 0, "prices": 0, "inventory": 0, "search_docs": 0}

    with get_conn() as conn:
        with conn.cursor() as cur:
            source_id, loc_ids = ensure_source_and_locations(cur)
            include_image_url = source_products_has_image_url(cur)
            if not include_image_url:
                logging.warning("source_products.image_url column not found; continuing without image_url field")

            # content
            logging.info("Loading content workbook in read-only mode: %s", paths["content"])
            wb = load_workbook(paths["content"], data_only=True, read_only=True)
            ws = wb.active
            headers = [str(c).strip() if c is not None else "" for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
            pending_content = 0
            for row in ws.iter_rows(min_row=2, values_only=True):
                rd = row_dict(headers, row)
                key = str(rd.get("Prod", "")).strip()
                if not key:
                    continue
                product_id = upsert_product(
                    cur,
                    source_id,
                    key,
                    str(rd.get("ManufacturerNumber") or "").strip() or None,
                    rd.get("Brand"),
                    rd.get("Brand"),
                    rd.get("ProductTitle"),
                    None,
                    rd.get("CategoryEnglish"),
                    rd.get("UnitDescription"),
                    None,
                    None,
                    {"source": "contentlicensing.xlsx", "row": rd},
                    include_image_url,
                )
                counts["products"] += 1
                pending_content += 1
                log_progress("content products", counts["products"])

                for i in range(1, 11):
                    an = rd.get(f"AttributeName{i}")
                    av = rd.get(f"AttributeValue{i}")
                    if not an or av in (None, ""):
                        continue
                    canonical = normalize_key(str(an))
                    cur.execute(
                        """
                        insert into probuy.attribute_definitions (canonical_name, display_name, data_type, unit, is_filterable, is_searchable)
                        values (%s,%s,'text',null,true,true)
                        on conflict (canonical_name) do update set display_name=excluded.display_name, updated_at=now()
                        returning id
                        """,
                        (canonical, str(an).strip()),
                    )
                    attr_id = cur.fetchone()[0]
                    cur.execute(
                        """
                        insert into probuy.product_attribute_values
                        (source_product_id, attribute_id, value_text, raw_data)
                        values (%s,%s,%s,%s::jsonb)
                        on conflict (source_product_id, attribute_id) do update
                        set value_text=excluded.value_text, raw_data=excluded.raw_data, updated_at=now()
                        """,
                        (product_id, attr_id, str(av).strip(), json.dumps({"source": "contentlicensing.xlsx", "attribute_name": an, "attribute_value": av})),
                    )
                    counts["attributes"] += 1
                    log_progress("content attributes", counts["attributes"])
                pending_content = maybe_commit(conn, "content", counts["products"], pending_content)

            if pending_content:
                conn.commit()
            wb.close()

            # pricing
            logging.info("Loading pricing workbook in read-only mode: %s", paths["pricing"])
            wb = load_workbook(paths["pricing"], data_only=True, read_only=True)
            ws = wb.active
            headers = [str(c).strip() if c is not None else "" for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
            pending_pricing = 0
            for row in ws.iter_rows(min_row=2, values_only=True):
                rd = row_dict(headers, row)
                key = str(rd.get("Prod", "")).strip()
                if not key:
                    continue
                product_id = upsert_product(cur, source_id, key, str(rd.get("ModelNo") or "").strip() or None, rd.get("Brand"), rd.get("Brand"), rd.get("Description"), None, rd.get("CategoryEnglish"), rd.get("UnitDescription"), None, None, {"source": "pricing.xlsx", "row": rd}, include_image_url)
                cur.execute(
                    """
                    insert into probuy.source_product_prices
                    (source_product_id, location_id, model_no, list_price, distributor_cost, msrp, currency_code, pricing_update_date, effective_at, raw_data)
                    values (%s,%s,%s,%s,%s,%s,'CAD',%s,%s,%s::jsonb)
                    on conflict (source_product_id, location_id) do update
                    set model_no=excluded.model_no, list_price=excluded.list_price, distributor_cost=excluded.distributor_cost, msrp=excluded.msrp,
                        pricing_update_date=excluded.pricing_update_date, effective_at=excluded.effective_at, raw_data=excluded.raw_data, updated_at=now()
                    """,
                    (
                        product_id,
                        loc_ids["SCN-CA"],
                        str(rd.get("ModelNo") or "").strip() or None,
                        parse_decimal(rd.get("ListPrice")),
                        parse_decimal(rd.get("DistCost")),
                        parse_decimal(rd.get("MSRP")),
                        parse_ts(rd.get("Date Last Modified")) or parse_ts(rd.get("LastPullDate")),
                        parse_ts(rd.get("Date Last Modified")) or parse_ts(rd.get("LastPullDate")),
                        json.dumps({"source": "pricing.xlsx", "row": rd}, default=str),
                    ),
                )
                counts["prices"] += 1
                pending_pricing += 1
                log_progress("pricing rows", counts["prices"])
                pending_pricing = maybe_commit(conn, "pricing", counts["prices"], pending_pricing)

            if pending_pricing:
                conn.commit()
            wb.close()

            # inventory
            logging.info("Loading inventory workbook in read-only mode: %s", paths["inventory"])
            wb = load_workbook(paths["inventory"], data_only=True, read_only=True)
            pending_inventory = 0
            for sheet_name in ["MTL", "VAN", "EDM"]:
                if sheet_name not in wb.sheetnames:
                    continue
                ws = wb[sheet_name]
                headers = [str(c).strip() if c is not None else "" for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
                for row in ws.iter_rows(min_row=2, values_only=True):
                    rd = row_dict(headers, row)
                    model_no = str(rd.get("Model No./No modèle", "")).strip()
                    if not model_no:
                        continue
                    key = model_no
                    product_id = upsert_product(cur, source_id, key, model_no, "SCN", "SCN", key, None, "Uncategorized", "Each", None, None, {"source": "inventory.xlsx", "sheet": sheet_name, "row": rd}, include_image_url)
                    cur.execute(
                        """
                        insert into probuy.source_product_inventory
                        (source_product_id, location_id, model_no, stock_status, quantity_available, inventory_update_date, raw_data)
                        values (%s,%s,%s,%s,%s,%s,%s::jsonb)
                        on conflict (source_product_id, location_id) do update
                        set model_no=excluded.model_no, stock_status=excluded.stock_status, quantity_available=excluded.quantity_available,
                            inventory_update_date=excluded.inventory_update_date, raw_data=excluded.raw_data, updated_at=now()
                        """,
                        (
                            product_id,
                            loc_ids[sheet_name],
                            model_no,
                            rd.get("Stock Status/État des stocks"),
                            parse_decimal(rd.get("Quantity Available/Quantité disponible")) or Decimal("0"),
                            parse_ts(rd.get("Inventory Update Date/Date de mise à jour de l'inventaire")),
                            json.dumps({"source": "inventory.xlsx", "sheet": sheet_name, "row": rd}, default=str),
                        ),
                    )
                    counts["inventory"] += 1
                    pending_inventory += 1
                    log_progress(f"inventory {sheet_name}", counts["inventory"])
                    pending_inventory = maybe_commit(conn, f"inventory {sheet_name}", counts["inventory"], pending_inventory)

            if pending_inventory:
                conn.commit()
            wb.close()
            logging.info("Building search documents")
            cur.execute(
                """
                with attribute_json as (
                    select pav.source_product_id, jsonb_object_agg(ad.canonical_name, coalesce(pav.value_text, pav.value_numeric::text, pav.value_boolean::text)) as attributes
                    from probuy.product_attribute_values pav
                    join probuy.attribute_definitions ad on ad.id = pav.attribute_id
                    group by pav.source_product_id
                ), scn_products as (
                    select sp.id, sp.brand, sp.manufacturer, sp.source_model_no, sp.category_en, sp.product_title_en, sp.description_en
                    from probuy.source_products sp
                    join probuy.primary_sources ps on ps.id = sp.source_id
                    where ps.code = 'SCN' and sp.is_active = true
                )
                insert into probuy.product_search_documents (source_product_id, search_text, search_vector, brand, manufacturer, model_no, category, attributes)
                select p.id,
                       trim(concat_ws(' ', p.product_title_en, p.description_en, p.brand, p.manufacturer, p.source_model_no, p.category_en, coalesce((select string_agg(value, ' ') from jsonb_each_text(coalesce(a.attributes, '{}'::jsonb))), ''))) as search_text,
                       to_tsvector('simple', trim(concat_ws(' ', p.product_title_en, p.description_en, p.brand, p.manufacturer, p.source_model_no, p.category_en, coalesce((select string_agg(value, ' ') from jsonb_each_text(coalesce(a.attributes, '{}'::jsonb))), '')))),
                       p.brand, p.manufacturer, p.source_model_no, p.category_en, coalesce(a.attributes, '{}'::jsonb)
                from scn_products p
                left join attribute_json a on a.source_product_id = p.id
                on conflict (source_product_id) do update
                set search_text = excluded.search_text,
                    search_vector = excluded.search_vector,
                    brand = excluded.brand,
                    manufacturer = excluded.manufacturer,
                    model_no = excluded.model_no,
                    category = excluded.category,
                    attributes = excluded.attributes,
                    updated_at = now()
                """
            )
            counts["search_docs"] = cur.rowcount

    elapsed = round(time.time() - started, 3)
    logging.info("Recon ingest completed in %s seconds", elapsed)
    print(json.dumps({"event": "recon_ingest_completed", "elapsed_seconds": elapsed, "file_info": file_info, "counts": counts, "finished_at": now_iso()}, ensure_ascii=False))


if __name__ == "__main__":
    main()

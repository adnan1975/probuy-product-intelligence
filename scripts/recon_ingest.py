#!/usr/bin/env python3
import hashlib
import json
import os
import re
import time
from datetime import UTC, datetime
import logging
from decimal import Decimal
from pathlib import Path

import psycopg2
from openpyxl import load_workbook

LOG_EVERY = int(os.getenv("RECON_LOG_EVERY", "5000"))


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
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _norm_field_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def row_get(rd: dict, *candidates, default=None):
    if not rd:
        return default
    normalized = {_norm_field_name(k): v for k, v in rd.items()}
    for candidate in candidates:
        direct = rd.get(candidate)
        if direct not in (None, ""):
            return direct
        normalized_match = normalized.get(_norm_field_name(candidate))
        if normalized_match not in (None, ""):
            return normalized_match
    return default


def detect_headers(ws, required_candidates, max_scan_rows=25):
    for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=max_scan_rows, values_only=True), start=1):
        headers = [str(c).strip() if c is not None else "" for c in row]
        rd = {h: h for h in headers if h}
        if all(row_get(rd, *cands) for cands in required_candidates):
            return headers, row_idx
    return None, None


def log_header_diagnostics(workbook_name: str, sheet_name: str, ws, required_candidates, max_scan_rows=25):
    preview = []
    for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=max_scan_rows, values_only=True), start=1):
        headers = [str(c).strip() if c is not None else "" for c in row]
        non_empty = [h for h in headers if h]
        if non_empty:
            matched = []
            rd = {h: h for h in headers if h}
            for candidate_group in required_candidates:
                matched.append(any(row_get(rd, c) for c in candidate_group))
            preview.append({"row": row_idx, "headers": non_empty[:8], "matched": matched})
    logging.warning(
        "Header detection failed workbook=%s sheet=%s required=%s scanned_rows=%s preview=%s",
        workbook_name,
        sheet_name,
        required_candidates,
        max_scan_rows,
        preview[:5],
    )


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
    return Decimal(txt)


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


def upsert_product(cur, source_id, key, model_no, brand, manufacturer, title, description, category, unit, product_url, raw_data):
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

    logging.info("Starting recon ingest; log cadence every %s rows", LOG_EVERY)
    file_info = {k: {"path": str(v), "sha256": checksum(v)} for k, v in paths.items()}
    counts = {"products": 0, "attributes": 0, "prices": 0, "inventory": 0, "search_docs": 0}

    with get_conn() as conn:
        with conn.cursor() as cur:
            source_id, loc_ids = ensure_source_and_locations(cur)
            # content
            logging.info("Loading content workbook in read-only mode: %s", paths["content"])
            wb = load_workbook(paths["content"], data_only=True, read_only=True)
            ws = wb.active
            headers, header_row = detect_headers(ws, [("Prod", "Product", "ProductCode", "Item")])
            if not headers:
                logging.warning("Could not detect content headers; skipping workbook")
                log_header_diagnostics("contentlicensing.xlsx", ws.title, ws, [("Prod", "Product", "ProductCode", "Item")])
            else:
                logging.info("Detected content header row at row=%s sheet=%s", header_row, ws.title)
                scanned_rows = 0
                skipped_missing_key = 0
                for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
                    scanned_rows += 1
                    rd = row_dict(headers, row)
                    key = str(row_get(rd, "Prod", "Product", "ProductCode", "Item", default="")).strip()
                    if not key:
                        skipped_missing_key += 1
                        continue
                    product_id = upsert_product(
                    cur,
                    source_id,
                    key,
                    str(row_get(rd, "ManufacturerNumber", "ModelNo", "Model No", default="") or "").strip() or None,
                    row_get(rd, "Brand"),
                    row_get(rd, "Brand"),
                    row_get(rd, "ProductTitle", "Description"),
                    None,
                    row_get(rd, "CategoryEnglish", "Category"),
                    row_get(rd, "UnitDescription", "Unit"),
                    None,
                    {"source": "contentlicensing.xlsx", "row": rd},
                )
                    counts["products"] += 1
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
                logging.info("Content ingest rows_scanned=%s rows_missing_key=%s upserted=%s", scanned_rows, skipped_missing_key, counts["products"])

            wb.close()

            # pricing
            logging.info("Loading pricing workbook in read-only mode: %s", paths["pricing"])
            wb = load_workbook(paths["pricing"], data_only=True, read_only=True)
            ws = wb.active
            headers, header_row = detect_headers(ws, [("Prod", "Product", "ProductCode", "Item")])
            if not headers:
                logging.warning("Could not detect pricing headers; skipping workbook")
                log_header_diagnostics("pricing.xlsx", ws.title, ws, [("Prod", "Product", "ProductCode", "Item")])
            else:
                logging.info("Detected pricing header row at row=%s sheet=%s", header_row, ws.title)
                scanned_rows = 0
                skipped_missing_key = 0
                for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
                    scanned_rows += 1
                    rd = row_dict(headers, row)
                    key = str(row_get(rd, "Prod", "Product", "ProductCode", "Item", default="")).strip()
                    if not key:
                        skipped_missing_key += 1
                        continue
                    product_id = upsert_product(cur, source_id, key, str(row_get(rd, "ModelNo", "Model No", default="") or "").strip() or None, row_get(rd, "Brand"), row_get(rd, "Brand"), row_get(rd, "Description", "ProductTitle"), None, row_get(rd, "CategoryEnglish", "Category"), row_get(rd, "UnitDescription", "Unit"), None, {"source": "pricing.xlsx", "row": rd})
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
                            str(row_get(rd, "ModelNo", "Model No", default="") or "").strip() or None,
                            parse_decimal(row_get(rd, "ListPrice")),
                            parse_decimal(row_get(rd, "DistCost", "DistributorCost")),
                            parse_decimal(row_get(rd, "MSRP")),
                            parse_ts(row_get(rd, "Date Last Modified", "LastPullDate")) or parse_ts(row_get(rd, "LastPullDate")),
                            parse_ts(row_get(rd, "Date Last Modified", "LastPullDate")) or parse_ts(row_get(rd, "LastPullDate")),
                            json.dumps({"source": "pricing.xlsx", "row": rd}, default=str),
                        ),
                    )
                    counts["prices"] += 1
                    log_progress("pricing rows", counts["prices"])
                logging.info("Pricing ingest rows_scanned=%s rows_missing_key=%s upserted=%s priced=%s", scanned_rows, skipped_missing_key, counts["products"], counts["prices"])

            wb.close()

            # inventory
            logging.info("Loading inventory workbook in read-only mode: %s", paths["inventory"])
            wb = load_workbook(paths["inventory"], data_only=True, read_only=True)
            for sheet_name in ["MTL", "VAN", "EDM"]:
                if sheet_name not in wb.sheetnames:
                    continue
                ws = wb[sheet_name]
                headers, header_row = detect_headers(ws, [("Model No./No modèle", "ModelNo", "Model No")])
                if not headers:
                    logging.warning("Could not detect inventory headers for sheet %s; skipping", sheet_name)
                    log_header_diagnostics("inventory.xlsx", sheet_name, ws, [("Model No./No modèle", "ModelNo", "Model No")])
                    continue
                logging.info("Detected inventory header row at row=%s sheet=%s", header_row, sheet_name)
                scanned_rows = 0
                skipped_missing_model = 0
                for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
                    scanned_rows += 1
                    rd = row_dict(headers, row)
                    model_no = str(row_get(rd, "Model No./No modèle", "ModelNo", "Model No", default="")).strip()
                    if not model_no:
                        skipped_missing_model += 1
                        continue
                    key = model_no
                    product_id = upsert_product(cur, source_id, key, model_no, "SCN", "SCN", key, None, "Uncategorized", "Each", None, {"source": "inventory.xlsx", "sheet": sheet_name, "row": rd})
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
                            row_get(rd, "Stock Status/État des stocks", "Stock Status"),
                            parse_decimal(row_get(rd, "Quantity Available/Quantité disponible", "Quantity Available")) or Decimal("0"),
                            parse_ts(row_get(rd, "Inventory Update Date/Date de mise à jour de l'inventaire", "Inventory Update Date")),
                            json.dumps({"source": "inventory.xlsx", "sheet": sheet_name, "row": rd}, default=str),
                        ),
                    )
                    counts["inventory"] += 1
                    log_progress(f"inventory {sheet_name}", counts["inventory"])
                logging.info("Inventory ingest sheet=%s rows_scanned=%s rows_missing_model=%s upserted=%s inventory_rows=%s", sheet_name, scanned_rows, skipped_missing_model, counts["products"], counts["inventory"])

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

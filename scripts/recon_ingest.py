#!/usr/bin/env python3
"""Full workbook recon ingest for SCN source."""

import hashlib
import json
import logging
import os
import re
import time
import gc
import argparse
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg2
from openpyxl import load_workbook

try:
    import resource  # Unix-only
except ModuleNotFoundError:  # pragma: no cover - platform dependent
    resource = None

try:
    import psutil
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    psutil = None

LOG_EVERY = int(os.getenv("RECON_LOG_EVERY", "1000"))
COMMIT_EVERY = int(os.getenv("RECON_COMMIT_EVERY", "250"))

FILES = {
    "content": "contentlicensing.xlsx",
    "pricing": "pricing.xlsx",
    "inventory": "inventory.xlsx",
}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def normalize_key(value: str) -> str:
    txt = str(value or "").strip().lower()
    txt = re.sub(r"[^a-z0-9]+", "_", txt)
    return re.sub(r"_+", "_", txt).strip("_")


def row_dict(headers, row):
    out = {}
    for idx, cell in enumerate(row):
        key = headers[idx] if idx < len(headers) else ""
        if key:
            out[key] = cell
    return out


def parse_decimal(value):
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    txt = str(value).replace("$", "").replace(",", "").strip()
    if not txt:
        return None
    try:
        return Decimal(txt)
    except InvalidOperation:
        return None


def parse_ts(value):
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None


def checksum(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def get_conn():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")
    return psycopg2.connect(database_url)


def memory_usage_mb() -> float:
    if resource is not None:
        # Linux ru_maxrss is in KB.
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    if psutil is not None:
        return psutil.Process().memory_info().rss / (1024 * 1024)
    return 0.0


def log_memory(stage: str, elapsed_start: float | None = None) -> None:
    elapsed = ""
    if elapsed_start is not None:
        elapsed = f" elapsed={round(time.time() - elapsed_start, 2)}s"
    logging.info("%s memory_mb=%.2f%s", stage, memory_usage_mb(), elapsed)


def commit_and_log(conn, stage, rows):
    conn.commit()
    gc.collect()
    log_memory(stage)
    logging.info("%s committed rows=%s", stage, rows)

def log_every(stage: str, processed: int):
    if processed and processed % LOG_EVERY == 0:
        logging.info("%s processed=%s", stage, processed)


def ensure_source_and_locations(cur):
    cur.execute(
        """
        insert into probuy.primary_sources (code, name, is_active)
        values ('SCN', 'SCN International', true)
        on conflict (code) do nothing
        returning id
        """
    )
    row = cur.fetchone()
    if row:
        source_id = row[0]
    else:
        cur.execute("select id from probuy.primary_sources where code = 'SCN'")
        source_id = cur.fetchone()[0]

    locations = [
        ("SCN-CA", "SCN Canada Pricing", "National", "CA", {"source": "pricing.xlsx"}),
        ("MTL", "Montreal Distribution Centre", "QC", "CA", {"source": "inventory.xlsx", "sheet": "MTL"}),
        ("VAN", "Vancouver Distribution Centre", "BC", "CA", {"source": "inventory.xlsx", "sheet": "VAN"}),
        ("EDM", "Edmonton Distribution Centre", "AB", "CA", {"source": "inventory.xlsx", "sheet": "EDM"}),
    ]
    ids = {}
    for code, name, province, country, raw_data in locations:
        cur.execute(
            """
            insert into probuy.source_locations (source_id, code, name, province, country, is_active, raw_data)
            values (%s, %s, %s, %s, %s, true, %s::jsonb)
            on conflict (source_id, code) do nothing
            returning id
            """,
            (source_id, code, name, province, country, json.dumps(raw_data)),
        )
        row = cur.fetchone()
        if row:
            ids[code] = row[0]
        else:
            cur.execute("select id from probuy.source_locations where source_id = %s and code = %s", (source_id, code))
            ids[code] = cur.fetchone()[0]
    return source_id, ids


def insert_product(cur, source_id, key, model_no, brand, manufacturer, title, description, category, unit, raw_data):
    cur.execute(
        """
        insert into probuy.source_products
        (source_id, source_product_key, source_model_no, brand, manufacturer, product_title_en, description_en, category_en, unit_description_en, is_active, raw_data)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,true,%s::jsonb)
        on conflict (source_id, source_product_key) do nothing
        returning id
        """,
        (source_id, key, model_no, brand, manufacturer, title, description, category, unit, json.dumps(raw_data, default=str)),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("select id from probuy.source_products where source_id = %s and source_product_key = %s", (source_id, key))
    return cur.fetchone()[0]


def header_alias_map_content():
    return {
        "prod": "prod",
        "manufacturernumber": "manufacturer_number",
        "brand": "brand",
        "producttitle": "product_title",
        "categoryenglish": "category_english",
        "unitdescription": "unit_description",
        "date_last_modified": "date_last_modified",
    }


def header_alias_map_pricing():
    return {
        "model_no_no_modele": "model_no",
        "mfg_model_no_no_fab": "mfg_model_no",
        "fabricant": "manufacturer_fr",
        "manufacturer": "manufacturer",
        "english_description_description_anglais": "description_en",
        "french_description_description_francais": "description_fr",
        "list_price_prix_liste": "list_price",
        "distributor_cost_cout_distributeur": "dist_cost",
        "pricing_update_date_derniere_mise_a_jour_de_prix": "pricing_update_date",
        "category_level_1_english_categorie_niveau_1_anglais": "category_en",
        "unit_of_sale_unite_de_vente": "unit_of_sale",
    }


def to_alias_headers(headers, aliases):
    resolved = []
    for h in headers:
        nk = normalize_key(h)
        resolved.append(aliases.get(nk, nk))
    return resolved


def ingest_content(conn, cur, source_id, counts, paths):
    stage_start = time.time()
    logging.info("file started: %s", paths["content"])
    wb = load_workbook(paths["content"], read_only=True, data_only=True)
    try:
        ws = wb.active
        ws.reset_dimensions()
        raw_headers = [str(c).strip() if c is not None else "" for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        headers = to_alias_headers(raw_headers, header_alias_map_content())
        logging.info("headers found content=%s", headers)
        pending = 0
        batch_seen = set()
        for row_num, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
            try:
                rd = row_dict(headers, row)
                key = str(rd.get("prod", "")).strip()
                if not key or key in batch_seen:
                    continue
                batch_seen.add(key)

                product_id = insert_product(
                    cur, source_id, key, str(rd.get("manufacturer_number") or "").strip() or None, rd.get("brand"),
                    rd.get("brand"), rd.get("product_title"), None, rd.get("category_english"), rd.get("unit_description"),
                    {"source": "contentlicensing.xlsx", "row": rd},
                )
                counts["products"] += 1
                pending += 1
                if counts["products"] % LOG_EVERY == 0:
                    logging.info("content rows processed=%s", counts["products"])
                    log_memory("content-progress", stage_start)

                for i in range(1, 16):
                    an = rd.get(f"attributename{i}")
                    av = rd.get(f"attributevalue{i}")
                    if not an or av in (None, ""):
                        continue
                    canonical = normalize_key(str(an))
                    cur.execute(
                        """insert into probuy.attribute_definitions (canonical_name, display_name, data_type, unit, is_filterable, is_searchable)
                        values (%s,%s,'text',null,true,true)
                        on conflict (canonical_name) do nothing returning id""",
                        (canonical, str(an).strip()),
                    )
                    attr_row = cur.fetchone()
                    attr_id = attr_row[0] if attr_row else None
                    if attr_id is None:
                        cur.execute("select id from probuy.attribute_definitions where canonical_name = %s", (canonical,))
                        attr_id = cur.fetchone()[0]
                    cur.execute(
                        """insert into probuy.product_attribute_values (source_product_id, attribute_id, value_text, raw_data)
                        values (%s,%s,%s,%s::jsonb) on conflict (source_product_id, attribute_id) do nothing""",
                        (product_id, attr_id, str(av).strip(), json.dumps({"source": "contentlicensing.xlsx", "attribute_name": an, "attribute_value": av})),
                    )
                    counts["attributes"] += 1

                if pending >= COMMIT_EVERY:
                    commit_and_log(conn, "content", counts["products"])
                    logging.info("content rows inserted this batch=%s", pending)
                    batch_seen.clear()
                    pending = 0
            except Exception as exc:
                logging.exception("content row error row_number=%s error=%s", row_num, exc)
        if pending:
            commit_and_log(conn, "content-final", counts["products"])
            logging.info("content rows inserted this batch=%s", pending)
    finally:
        wb.close()


def ingest_pricing(conn, cur, source_id, loc_id, counts, paths):
    stage_start = time.time()
    logging.info("file started: %s", paths["pricing"])
    wb = load_workbook(paths["pricing"], read_only=True, data_only=True)
    try:
        ws = wb.active
        raw_headers = [str(c).strip() if c is not None else "" for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        headers = to_alias_headers(raw_headers, header_alias_map_pricing())
        logging.info("headers found pricing=%s", headers)
        pending = 0
        batch_seen = set()
        for row_num, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
            try:
                rd = row_dict(headers, row)
                model_no = str(rd.get("model_no", "")).strip()
                if not model_no or model_no in batch_seen:
                    continue
                batch_seen.add(model_no)

                product_id = insert_product(cur, source_id, model_no, model_no, rd.get("manufacturer") or "SCN", rd.get("manufacturer") or "SCN", rd.get("description_en") or model_no, None, rd.get("category_en"), rd.get("unit_of_sale"), {"source": "pricing.xlsx", "row": rd})
                cur.execute("""
                    insert into probuy.source_product_prices
                    (source_product_id, location_id, model_no, list_price, distributor_cost, currency_code, pricing_update_date, effective_at, raw_data)
                    values (%s,%s,%s,%s,%s,'CAD',%s,%s,%s::jsonb)
                    on conflict (source_product_id, location_id) do nothing
                    """,
                    (product_id, loc_id, model_no, parse_decimal(rd.get("list_price")), parse_decimal(rd.get("dist_cost")), parse_ts(rd.get("pricing_update_date")), parse_ts(rd.get("pricing_update_date")), json.dumps({"source": "pricing.xlsx", "row": rd}, default=str)),
                )

                counts["prices"] += 1
                pending += 1
                if counts["prices"] % LOG_EVERY == 0:
                    logging.info("pricing rows processed=%s", counts["prices"])
                    log_memory("pricing-progress", stage_start)
                if pending >= COMMIT_EVERY:
                    commit_and_log(conn, "pricing", counts["prices"])
                    logging.info("pricing rows inserted this batch=%s", pending)
                    batch_seen.clear()
                    pending = 0
            except Exception as exc:
                logging.exception("pricing row error row_number=%s error=%s", row_num, exc)

        if pending:
            commit_and_log(conn, "pricing-final", counts["prices"])
            logging.info("pricing rows inserted this batch=%s", pending)
    finally:
        wb.close()


def ingest_inventory(conn, cur, source_id, loc_ids, counts, paths):
    stage_start = time.time()
    logging.info("file started: %s", paths["inventory"])
    wb = load_workbook(paths["inventory"], read_only=True, data_only=True)
    pending = 0
    try:
        for sheet_name in ["MTL", "VAN", "EDM"]:
            if sheet_name not in wb.sheetnames:
                logging.warning("Inventory sheet missing, skipping: %s", sheet_name)
                continue
            ws = wb[sheet_name]
            headers = [str(c).strip() if c is not None else "" for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
            headers = [normalize_key(h) for h in headers]
            logging.info("headers found inventory sheet=%s headers=%s", sheet_name, headers)
            batch_seen = set()
            for row_num, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
                try:
                    rd = row_dict(headers, row)
                    model_no = str(rd.get("model_no_no_modele", "")).strip()
                    dedupe_key = (sheet_name, model_no)
                    if not model_no or dedupe_key in batch_seen:
                        continue
                    batch_seen.add(dedupe_key)
                    product_id = insert_product(
                cur,
                source_id,
                model_no,
                model_no,
                "SCN",
                "SCN",
                model_no,
                None,
                "Uncategorized",
                "Each",
                {"source": "inventory.xlsx", "sheet": sheet_name, "row": rd},
            )
                    cur.execute(
                """
                insert into probuy.source_product_inventory
                (source_product_id, location_id, model_no, stock_status, quantity_available, inventory_update_date, raw_data)
                values (%s,%s,%s,%s,%s,%s,%s::jsonb)
                on conflict (source_product_id, location_id) do nothing
                """,
                (
                    product_id,
                    loc_ids[sheet_name],
                    model_no,
                    rd.get("status_etat_des_stocks") or rd.get("stock_status_etat_des_stocks"),
                    parse_decimal(rd.get("quantity_available_quantite_disponible")) or Decimal("0"),
                    parse_ts(rd.get("inventory_update_date_date_de_mise_a_jour_de_l_inventaire")),
                    json.dumps({"source": "inventory.xlsx", "sheet": sheet_name, "row": rd}, default=str),
                ),
            )
                    counts["inventory"] += 1
                    pending += 1
                    if counts["inventory"] % LOG_EVERY == 0:
                        logging.info("inventory rows processed=%s", counts["inventory"])
                        log_memory("inventory-progress", stage_start)
                    if pending >= COMMIT_EVERY:
                        commit_and_log(conn, f"inventory-{sheet_name}", counts["inventory"])
                        logging.info("inventory rows inserted this batch=%s", pending)
                        batch_seen.clear()
                        pending = 0
                except Exception as exc:
                    logging.exception("inventory row error sheet=%s row_number=%s error=%s", sheet_name, row_num, exc)
        if pending:
            commit_and_log(conn, "inventory-final", counts["inventory"])
            logging.info("inventory rows inserted this batch=%s", pending)
    finally:
        wb.close()


def main():
    parser = argparse.ArgumentParser(description="Full workbook recon ingest for SCN source.")
    parser.add_argument(
        "--start-at",
        choices=["content", "pricing", "inventory"],
        default="content",
        help="Start ingest from this phase (default: content).",
    )
    args = parser.parse_args()

    configure_logging()
    start = time.time()

    input_dir = Path("../input/data")
    paths = {k: input_dir / v for k, v in FILES.items()}
    missing = [str(p) for p in paths.values() if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required files: {', '.join(missing)}")

    info = {k: {"path": str(v), "sha256": checksum(v)} for k, v in paths.items()}
    counts = {"products": 0, "attributes": 0, "prices": 0, "inventory": 0}

    logging.info("Starting full recon ingest with streaming workbooks")
    logging.info("Purge step disabled for recon ingest")
    logging.info("Config log_every=%s commit_every=%s", LOG_EVERY, COMMIT_EVERY)

    with get_conn() as conn:
        with conn.cursor() as cur:
            source_id, loc_ids = ensure_source_and_locations(cur)
            conn.commit()

            phase_order = ["content", "pricing", "inventory"]
            start_index = phase_order.index(args.start_at)
            phases_to_run = phase_order[start_index:]
            logging.info("Running phases=%s", phases_to_run)

            if "content" in phases_to_run:
                ingest_content(conn, cur, source_id, counts, paths)
            if "pricing" in phases_to_run:
                ingest_pricing(conn, cur, source_id, loc_ids["SCN-CA"], counts, paths)
            if "inventory" in phases_to_run:
                ingest_inventory(conn, cur, source_id, loc_ids, counts, paths)

    elapsed = round(time.time() - start, 3)
    logging.info("Full recon ingest complete in %ss", elapsed)
    print(json.dumps({"event": "ingestfullrecon_complete", "elapsed_seconds": elapsed, "counts": counts, "files": info}, ensure_ascii=False))


if __name__ == "__main__":
    main()

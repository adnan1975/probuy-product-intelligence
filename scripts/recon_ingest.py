#!/usr/bin/env python3
"""Full workbook recon ingest for SCN source.

Reads the large workbooks from ../input/data in streaming mode and upserts
content, pricing, and inventory data into Supabase/Postgres.
"""

import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg2
from openpyxl import load_workbook

LOG_EVERY = int(os.getenv("RECON_LOG_EVERY", "250"))
COMMIT_EVERY = int(os.getenv("RECON_COMMIT_EVERY", "1000"))

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
        on conflict (code) do update
        set name = excluded.name, is_active = excluded.is_active, updated_at = now()
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
    ids = {}
    for code, name, province, country, raw_data in locations:
        cur.execute(
            """
            insert into probuy.source_locations (source_id, code, name, province, country, is_active, raw_data)
            values (%s, %s, %s, %s, %s, true, %s::jsonb)
            on conflict (source_id, code) do update
            set name = excluded.name,
                province = excluded.province,
                country = excluded.country,
                is_active = excluded.is_active,
                raw_data = excluded.raw_data,
                updated_at = now()
            returning id
            """,
            (source_id, code, name, province, country, json.dumps(raw_data)),
        )
        ids[code] = cur.fetchone()[0]
    return source_id, ids


def upsert_product(cur, source_id, key, model_no, brand, manufacturer, title, description, category, unit, raw_data):
    cur.execute(
        """
        insert into probuy.source_products
        (source_id, source_product_key, source_model_no, brand, manufacturer, product_title_en, description_en, category_en, unit_description_en, is_active, raw_data)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,true,%s::jsonb)
        on conflict (source_id, source_product_key) do update
        set source_model_no = excluded.source_model_no,
            brand = excluded.brand,
            manufacturer = excluded.manufacturer,
            product_title_en = excluded.product_title_en,
            description_en = excluded.description_en,
            category_en = excluded.category_en,
            unit_description_en = excluded.unit_description_en,
            is_active = excluded.is_active,
            raw_data = excluded.raw_data,
            updated_at = now()
        returning id
        """,
        (source_id, key, model_no, brand, manufacturer, title, description, category, unit, json.dumps(raw_data, default=str)),
    )
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
    logging.info("Loading content workbook (streaming): %s", paths["content"])
    wb = load_workbook(paths["content"], read_only=True, data_only=True)
    ws = wb.active
    ws.reset_dimensions() 
    
    raw_headers = [str(c).strip() if c is not None else "" for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
    headers = to_alias_headers(raw_headers, header_alias_map_content())
    print( headers)
    pending = 0
    for row in ws.iter_rows(min_row=2, values_only=True):
        rd = row_dict(headers, row)
        key = str(rd.get("prod", "")).strip()
        if not key:
            continue

        product_id = upsert_product(
            cur,
            source_id,
            key,
            str(rd.get("manufacturer_number") or "").strip() or None,
            rd.get("brand"),
            rd.get("brand"),
            rd.get("product_title"),
            None,
            rd.get("category_english"),
            rd.get("unit_description"),
            {"source": "contentlicensing.xlsx", "row": rd},
        )
        counts["products"] += 1
        pending += 1
        log_every("content", counts["products"])

        for i in range(1, 16):
            an = rd.get(f"attributename{i}")
            av = rd.get(f"attributevalue{i}")
            if not an or av in (None, ""):
                continue
            canonical = normalize_key(str(an))
            cur.execute(
                """
                insert into probuy.attribute_definitions (canonical_name, display_name, data_type, unit, is_filterable, is_searchable)
                values (%s,%s,'text',null,true,true)
                on conflict (canonical_name) do update
                set display_name = excluded.display_name, updated_at = now()
                returning id
                """,
                (canonical, str(an).strip()),
            )
            attr_id = cur.fetchone()[0]
            cur.execute(
                """
                insert into probuy.product_attribute_values (source_product_id, attribute_id, value_text, raw_data)
                values (%s,%s,%s,%s::jsonb)
                on conflict (source_product_id, attribute_id) do update
                set value_text = excluded.value_text,
                    raw_data = excluded.raw_data,
                    updated_at = now()
                """,
                (product_id, attr_id, str(av).strip(), json.dumps({"source": "contentlicensing.xlsx", "attribute_name": an, "attribute_value": av})),
            )
            counts["attributes"] += 1

        if pending >= COMMIT_EVERY:
            commit_and_log(conn, "content", counts["products"])
            pending = 0

    if pending:
        commit_and_log(conn, "content-final", counts["products"])
    wb.close()


def ingest_pricing(conn, cur, source_id, loc_id, counts, paths):
    logging.info("Loading pricing workbook (streaming): %s", paths["pricing"])
    wb = load_workbook(paths["pricing"], read_only=True, data_only=True)
    ws = wb.active
    raw_headers = [str(c).strip() if c is not None else "" for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
    headers = to_alias_headers(raw_headers, header_alias_map_pricing())

    pending = 0
    for row in ws.iter_rows(min_row=2, values_only=True):
        rd = row_dict(headers, row)
        model_no = str(rd.get("model_no", "")).strip()
        if not model_no:
            continue

        product_id = upsert_product(
            cur,
            source_id,
            model_no,
            model_no,
            rd.get("manufacturer") or "SCN",
            rd.get("manufacturer") or "SCN",
            rd.get("description_en") or model_no,
            None,
            rd.get("category_en"),
            rd.get("unit_of_sale"),
            {"source": "pricing.xlsx", "row": rd},
        )
        cur.execute(
            """
            insert into probuy.source_product_prices
            (source_product_id, location_id, model_no, list_price, distributor_cost, currency_code, pricing_update_date, effective_at, raw_data)
            values (%s,%s,%s,%s,%s,'CAD',%s,%s,%s::jsonb)
            on conflict (source_product_id, location_id) do update
            set model_no = excluded.model_no,
                list_price = excluded.list_price,
                distributor_cost = excluded.distributor_cost,
                pricing_update_date = excluded.pricing_update_date,
                effective_at = excluded.effective_at,
                raw_data = excluded.raw_data,
                updated_at = now()
            """,
            (
                product_id,
                loc_id,
                model_no,
                parse_decimal(rd.get("list_price")),
                parse_decimal(rd.get("dist_cost")),
                parse_ts(rd.get("pricing_update_date")),
                parse_ts(rd.get("pricing_update_date")),
                json.dumps({"source": "pricing.xlsx", "row": rd}, default=str),
            ),
        )

        counts["prices"] += 1
        pending += 1
        log_every("pricing", counts["prices"])
        if pending >= COMMIT_EVERY:
            commit_and_log(conn, "pricing", counts["prices"])
            pending = 0

    if pending:
        commit_and_log(conn, "pricing-final", counts["prices"])
    wb.close()


def ingest_inventory(conn, cur, source_id, loc_ids, counts, paths):
    logging.info("Loading inventory workbook (streaming): %s", paths["inventory"])
    wb = load_workbook(paths["inventory"], read_only=True, data_only=True)
    pending = 0

    for sheet_name in ["MTL", "VAN", "EDM"]:
        if sheet_name not in wb.sheetnames:
            logging.warning("Inventory sheet missing, skipping: %s", sheet_name)
            continue

        ws = wb[sheet_name]
        headers = [str(c).strip() if c is not None else "" for c in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
        headers = [normalize_key(h) for h in headers]
        for row in ws.iter_rows(min_row=2, values_only=True):
            rd = row_dict(headers, row)
            model_no = str(rd.get("model_no_no_modele", "")).strip()
            if not model_no:
                continue

            product_id = upsert_product(
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
                on conflict (source_product_id, location_id) do update
                set model_no = excluded.model_no,
                    stock_status = excluded.stock_status,
                    quantity_available = excluded.quantity_available,
                    inventory_update_date = excluded.inventory_update_date,
                    raw_data = excluded.raw_data,
                    updated_at = now()
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
            log_every(f"inventory-{sheet_name}", counts["inventory"])
            if pending >= COMMIT_EVERY:
                commit_and_log(conn, f"inventory-{sheet_name}", counts["inventory"])
                pending = 0

    if pending:
        commit_and_log(conn, "inventory-final", counts["inventory"])
    wb.close()


def main():
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
    logging.info("Config log_every=%s commit_every=%s", LOG_EVERY, COMMIT_EVERY)

    with get_conn() as conn:
        with conn.cursor() as cur:
            source_id, loc_ids = ensure_source_and_locations(cur)
            conn.commit()

            ingest_content(conn, cur, source_id, counts, paths)
            ingest_pricing(conn, cur, source_id, loc_ids["SCN-CA"], counts, paths)
            ingest_inventory(conn, cur, source_id, loc_ids, counts, paths)

    elapsed = round(time.time() - start, 3)
    logging.info("Full recon ingest complete in %ss", elapsed)
    print(json.dumps({"event": "ingestfullrecon_complete", "elapsed_seconds": elapsed, "counts": counts, "files": info}, ensure_ascii=False))


if __name__ == "__main__":
    main()

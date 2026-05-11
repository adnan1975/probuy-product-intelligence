"""
build_import_csv.py
-------------------
Merges scn_results.jsonl with the original Shopify export CSV
and produces a Shopify-ready metafield import CSV.

Only SKUs with status "ok" (shipping text found) are included.
SKUs that were not found on SCN or had errors are skipped and
logged to a separate file for review.

Usage:
    python scripts/build_import_csv.py \
        --csv      "shopify_export/products_export_1(2).csv" \
        --results  scn_results.jsonl \
        --output   shopify_import.csv \
        --skipped  skipped_skus.csv

Shopify import format produced:
    Handle | Variant SKU | Shipping Time (product.metafields.custom.shipping_time)

Import instructions:
    Shopify Admin → Products → Import → upload shopify_import.csv
"""

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# The metafield column name Shopify expects in the import CSV
METAFIELD_COLUMN = "Shipping Time (product.metafields.custom.shipping_time)"


# ------------------------------------------------------------------
# Loaders
# ------------------------------------------------------------------

def load_results(jsonl_path: Path) -> dict:
    """
    Load scn_results.jsonl into a dict keyed by SKU.
    Handles both proper JSONL (one object per line) and
    pretty-printed JSON objects written across multiple lines.
    """
    results = {}
    raw = jsonl_path.read_text(encoding="utf-8").strip()

    # Try parsing the whole file as a JSON array first
    # then as concatenated objects by splitting on }{
    # This handles pretty-printed output from the smoke test
    chunks = []
    try:
        parsed = json.loads(raw)
        chunks = parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        # Split on newlines and try line-by-line (true JSONL)
        # then fall back to re-assembling multi-line objects
        buffer = ""
        for line in raw.splitlines():
            buffer += line.strip()
            try:
                obj = json.loads(buffer)
                chunks.append(obj)
                buffer = ""
            except json.JSONDecodeError:
                continue  # keep accumulating lines

    for obj in chunks:
        sku = obj.get("sku", "").strip()
        if sku:
            results[sku] = obj

    logger.info("Loaded results for %d SKUs from JSONL.", len(results))
    return results


def load_sku_to_handle(csv_paths: list[Path]) -> dict:
    """
    Build a SKU → Handle mapping from one or more Shopify export CSVs.
    """
    mapping = {}
    for csv_path in csv_paths:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            current_handle = ""
            for row in reader:
                handle = row.get("Handle", "").strip()
                if handle:
                    current_handle = handle
                sku = row.get("Variant SKU", "").strip()
                if sku and sku not in mapping:
                    mapping[sku] = current_handle
    logger.info("Loaded %d SKU→Handle mappings from %d CSV file(s).", len(mapping), len(csv_paths))
    return mapping


# ------------------------------------------------------------------
# Builder
# ------------------------------------------------------------------

def build_import_csv(
    csv_paths: list[Path],
    results_path: Path,
    output_path: Path,
    skipped_path: Path,
) -> None:

    results       = load_results(results_path)
    sku_to_handle = load_sku_to_handle(csv_paths)

    import_rows  = []
    skipped_rows = []

    for sku, result in results.items():
        handle        = sku_to_handle.get(sku, "")
        status        = result.get("status")
        shipping_text = result.get("shipping_text", "")

        if status == "ok" and shipping_text:
            import_rows.append({
                "Handle":        handle,
                "Variant SKU":   sku,
                METAFIELD_COLUMN: shipping_text,
            })
        else:
            skipped_rows.append({
                "Variant SKU":  sku,
                "Handle":       handle,
                "Status":       status,
                "Error":        result.get("error", ""),
                "Product URL":  result.get("product_url", ""),
            })

    # --- Write import CSV ---
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Handle", "Variant SKU", METAFIELD_COLUMN],
        )
        writer.writeheader()
        writer.writerows(import_rows)

    logger.info("✅ Import CSV written: %s  (%d rows)", output_path, len(import_rows))

    # --- Write skipped CSV ---
    if skipped_rows:
        with open(skipped_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["Variant SKU", "Handle", "Status", "Error", "Product URL"],
            )
            writer.writeheader()
            writer.writerows(skipped_rows)
        logger.info(
            "⚠️  Skipped CSV written: %s  (%d rows — review these manually)",
            skipped_path, len(skipped_rows),
        )
    else:
        logger.info("No skipped SKUs.")

    # --- Summary ---
    logger.info("─" * 50)
    logger.info("Total results in JSONL : %d", len(results))
    logger.info("Ready to import        : %d", len(import_rows))
    logger.info("Skipped (no data)      : %d", len(skipped_rows))


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Build Shopify metafield import CSV")
    p.add_argument(
        "--csv",
        nargs="+",
        default=[
            "shopify_export/products_export_1.csv",
            "shopify_export/products_export_2.csv",
            "shopify_export/products_export_3.csv",
        ],
        help="One or more Shopify export CSV files",
    )
    p.add_argument("--results", default="scn_results.jsonl",
                   help="Scrape results (JSON Lines)")
    p.add_argument("--output",  default="shopify_import.csv",
                   help="Output import CSV for Shopify")
    p.add_argument("--skipped", default="skipped_skus.csv",
                   help="SKUs not found or errored — for manual review")
    return p.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    args = parse_args()
    build_import_csv(
        csv_paths    = [Path(p) for p in args.csv],
        results_path = Path(args.results),
        output_path  = Path(args.output),
        skipped_path = Path(args.skipped),
    )

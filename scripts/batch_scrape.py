"""
batch_scrape.py
---------------
Reads every SKU from the Shopify export CSV, looks up the shipping time
on scnindustrial.com, and saves results to a JSONL file.

Progress is saved after every SKU — if the script is interrupted,
restart it and it will skip already-processed SKUs automatically.

Dependencies:
    pip install cloudscraper beautifulsoup4 lxml

Usage:
    python batch_scrape.py \
        --csv     shopify_export/products_export_1\(2\).csv \
        --cookies scn_cookies.json \
        --output  scn_results.jsonl \
        --progress scn_progress.json

Files produced:
    scn_results.jsonl   — one JSON line per SKU with result
    scn_progress.json   — set of already-done SKUs (auto-managed)
"""

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path

from scn_scraper import SCNClient, SCNCookieError, SCNSessionExpiredError

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# CSV helpers
# ------------------------------------------------------------------

def read_skus_from_csv(csv_paths: list[Path]) -> list[str]:
    """
    Return a deduplicated list of non-empty Variant SKUs from one or
    more Shopify export CSVs, preserving first-seen order across files.
    """
    seen = set()
    skus = []
    for csv_path in csv_paths:
        logger.info("Reading SKUs from %s …", csv_path)
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sku = row.get("Variant SKU", "").strip()
                if sku and sku not in seen:
                    seen.add(sku)
                    skus.append(sku)
    return skus


# ------------------------------------------------------------------
# Progress helpers
# ------------------------------------------------------------------

def load_progress(progress_path: Path) -> set:
    """Return the set of SKUs already processed in a previous run."""
    if not progress_path.exists():
        return set()
    try:
        data = json.loads(progress_path.read_text(encoding="utf-8"))
        return set(data.get("done", []))
    except Exception:
        logger.warning("Could not read progress file — starting fresh.")
        return set()


def save_progress(progress_path: Path, done: set) -> None:
    progress_path.write_text(
        json.dumps({"done": sorted(done)}, indent=2),
        encoding="utf-8",
    )


# ------------------------------------------------------------------
# Main batch loop
# ------------------------------------------------------------------

def run_batch(
    csv_paths: list[Path],
    cookie_file: Path,
    output_path: Path,
    progress_path: Path,
) -> None:

    # --- Load SKUs from all CSV files ---
    all_skus = read_skus_from_csv(csv_paths)
    logger.info("Found %d unique SKUs in CSV.", len(all_skus))

    # --- Load progress ---
    done = load_progress(progress_path)
    remaining = [s for s in all_skus if s not in done]
    logger.info(
        "Already done: %d  |  Remaining: %d",
        len(done), len(remaining),
    )

    if not remaining:
        logger.info("Nothing to do — all SKUs already processed.")
        return

    # --- Set up SCN client ---
    client = SCNClient(cookie_file=str(cookie_file))
    try:
        client.load_cookies()
    except SCNCookieError as e:
        logger.error("Cookie error: %s", e)
        sys.exit(1)
    except SCNSessionExpiredError as e:
        logger.error("Session expired: %s", e)
        sys.exit(1)

    # --- Open output file in append mode (safe for resume) ---
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out_file = open(output_path, "a", encoding="utf-8")

    total     = len(all_skus)
    processed = len(done)

    try:
        for sku in remaining:
            processed += 1
            pct = processed / total * 100
            logger.info("[%d/%d  %.1f%%] Scraping SKU: %s", processed, total, pct, sku)

            try:
                result = client.get_shipping_time(sku)
            except SCNSessionExpiredError as e:
                logger.error("Session expired mid-run: %s", e)
                logger.error("Re-export cookies and restart — progress is saved.")
                break

            # Write result line
            out_file.write(json.dumps(result) + "\n")
            out_file.flush()

            # Mark as done and persist immediately
            done.add(sku)
            save_progress(progress_path, done)

    finally:
        out_file.close()

    # --- Summary ---
    ok            = 0
    not_found     = 0
    no_ship_info  = 0
    errors        = 0

    if output_path.exists():
        with open(output_path, encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                    status = r.get("status", "error")
                    if status == "ok":            ok += 1
                    elif status == "not_found":   not_found += 1
                    elif status == "no_shipping_info": no_ship_info += 1
                    else:                         errors += 1
                except json.JSONDecodeError:
                    pass

    logger.info("─" * 50)
    logger.info("Run complete.")
    logger.info("  ✅ OK (shipping text found) : %d", ok)
    logger.info("  ❓ Not found on SCN         : %d", not_found)
    logger.info("  ⚠️  No shipping info on page : %d", no_ship_info)
    logger.info("  ❌ Errors                   : %d", errors)
    logger.info("Results saved to : %s", output_path)
    logger.info("Progress saved to: %s", progress_path)


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Batch SCN shipping time scraper")
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
    p.add_argument("--cookies",  default="scn_cookies.json",
                   help="Path to Cookie-Editor JSON export")
    p.add_argument("--output",   default="scn_results.jsonl",
                   help="Output file (JSON Lines, one result per SKU)")
    p.add_argument("--progress", default="scn_progress.json",
                   help="Progress tracker file")
    return p.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("batch_scrape.log", encoding="utf-8"),
        ],
    )

    args = parse_args()
    run_batch(
        csv_paths     = [Path(p) for p in args.csv],
        cookie_file   = Path(args.cookies),
        output_path   = Path(args.output),
        progress_path = Path(args.progress),
    )

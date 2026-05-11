#!/usr/bin/env python3
"""Update one Shopify product metafield per CSV row using product handle lookup."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, request

DEFAULT_API_VERSION = "2025-10"
DEFAULT_CSV = "shopify_import.csv"
DEFAULT_NAMESPACE = "custom"
DEFAULT_KEY = "shipping_time"
DEFAULT_TYPE = "single_line_text_field"
DEFAULT_REPORT_PATH = Path("output/shopify_metafield_update_report.csv")


@dataclass
class RowResult:
    handle: str
    product_id: str
    status: str
    reason: str
    metafield_namespace: str
    metafield_key: str
    value_preview: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default=DEFAULT_CSV, help="Input CSV file path.")
    parser.add_argument("--shop-domain", default=os.getenv("SHOPIFY_SHOP_DOMAIN"))
    parser.add_argument("--access-token", default=os.getenv("SHOPIFY_ADMIN_ACCESS_TOKEN"))
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--key", default=DEFAULT_KEY)
    parser.add_argument("--type", dest="metafield_type", default=DEFAULT_TYPE)
    parser.add_argument("--api-version", default=DEFAULT_API_VERSION)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    return parser.parse_args()


def build_value_column_name(key: str) -> str:
    return f"Shipping Time (product.metafields.custom.{key})"


def graphql_request(
    *,
    shop_domain: str,
    access_token: str,
    api_version: str,
    query: str,
    variables: dict[str, Any] | None = None,
    max_attempts: int = 5,
) -> dict[str, Any]:
    url = f"https://{shop_domain}/admin/api/{api_version}/graphql.json"
    payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")

    for attempt in range(max_attempts):
        req = request.Request(
            url=url,
            data=payload,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-Shopify-Access-Token": access_token,
            },
        )
        try:
            with request.urlopen(req, timeout=30) as response:
                body = response.read().decode("utf-8")
                return json.loads(body)
        except error.HTTPError as exc:
            if exc.code in (429, 500, 502, 503, 504) and attempt < (max_attempts - 1):
                sleep_s = min(2**attempt, 16)
                print(f"Retrying after HTTP {exc.code}; sleeping {sleep_s}s", file=sys.stderr)
                time.sleep(sleep_s)
                continue
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"GraphQL HTTP error {exc.code}: {detail}") from exc
        except error.URLError as exc:
            if attempt < (max_attempts - 1):
                sleep_s = min(2**attempt, 16)
                print(f"Retrying after network error; sleeping {sleep_s}s", file=sys.stderr)
                time.sleep(sleep_s)
                continue
            raise RuntimeError(f"GraphQL network error: {exc}") from exc

    raise RuntimeError("Unreachable retry state")


def lookup_product_id(shop_domain: str, access_token: str, api_version: str, handle: str) -> tuple[str | None, str]:
    query = """
    query ProductByHandle($q: String!) {
      products(first: 2, query: $q) {
        edges {
          node {
            id
            handle
          }
        }
      }
    }
    """
    data = graphql_request(
        shop_domain=shop_domain,
        access_token=access_token,
        api_version=api_version,
        query=query,
        variables={"q": f"handle:{handle}"},
    )
    if data.get("errors"):
        return None, f"lookup_graphql_errors={data['errors']}"

    edges = data.get("data", {}).get("products", {}).get("edges", [])
    exact = [edge["node"] for edge in edges if edge.get("node", {}).get("handle") == handle]
    if len(exact) == 0:
        return None, "product_not_found"
    if len(exact) > 1:
        return None, "multiple_products_found"
    return exact[0]["id"], "ok"


def set_metafield(
    *,
    shop_domain: str,
    access_token: str,
    api_version: str,
    owner_id: str,
    namespace: str,
    key: str,
    metafield_type: str,
    value: str,
) -> tuple[bool, str]:
    mutation = """
    mutation SetMetafield($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          id
          key
          namespace
        }
        userErrors {
          field
          message
          code
        }
      }
    }
    """
    data = graphql_request(
        shop_domain=shop_domain,
        access_token=access_token,
        api_version=api_version,
        query=mutation,
        variables={
            "metafields": [
                {
                    "ownerId": owner_id,
                    "namespace": namespace,
                    "key": key,
                    "type": metafield_type,
                    "value": value,
                }
            ]
        },
    )
    if data.get("errors"):
        return False, f"mutation_graphql_errors={data['errors']}"
    user_errors = data.get("data", {}).get("metafieldsSet", {}).get("userErrors", [])
    if user_errors:
        return False, f"mutation_user_errors={user_errors}"
    return True, "updated"


def main() -> int:
    args = parse_args()
    if not args.shop_domain or not args.access_token:
        print("Error: --shop-domain and --access-token are required (or env vars).", file=sys.stderr)
        return 2

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"Error: CSV not found: {csv_path}", file=sys.stderr)
        return 2

    value_column = build_value_column_name(args.key)
    results: list[RowResult] = []
    processed = updated = skipped = failed = 0

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            if args.limit is not None and processed >= args.limit:
                break

            processed += 1
            handle = (row.get("Handle") or "").strip()
            value = (row.get(value_column) or "").strip()
            value_preview = value[:80]

            if not handle:
                skipped += 1
                results.append(RowResult("", "", "skipped", "empty_handle", args.namespace, args.key, value_preview))
                continue
            if not value:
                skipped += 1
                results.append(RowResult(handle, "", "skipped", "empty_value", args.namespace, args.key, ""))
                continue

            try:
                product_id, reason = lookup_product_id(args.shop_domain, args.access_token, args.api_version, handle)
                if not product_id:
                    skipped += 1
                    results.append(RowResult(handle, "", "skipped", reason, args.namespace, args.key, value_preview))
                    continue

                if args.dry_run:
                    updated += 1
                    results.append(RowResult(handle, product_id, "dry_run", "would_update", args.namespace, args.key, value_preview))
                    continue

                ok, reason = set_metafield(
                    shop_domain=args.shop_domain,
                    access_token=args.access_token,
                    api_version=args.api_version,
                    owner_id=product_id,
                    namespace=args.namespace,
                    key=args.key,
                    metafield_type=args.metafield_type,
                    value=value,
                )
                if ok:
                    updated += 1
                    results.append(RowResult(handle, product_id, "updated", reason, args.namespace, args.key, value_preview))
                else:
                    failed += 1
                    results.append(RowResult(handle, product_id, "failed", reason, args.namespace, args.key, value_preview))
            except Exception as exc:  # continue per-row on failures
                failed += 1
                results.append(RowResult(handle, "", "failed", str(exc), args.namespace, args.key, value_preview))

    DEFAULT_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with DEFAULT_REPORT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "handle",
                "product_id",
                "status",
                "reason",
                "metafield_namespace",
                "metafield_key",
                "value_preview",
            ],
        )
        writer.writeheader()
        for result in results:
            writer.writerow(result.__dict__)

    print(f"Report written to {DEFAULT_REPORT_PATH}")
    print(f"Totals: processed={processed}, updated={updated}, skipped={skipped}, failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

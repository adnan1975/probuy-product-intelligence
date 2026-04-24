#!/usr/bin/env python3
import json
import sys

from api.search_sync import MeilisearchUnavailableError, sync_meilisearch_index


def main() -> int:
    try:
        result = sync_meilisearch_index()
    except (MeilisearchUnavailableError, ValueError) as exc:
        print(f"sync failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

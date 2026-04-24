import json
import os
from typing import Any
from urllib import error, request


class MeilisearchUnavailableError(Exception):
    """Raised when Meilisearch is selected but cannot be used."""


class MeilisearchClient:
    def __init__(self, host: str, api_key: str | None, index_name: str, timeout_seconds: float = 2.0):
        self.host = host.rstrip("/")
        self.api_key = api_key
        self.index_name = index_name
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_env(cls) -> "MeilisearchClient":
        host = os.getenv("MEILISEARCH_HOST", "http://localhost:7700")
        index_name = os.getenv("MEILISEARCH_INDEX", "products")
        api_key = os.getenv("MEILISEARCH_API_KEY")
        timeout_seconds = float(os.getenv("MEILISEARCH_TIMEOUT_SECONDS", "2.0"))
        return cls(
            host=host,
            api_key=api_key,
            index_name=index_name,
            timeout_seconds=timeout_seconds,
        )

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _post_json(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            req = request.Request(
                url=url,
                method="POST",
                data=json.dumps(payload).encode("utf-8"),
                headers=self._headers(),
            )
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError, ValueError) as exc:
            raise MeilisearchUnavailableError(f"Meilisearch request failed: {exc}") from exc

    def _get_json(self, url: str) -> dict[str, Any]:
        try:
            req = request.Request(url=url, method="GET", headers=self._headers())
            with request.urlopen(req, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError, ValueError) as exc:
            raise MeilisearchUnavailableError(f"Meilisearch health check failed: {exc}") from exc

    def health(self) -> dict[str, Any]:
        return self._get_json(f"{self.host}/health")

    def search_products(
        self,
        query: str,
        brand: str | None,
        source: str | None,
        attribute_filters: dict[str, str],
        limit: int,
        offset: int,
    ) -> dict[str, Any]:
        filters: list[str] = []
        if brand:
            filters.append(f'brand = "{brand.strip()}"')
        if source:
            filters.append(f'source_code = "{source.strip().upper()}"')
        for key, value in attribute_filters.items():
            escaped_value = value.replace('"', '\\"')
            filters.append(f'attributes.{key} = "{escaped_value}"')

        payload: dict[str, Any] = {
            "q": query.strip(),
            "limit": limit,
            "offset": offset,
        }
        if filters:
            payload["filter"] = " AND ".join(filters)

        return self._post_json(
            f"{self.host}/indexes/{self.index_name}/search",
            payload,
        )

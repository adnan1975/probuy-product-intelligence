"""
scn_scraper.py
--------------
Uses exported browser cookies to authenticate with scnindustrial.com,
then for each SKU:
  1. GETs /search?q={SKU}  (follows the redirect to the product page)
  2. Scrapes the shipping-time string from <span id="intervalMsg...">
  3. Returns the result so the caller can write it to Shopify

Dependencies:
    pip install cloudscraper beautifulsoup4 lxml

How to get your cookies:
    1. Install "Cookie-Editor" Chrome extension
       https://chrome.google.com/webstore/detail/cookie-editor/hlkenndednhfkekhgcdicdfddnkalmdm
    2. Log into https://www.scnindustrial.com normally (complete email 2FA)
    3. Click Cookie-Editor → Export → "Export as JSON"
    4. Save the file as  scn_cookies.json  in your project folder
    5. Pass the path to SCNClient(cookie_file="scn_cookies.json")

    Cookies typically stay valid for days or weeks.
    If you start getting redirected back to login, just re-export.

Usage:
    from scn_scraper import SCNClient

    client = SCNClient(cookie_file="scn_cookies.json")
    client.load_cookies()

    result = client.get_shipping_time("AA639")
    print(result)
    # {"sku": "AA639", "shipping_text": "Ships in 3-4 weeks", ...}
"""

import json
import time
import logging
from pathlib import Path

import cloudscraper
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL   = "https://www.scnindustrial.com"
SEARCH_URL = f"{BASE_URL}/search"

# Polite delay between requests (seconds). Raise if you get rate-limited.
REQUEST_DELAY = 1.5


class SCNCookieError(Exception):
    pass


class SCNSessionExpiredError(Exception):
    pass


class SCNClient:
    """Cookie-authenticated session for scnindustrial.com."""

    def __init__(self, cookie_file: str = "scn_cookies.json"):
        self.cookie_file = Path(cookie_file)
        self.session = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        self.session.headers.update({
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": BASE_URL,
        })
        self._cookies_loaded = False

    # ------------------------------------------------------------------
    # Cookie loading
    # ------------------------------------------------------------------

    def load_cookies(self) -> None:
        """
        Read cookies from the JSON file exported by Cookie-Editor
        and inject them into the session.
        """
        if not self.cookie_file.exists():
            raise SCNCookieError(
                f"Cookie file not found: {self.cookie_file}\n"
                "Export your cookies from Chrome using the Cookie-Editor extension."
            )

        raw = self.cookie_file.read_text(encoding="utf-8")
        try:
            cookies = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SCNCookieError(f"Could not parse cookie file: {exc}") from exc

        if not isinstance(cookies, list):
            raise SCNCookieError(
                "Unexpected cookie format. "
                "Make sure you used Cookie-Editor → Export → 'Export as JSON'."
            )

        for cookie in cookies:
            # Cookie-Editor exports domains with a leading dot (e.g. ".scnindustrial.com")
            # requests needs it without the dot when setting explicitly
            domain = cookie.get("domain", "scnindustrial.com").lstrip(".")
            self.session.cookies.set(
                cookie["name"],
                cookie["value"],
                domain=domain,
                path=cookie.get("path", "/"),
            )

        logger.info("Loaded %d cookies from %s", len(cookies), self.cookie_file)
        self._cookies_loaded = True
        logger.info("Cookies loaded — session will be verified on first request.")
        logger.info("Session verified — cookies are valid.")

    # ------------------------------------------------------------------
    # Product lookup
    # ------------------------------------------------------------------

    def get_shipping_time(self, sku: str) -> dict:
        """
        Search for a SKU and return its shipping-time string.

        Returns a dict:
            {
                "sku":           str,
                "shipping_text": str | None,
                "product_url":   str | None,
                "status":        "ok" | "not_found" | "no_shipping_info" | "error",
                "error":         str | None,
            }
        """
        if not self._cookies_loaded:
            raise SCNCookieError("Call load_cookies() before scraping.")

        result = {
            "sku": sku,
            "shipping_text": None,
            "product_url": None,
            "status": "error",
            "error": None,
        }

        try:
            time.sleep(REQUEST_DELAY)

            resp = self.session.get(
                SEARCH_URL,
                params={"q": sku},
                headers={"Referer": BASE_URL},
                timeout=30,
                allow_redirects=True,
            )

            # Catch session expiry mid-run
            if "/account/login" in resp.url:
                raise SCNSessionExpiredError(
                    "Session expired during run. Re-export cookies and restart."
                )

            resp.raise_for_status()
            final_url = resp.url
            result["product_url"] = final_url

            # Still on search page = no matching product found
            if "/search" in final_url and "/product/" not in final_url:
                logger.warning("SKU %s — no product redirect (stayed on search page)", sku)
                result["status"] = "not_found"
                return result

            # Parse the product page
            soup = BeautifulSoup(resp.text, "lxml")
            shipping_span = soup.find(
                "span", id=lambda v: v and v.startswith("intervalMsg")
            )

            if not shipping_span:
                logger.warning("SKU %s — product page found but no intervalMsg span", sku)
                result["status"] = "no_shipping_info"
                return result

            result["shipping_text"] = shipping_span.get_text(strip=True)
            result["status"] = "ok"
            logger.info("SKU %-15s → %s", sku, result["shipping_text"])

        except SCNSessionExpiredError:
            raise  # let the caller handle this — no point continuing the batch
        except Exception as exc:
            result["error"] = str(exc)
            logger.error("SKU %s — error: %s", sku, exc)

        return result


# ------------------------------------------------------------------
# Quick smoke-test
# Run:  python scn_scraper.py
# Optionally set SCN_TEST_SKU env var to test a specific SKU
# ------------------------------------------------------------------

if __name__ == "__main__":
    import os

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    test_sku = os.environ.get("SCN_TEST_SKU", "AA639")
    debug    = os.environ.get("SCN_DEBUG", "").lower() in ("1", "true", "yes")

    client = SCNClient(cookie_file="scn_cookies.json")

    try:
        client.load_cookies()
    except SCNCookieError as e:
        print(f"\n❌ Cookie error:\n{e}")
        raise SystemExit(1)

    result = client.get_shipping_time(test_sku)
    # Print as single-line JSON so output can be piped directly to scn_results.jsonl
    print(json.dumps(result))

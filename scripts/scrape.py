import argparse
import asyncio
import csv
import json
import random
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp


DEFAULT_BUILD_ID = "categoriespages-consumersite-2.1186.0"
BASE_HOST = "https://www.trustpilot.com"
DEFAULT_INPUT_CATEGORIES = Path("data/catgories.csv")
DEFAULT_OUTPUT = Path("data/data.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Trustpilot category company data with asyncio + aiohttp."
    )
    parser.add_argument(
        "--categories-csv",
        type=Path,
        default=DEFAULT_INPUT_CATEGORIES,
        help="Path to categories CSV (default: data/catgories.csv).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output CSV path (default: data/data.csv).",
    )
    parser.add_argument(
        "--build-id",
        default=None,
        help="Optional fixed Next.js build id. If omitted, auto-detected.",
    )
    parser.add_argument(
        "--request-concurrency",
        type=int,
        default=8,
        help="Maximum concurrent HTTP requests.",
    )
    parser.add_argument(
        "--category-concurrency",
        type=int,
        default=2,
        help="Maximum categories processed concurrently.",
    )
    parser.add_argument(
        "--page-batch-size",
        type=int,
        default=12,
        help="How many pages per category to schedule at once.",
    )
    parser.add_argument(
        "--request-delay-ms",
        type=int,
        default=120,
        help="Random delay (0..N ms) before each request to reduce rate limiting.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Retries per request.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=40,
        help="HTTP timeout in seconds.",
    )
    return parser.parse_args()


def load_category_ids(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Categories CSV not found: {path}")
    ids: List[str] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            category_id = (row.get("category_id") or "").strip()
            if category_id:
                ids.append(category_id)
    # Preserve order while deduplicating.
    return list(dict.fromkeys(ids))


class Scraper:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        build_id: str,
        request_concurrency: int,
        category_concurrency: int,
        max_retries: int,
        page_batch_size: int,
        request_delay_ms: int,
    ) -> None:
        self.session = session
        self.build_id = build_id
        self.request_semaphore = asyncio.Semaphore(request_concurrency)
        self.category_semaphore = asyncio.Semaphore(category_concurrency)
        self.max_retries = max_retries
        self.page_batch_size = max(1, page_batch_size)
        self.request_delay_ms = max(0, request_delay_ms)
        self.rows: List[Dict[str, Any]] = []
        self._rows_lock = asyncio.Lock()

        self.headers = {
            "accept": "*/*",
            "x-nextjs-data": "1",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
        }

    async def fetch_json(self, slug: str, page: Optional[int]) -> Optional[Dict[str, Any]]:
        url = f"{BASE_HOST}/_next/data/{self.build_id}/categories/{slug}.json"
        params = {"categoryId": slug}
        # Important: page=1 returns redirect metadata. Omit page for first page.
        if page is not None and page > 1:
            params["page"] = str(page)

        for attempt in range(1, self.max_retries + 1):
            try:
                async with self.request_semaphore:
                    if self.request_delay_ms:
                        await asyncio.sleep(random.uniform(0, self.request_delay_ms / 1000.0))
                    async with self.session.get(
                        url, params=params, headers=self.headers
                    ) as resp:
                        if resp.status in (403, 429, 500, 502, 503, 504):
                            wait = min(10.0, 0.7 * (2 ** (attempt - 1))) + random.random()
                            await asyncio.sleep(wait)
                            continue
                        if resp.status != 200:
                            text = await resp.text()
                            print(
                                f"[WARN] {slug} page={page or 1} status={resp.status} "
                                f"body={text[:200]!r}"
                            )
                            return None
                        return await resp.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt == self.max_retries:
                    print(f"[ERROR] {slug} page={page or 1} failed: {exc}")
                    return None
                wait = min(10.0, 0.7 * (2 ** (attempt - 1))) + random.random()
                await asyncio.sleep(wait)
        return None

    @staticmethod
    def _extract_page_data(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        page_props = payload.get("pageProps") or {}
        # Redirect payloads do not include business data.
        if "__N_REDIRECT" in page_props:
            return None
        business_units = page_props.get("businessUnits") or {}
        businesses = business_units.get("businesses") or []
        total_pages = int(business_units.get("totalPages") or 1)
        total_hits = int(business_units.get("totalHits") or 0)
        return {
            "businesses": businesses,
            "total_pages": total_pages,
            "total_hits": total_hits,
        }

    @staticmethod
    def _business_to_row(category_id: str, page: int, total_pages: int, bu: Dict[str, Any]) -> Dict[str, Any]:
        location = bu.get("location") or {}
        contact = bu.get("contact") or {}
        categories = bu.get("categories") or []

        return {
            "category_id": category_id,
            "page": page,
            "total_pages": total_pages,
            "business_unit_id": bu.get("businessUnitId", ""),
            "identifying_name": bu.get("identifyingName", ""),
            "display_name": bu.get("displayName", ""),
            "stars": bu.get("stars", ""),
            "trust_score": bu.get("trustScore", ""),
            "number_of_reviews": bu.get("numberOfReviews", ""),
            "is_recommended_in_categories": bu.get("isRecommendedInCategories", ""),
            "website": contact.get("website", ""),
            "email": contact.get("email", ""),
            "phone": contact.get("phone", ""),
            "address": location.get("address", ""),
            "city": location.get("city", ""),
            "zip_code": location.get("zipCode", ""),
            "country": location.get("country", ""),
            "logo_url": bu.get("logoUrl", ""),
            "business_categories_json": json.dumps(categories, ensure_ascii=False),
            "profile_url": (
                f"{BASE_HOST}/review/{bu.get('identifyingName', '')}"
                if bu.get("identifyingName")
                else ""
            ),
        }

    async def scrape_category(self, category_id: str) -> None:
        async with self.category_semaphore:
            first_payload = await self.fetch_json(category_id, None)
            if not first_payload:
                print(f"[WARN] Skipped category={category_id}: no first-page payload")
                return

            first_data = self._extract_page_data(first_payload)
            if not first_data:
                print(f"[WARN] Skipped category={category_id}: unexpected first-page payload")
                return

            total_pages = max(1, first_data["total_pages"])
            category_rows: List[Dict[str, Any]] = [
                self._business_to_row(category_id, 1, total_pages, bu)
                for bu in first_data["businesses"]
            ]

            if total_pages > 1:
                pages = list(range(2, total_pages + 1))
                for i in range(0, len(pages), self.page_batch_size):
                    batch_pages = pages[i : i + self.page_batch_size]
                    tasks = {
                        page: asyncio.create_task(self.fetch_json(category_id, page))
                        for page in batch_pages
                    }
                    for page, task in tasks.items():
                        payload = await task
                        if not payload:
                            continue
                        page_data = self._extract_page_data(payload)
                        if not page_data:
                            continue
                        for bu in page_data["businesses"]:
                            category_rows.append(
                                self._business_to_row(category_id, page, total_pages, bu)
                            )

            async with self._rows_lock:
                self.rows.extend(category_rows)

            print(
                f"[OK] category={category_id} pages={total_pages} "
                f"rows={len(category_rows)} total_hits={first_data['total_hits']}"
            )

    async def scrape_all(self, category_ids: List[str]) -> List[Dict[str, Any]]:
        tasks = [asyncio.create_task(self.scrape_category(cid)) for cid in category_ids]
        await asyncio.gather(*tasks)
        return self.rows


async def detect_build_id(session: aiohttp.ClientSession) -> Optional[str]:
    url = f"{BASE_HOST}/categories"
    headers = {"user-agent": "Mozilla/5.0"}
    async with session.get(url, headers=headers) as resp:
        if resp.status != 200:
            return None
        html = await resp.text()
    m = re.search(r'"buildId":"([^"]+)"', html)
    if not m:
        return None
    return m.group(1)


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "category_id",
        "page",
        "total_pages",
        "business_unit_id",
        "identifying_name",
        "display_name",
        "stars",
        "trust_score",
        "number_of_reviews",
        "is_recommended_in_categories",
        "website",
        "email",
        "phone",
        "address",
        "city",
        "zip_code",
        "country",
        "logo_url",
        "business_categories_json",
        "profile_url",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


async def async_main(args: argparse.Namespace) -> None:
    category_ids = load_category_ids(args.categories_csv)
    timeout = aiohttp.ClientTimeout(total=args.timeout_seconds)
    connector = aiohttp.TCPConnector(ssl=False)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=False) as session:
        build_id = args.build_id
        if not build_id:
            detected = await detect_build_id(session)
            build_id = detected or DEFAULT_BUILD_ID
        print(f"[INFO] build_id={build_id}")
        print(f"[INFO] categories={len(category_ids)}")

        scraper = Scraper(
            session=session,
            build_id=build_id,
            request_concurrency=args.request_concurrency,
            category_concurrency=args.category_concurrency,
            max_retries=args.max_retries,
            page_batch_size=args.page_batch_size,
            request_delay_ms=args.request_delay_ms,
        )
        rows = await scraper.scrape_all(category_ids)
        write_csv(args.output, rows)
        print(f"[DONE] wrote {len(rows)} rows to {args.output}")


def main() -> None:
    args = parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()

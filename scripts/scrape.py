import argparse
import asyncio
import csv
import json
import random
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp


DEFAULT_BUILD_ID = "categoriespages-consumersite-2.1186.0"
BASE_HOST = "https://www.trustpilot.com"
DEFAULT_INPUT_CATEGORIES = Path("data/catgories.csv")
DEFAULT_OUTPUT = Path("data/data.csv")
DEFAULT_STATE_FILE = Path("data/scrape_state.json")

CSV_FIELDS = [
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resumable Trustpilot category scraper using asyncio + aiohttp."
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
        "--state-file",
        type=Path,
        default=DEFAULT_STATE_FILE,
        help="Checkpoint state file path (default: data/scrape_state.json).",
    )
    parser.add_argument(
        "--build-id",
        default=None,
        help="Optional fixed Next.js build id. If omitted, auto-detected.",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume from checkpoint if available (default: true).",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Force fresh run (clears checkpoint and output).",
    )
    parser.add_argument(
        "--request-concurrency",
        type=int,
        default=6,
        help="Maximum concurrent HTTP requests.",
    )
    parser.add_argument(
        "--category-concurrency",
        type=int,
        default=1,
        help="Maximum categories processed concurrently.",
    )
    parser.add_argument(
        "--page-batch-size",
        type=int,
        default=6,
        help="How many pages per category to schedule at once.",
    )
    parser.add_argument(
        "--request-delay-ms",
        type=int,
        default=250,
        help="Random delay (0..N ms) before each request.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=6,
        help="Retries per request.",
    )
    parser.add_argument(
        "--retry-rounds",
        type=int,
        default=6,
        help="Rounds to re-try failed pages after main pass.",
    )
    parser.add_argument(
        "--retry-pause-seconds",
        type=int,
        default=20,
        help="Pause between failed-page retry rounds.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=40,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--limit-categories",
        type=int,
        default=0,
        help="Only scrape first N categories (0 means all).",
    )
    return parser.parse_args()


def load_category_ids(path: Path, limit: int = 0) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Categories CSV not found: {path}")
    ids: List[str] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            category_id = (row.get("category_id") or "").strip()
            if category_id:
                ids.append(category_id)
    deduped = list(dict.fromkeys(ids))
    if limit > 0:
        return deduped[:limit]
    return deduped


class CSVAppender:
    def __init__(self, path: Path, fieldnames: List[str], fresh: bool) -> None:
        self.path = path
        self.fieldnames = fieldnames
        self._lock = asyncio.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

        if fresh and self.path.exists():
            self.path.unlink()
        if not self.path.exists():
            with self.path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()

    async def append_rows(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        async with self._lock:
            with self.path.open("a", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writerows(rows)


class Checkpoint:
    def __init__(self, path: Path, build_id: str, fresh: bool, resume: bool) -> None:
        self.path = path
        self.build_id = build_id
        self._lock = asyncio.Lock()
        self._dirty_updates = 0
        self._save_every = 20

        self.processed_pages: Dict[str, Set[int]] = {}
        self.category_total_pages: Dict[str, int] = {}
        self.failed_pages: Set[Tuple[str, int]] = set()

        if fresh or not resume:
            self._initialize_empty()
            self.save()
            return

        if self.path.exists():
            self._load_from_disk()
            # Build id changed means old page URLs may be stale.
            if self._loaded_build_id and self._loaded_build_id != self.build_id:
                print(
                    f"[INFO] build_id changed ({self._loaded_build_id} -> {self.build_id}); "
                    "checkpoint reset."
                )
                self._initialize_empty()
                self.save()
        else:
            self._initialize_empty()
            self.save()

    def _initialize_empty(self) -> None:
        self._loaded_build_id = self.build_id
        self.processed_pages = {}
        self.category_total_pages = {}
        self.failed_pages = set()

    def _load_from_disk(self) -> None:
        data = json.loads(self.path.read_text(encoding="utf-8"))
        self._loaded_build_id = data.get("build_id")
        self.processed_pages = {
            k: set(int(v) for v in vals)
            for k, vals in (data.get("processed_pages") or {}).items()
        }
        self.category_total_pages = {
            k: int(v) for k, v in (data.get("category_total_pages") or {}).items()
        }
        self.failed_pages = {
            (item["category_id"], int(item["page"]))
            for item in (data.get("failed_pages") or [])
            if "category_id" in item and "page" in item
        }

    def _to_dict(self) -> Dict[str, Any]:
        return {
            "version": 1,
            "build_id": self.build_id,
            "processed_pages": {
                category_id: sorted(list(pages))
                for category_id, pages in self.processed_pages.items()
            },
            "category_total_pages": self.category_total_pages,
            "failed_pages": [
                {"category_id": category_id, "page": page}
                for category_id, page in sorted(self.failed_pages)
            ],
        }

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(self._to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    async def _save_if_needed(self, force: bool = False) -> None:
        self._dirty_updates += 1
        if force or self._dirty_updates >= self._save_every:
            self.save()
            self._dirty_updates = 0

    async def is_page_done(self, category_id: str, page: int) -> bool:
        async with self._lock:
            return page in self.processed_pages.get(category_id, set())

    async def mark_done(
        self,
        category_id: str,
        page: int,
        total_pages: Optional[int] = None,
    ) -> None:
        async with self._lock:
            self.processed_pages.setdefault(category_id, set()).add(page)
            self.failed_pages.discard((category_id, page))
            if total_pages is not None and total_pages > 0:
                self.category_total_pages[category_id] = total_pages
            await self._save_if_needed()

    async def mark_failed(self, category_id: str, page: int) -> None:
        async with self._lock:
            if page not in self.processed_pages.get(category_id, set()):
                self.failed_pages.add((category_id, page))
            await self._save_if_needed()

    async def get_total_pages(self, category_id: str) -> Optional[int]:
        async with self._lock:
            return self.category_total_pages.get(category_id)

    async def get_pending_pages(self, category_id: str, total_pages: int) -> List[int]:
        async with self._lock:
            done = self.processed_pages.get(category_id, set())
            return [p for p in range(1, total_pages + 1) if p not in done]

    async def pop_failed_pages_snapshot(self) -> List[Tuple[str, int]]:
        async with self._lock:
            return sorted(self.failed_pages)

    async def flush(self) -> None:
        async with self._lock:
            await self._save_if_needed(force=True)


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
        writer: CSVAppender,
        checkpoint: Checkpoint,
    ) -> None:
        self.session = session
        self.build_id = build_id
        self.request_semaphore = asyncio.Semaphore(request_concurrency)
        self.category_semaphore = asyncio.Semaphore(category_concurrency)
        self.max_retries = max_retries
        self.page_batch_size = max(1, page_batch_size)
        self.request_delay_ms = max(0, request_delay_ms)
        self.writer = writer
        self.checkpoint = checkpoint

        self.headers = {
            "accept": "*/*",
            "x-nextjs-data": "1",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
        }

    async def fetch_json(self, slug: str, page: int) -> Optional[Dict[str, Any]]:
        url = f"{BASE_HOST}/_next/data/{self.build_id}/categories/{slug}.json"
        params = {"categoryId": slug}
        # page=1 should be omitted for this endpoint.
        if page > 1:
            params["page"] = str(page)

        for attempt in range(1, self.max_retries + 1):
            try:
                async with self.request_semaphore:
                    if self.request_delay_ms:
                        await asyncio.sleep(random.uniform(0, self.request_delay_ms / 1000.0))
                    async with self.session.get(url, params=params, headers=self.headers) as resp:
                        if resp.status in (403, 429, 500, 502, 503, 504):
                            wait = min(15.0, 0.9 * (2 ** (attempt - 1))) + random.random()
                            await asyncio.sleep(wait)
                            continue
                        if resp.status != 200:
                            text = await resp.text()
                            print(
                                f"[WARN] {slug} page={page} status={resp.status} "
                                f"body={text[:180]!r}"
                            )
                            return None
                        return await resp.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt == self.max_retries:
                    print(f"[ERROR] {slug} page={page} failed: {exc}")
                    return None
                wait = min(15.0, 0.9 * (2 ** (attempt - 1))) + random.random()
                await asyncio.sleep(wait)
        return None

    @staticmethod
    def extract_page_data(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        page_props = payload.get("pageProps") or {}
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
    def business_to_row(category_id: str, page: int, total_pages: int, bu: Dict[str, Any]) -> Dict[str, Any]:
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

    async def process_single_page(self, category_id: str, page: int) -> bool:
        if await self.checkpoint.is_page_done(category_id, page):
            return True

        payload = await self.fetch_json(category_id, page)
        if not payload:
            await self.checkpoint.mark_failed(category_id, page)
            return False

        page_data = self.extract_page_data(payload)
        if not page_data:
            await self.checkpoint.mark_failed(category_id, page)
            return False

        total_pages = max(1, page_data["total_pages"])
        rows = [
            self.business_to_row(category_id, page, total_pages, bu)
            for bu in page_data["businesses"]
        ]
        await self.writer.append_rows(rows)
        await self.checkpoint.mark_done(category_id, page, total_pages=total_pages)
        return True

    async def scrape_category(self, category_id: str) -> None:
        async with self.category_semaphore:
            # Ensure page 1 is completed first to discover total_pages reliably.
            first_ok = await self.process_single_page(category_id, 1)
            if not first_ok:
                print(f"[WARN] category={category_id} page=1 failed; queued for retry")
                return

            total_pages = await self.checkpoint.get_total_pages(category_id)
            if not total_pages:
                total_pages = 1

            pending_pages = await self.checkpoint.get_pending_pages(category_id, total_pages)
            remaining = [p for p in pending_pages if p > 1]
            if not remaining:
                print(f"[OK] category={category_id} pages={total_pages} up-to-date")
                return

            done_count = 0
            fail_count = 0
            for i in range(0, len(remaining), self.page_batch_size):
                batch = remaining[i : i + self.page_batch_size]
                tasks = {p: asyncio.create_task(self.process_single_page(category_id, p)) for p in batch}
                for page, task in tasks.items():
                    ok = await task
                    if ok:
                        done_count += 1
                    else:
                        fail_count += 1
                        print(f"[WARN] category={category_id} page={page} queued for retry")

            print(
                f"[OK] category={category_id} pages={total_pages} "
                f"done={done_count + 1} failed={fail_count}"
            )

    async def scrape_all(self, category_ids: List[str]) -> None:
        tasks = [asyncio.create_task(self.scrape_category(cid)) for cid in category_ids]
        await asyncio.gather(*tasks)

    async def retry_failed_pages(self, rounds: int, pause_seconds: int) -> None:
        for round_idx in range(1, rounds + 1):
            failed = await self.checkpoint.pop_failed_pages_snapshot()
            if not failed:
                print("[INFO] no failed pages left")
                return

            print(f"[INFO] retry round {round_idx}/{rounds}: {len(failed)} failed pages")
            recovered = 0
            for category_id, page in failed:
                if await self.checkpoint.is_page_done(category_id, page):
                    continue
                ok = await self.process_single_page(category_id, page)
                if ok:
                    recovered += 1

            remaining = await self.checkpoint.pop_failed_pages_snapshot()
            print(
                f"[INFO] retry round {round_idx} recovered={recovered} "
                f"remaining_failed={len(remaining)}"
            )
            if not remaining:
                return
            await asyncio.sleep(max(0, pause_seconds))


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


async def async_main(args: argparse.Namespace) -> None:
    category_ids = load_category_ids(args.categories_csv, limit=args.limit_categories)
    timeout = aiohttp.ClientTimeout(total=args.timeout_seconds)
    connector = aiohttp.TCPConnector(ssl=False)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=False) as session:
        detected = await detect_build_id(session) if not args.build_id else args.build_id
        build_id = detected or DEFAULT_BUILD_ID
        print(f"[INFO] build_id={build_id}")
        print(f"[INFO] categories={len(category_ids)}")

        # If user asks to resume but state file is missing, existing CSV cannot be trusted
        # for page-complete detection, so fresh output is safer.
        fresh_output = args.fresh or (not args.resume) or (
            args.resume and not args.state_file.exists()
        )
        if args.resume and not args.state_file.exists() and args.output.exists() and not args.fresh:
            print("[INFO] resume requested but no checkpoint found; starting fresh output.")

        writer = CSVAppender(args.output, CSV_FIELDS, fresh=fresh_output)
        checkpoint = Checkpoint(
            path=args.state_file,
            build_id=build_id,
            fresh=args.fresh,
            resume=args.resume,
        )

        scraper = Scraper(
            session=session,
            build_id=build_id,
            request_concurrency=args.request_concurrency,
            category_concurrency=args.category_concurrency,
            max_retries=args.max_retries,
            page_batch_size=args.page_batch_size,
            request_delay_ms=args.request_delay_ms,
            writer=writer,
            checkpoint=checkpoint,
        )

        await scraper.scrape_all(category_ids)
        await scraper.retry_failed_pages(
            rounds=args.retry_rounds,
            pause_seconds=args.retry_pause_seconds,
        )
        await checkpoint.flush()

        failed_left = len(await checkpoint.pop_failed_pages_snapshot())
        done_pages = sum(len(v) for v in checkpoint.processed_pages.values())
        print(
            f"[DONE] output={args.output} state={args.state_file} "
            f"processed_pages={done_pages} remaining_failed={failed_left}"
        )


def main() -> None:
    args = parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()

"""
Real-time async scraper — one independent worker per category URL.

SCRAPE_ONLY_NEW = True  (الوضع الافتراضي)
  أول دورة: warm-up — يحفظ كل الـ URLs الموجودة في _seen_urls
             بدون معالجة ولا نشر.
  من الدورة الثانية: يعالج فقط URLs جديدة لم تكن موجودة.

SCRAPE_ONLY_NEW = False
  يعامل كل URL غير موجود في DB كخبر جديد فوراً من أول دورة.
"""
from __future__ import annotations

import asyncio
import re
import time
import unicodedata
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup

from config.settings import (
    CATEGORIES,
    SCRAPE_INTERVAL_SECONDS,
    SCRAPE_ONLY_NEW,
    SCRAPE_TIMEOUT_CONNECT,
    SCRAPE_TIMEOUT_READ,
    SCRAPE_MAX_RETRIES,
    SCRAPE_RETRY_DELAY,
    MAX_SCRAPE_PAGES,
)
from DB.db import db_execute
from utils.logger import logger

BASE_URL = "https://mnsht.net"
HEADERS  = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "ar,en;q=0.9",
}


# ─────────────────────────────────────────────────────────────────────────────
# Text helpers
# ─────────────────────────────────────────────────────────────────────────────

def clean_text(text) -> str:
    return unicodedata.normalize("NFKC", str(text or "")).strip()


def clean_image_url(src: Optional[str]) -> Optional[str]:
    if not src:
        return None
    full_url = urljoin(BASE_URL, src)
    full_url = full_url.replace("/UploadCache/libfiles/", "/Upload/libfiles/")
    full_url = re.sub(r"/\d+x\d+o?/", "/", full_url)
    return full_url


def now_ts() -> datetime:
    return datetime.now(timezone.utc)


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def article_exists(url: str, title: Optional[str] = None) -> bool:
    if title:
        row = db_execute(
            "SELECT id FROM news WHERE url = %s OR title = %s LIMIT 1",
            (url, title), fetch=True,
        )
    else:
        row = db_execute(
            "SELECT id FROM news WHERE url = %s LIMIT 1",
            (url,), fetch=True,
        )
    return bool(row)


def update_scraper_health(
    category_url: str,
    category_name: str,
    *,
    success: bool,
    articles_found: int = 0,
) -> None:
    if success:
        db_execute(
            """
            INSERT INTO scraper_health (category_url, category_name, last_checked,
                last_success, consecutive_failures, articles_found, status)
            VALUES (%s, %s, NOW(), NOW(), 0, %s, 'ok')
            ON CONFLICT (category_url) DO UPDATE SET
                last_checked         = NOW(),
                last_success         = NOW(),
                consecutive_failures = 0,
                articles_found       = scraper_health.articles_found + EXCLUDED.articles_found,
                status               = 'ok'
            """,
            (category_url, category_name, articles_found),
        )
    else:
        db_execute(
            """
            INSERT INTO scraper_health (category_url, category_name, last_checked,
                last_failure, consecutive_failures, status)
            VALUES (%s, %s, NOW(), NOW(), 1, 'degraded')
            ON CONFLICT (category_url) DO UPDATE SET
                last_checked         = NOW(),
                last_failure         = NOW(),
                consecutive_failures = scraper_health.consecutive_failures + 1,
                status = CASE
                    WHEN scraper_health.consecutive_failures + 1 >= 5 THEN 'down'
                    ELSE 'degraded'
                END
            """,
            (category_url, category_name),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Async HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_html(
    session: aiohttp.ClientSession,
    url: str,
    *,
    retries: int = SCRAPE_MAX_RETRIES,
) -> Optional[str]:
    timeout = aiohttp.ClientTimeout(
        connect=SCRAPE_TIMEOUT_CONNECT,
        total=SCRAPE_TIMEOUT_READ,
    )
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, timeout=timeout) as resp:
                resp.raise_for_status()
                return await resp.text(encoding="utf-8", errors="replace")
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                wait = SCRAPE_RETRY_DELAY * attempt
                logger.warning(
                    f"⚠️ fetch attempt {attempt}/{retries} failed for {url}: "
                    f"{exc} — retrying in {wait}s"
                )
                await asyncio.sleep(wait)
    logger.error(f"❌ All {retries} fetch attempts failed for {url}: {last_exc}")
    return None


async def _fetch_article_content(
    session: aiohttp.ClientSession,
    url: str,
    max_words: int = 150,
) -> Optional[str]:
    html = await _fetch_html(session, url)
    if not html:
        return None
    try:
        soup       = BeautifulSoup(html, "lxml")
        tag        = soup.select_one("div.paragraph-list")
        if not tag:
            return None
        paragraphs = tag.find_all("p")
        content    = " ".join(p.get_text(strip=True) for p in paragraphs)
        words      = content.split()
        chunk      = " ".join(words[:max_words])
        cutoff     = " ".join(words[:30])
        rest       = chunk[len(cutoff):]
        dot_idx    = -1
        for sep in (".", ",", "؟", "!", "…"):
            idx = rest.find(sep)
            if idx != -1 and (dot_idx == -1 or idx < dot_idx):
                dot_idx = idx
        content = cutoff + rest[: dot_idx + 1] if dot_idx != -1 else chunk
        return clean_text(content) or None
    except Exception as exc:
        logger.warning(f"⚠️ Content parse error for {url}: {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Page parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_cards(html: str, category_url: str) -> list[dict]:
    """Return list of raw article dicts from one category page."""
    soup  = BeautifulSoup(html, "lxml")
    cards = soup.find_all("div", class_="item-card")
    items = []
    for card in cards:
        try:
            a_tag = card.find("a")
            if not a_tag:
                continue
            url = urljoin(BASE_URL, str(a_tag.get("href") or ""))
            if not url or url == BASE_URL:
                continue
            h3    = card.find("h3")
            title = clean_text(h3.get_text(strip=True)) if h3 else "بدون عنوان"
            img_tag = card.find("img")
            raw_src: Optional[str] = None
            if img_tag:
                _s = img_tag.get("data-src") or img_tag.get("src")
                raw_src = str(_s) if _s else None
            image = clean_image_url(raw_src)
            if image and "logo" in image.lower():
                image = None
            items.append({"url": url, "title": title, "image": image})
        except Exception as exc:
            logger.error(f"❌ Card parse error ({category_url}): {exc}")
    return items


# ─────────────────────────────────────────────────────────────────────────────
# Per-category async worker
# ─────────────────────────────────────────────────────────────────────────────

async def category_worker(
    category_url: str,
    out_queue: asyncio.Queue,
    session: aiohttp.ClientSession,
) -> None:
    """
    Continuously poll one category page and push new articles to out_queue.

    Warm-up behaviour (SCRAPE_ONLY_NEW=True):
      - First iteration: collect all current URLs into _seen_urls → skip them.
      - Second iteration onwards: only process URLs not in _seen_urls.

    Normal behaviour (SCRAPE_ONLY_NEW=False):
      - Process every URL not already in the DB from the very first iteration.
    """
    cfg          = CATEGORIES[category_url]
    cat_name     = cfg["name"]
    cat_label    = cfg["label"]
    template_key = cfg["template_key"]
    use_ai       = cfg["use_ai"]

    # In-memory seen set — keyed on URL, survives across polling cycles
    _seen_urls: set[str] = set()
    _is_warmup: bool     = SCRAPE_ONLY_NEW   # True only on the very first cycle

    if _is_warmup:
        logger.info(f"🔍 Warm-up mode: [{cat_label}] — first cycle will snapshot existing URLs only")
    else:
        logger.info(f"🚀 Worker started (process-all mode): [{cat_label}]")

    while True:
        loop_start  = time.monotonic()
        detected_at = now_ts()

        try:
            html = await _fetch_html(session, category_url)
            if html is None:
                update_scraper_health(category_url, cat_name, success=False)
                await asyncio.sleep(SCRAPE_INTERVAL_SECONDS)
                continue

            raw_items = _parse_cards(html, category_url)

            # Paginate if the first page returned fewer than 10 cards
            page = 2
            while page <= MAX_SCRAPE_PAGES and len(raw_items) < 10:
                page_html = await _fetch_html(session, f"{category_url}/page/{page}")
                if page_html:
                    raw_items += _parse_cards(page_html, category_url)
                page += 1

            # ── Warm-up cycle: snapshot URLs and exit immediately ──────────
            if _is_warmup:
                for item in raw_items:
                    _seen_urls.add(item["url"])
                _is_warmup = False
                logger.info(
                    f"✅ Warm-up done: [{cat_label}] — "
                    f"{len(_seen_urls)} existing URLs snapshotted, "
                    f"watching for NEW articles from next cycle..."
                )
                update_scraper_health(category_url, cat_name, success=True, articles_found=0)
                elapsed   = time.monotonic() - loop_start
                sleep_for = max(0.0, SCRAPE_INTERVAL_SECONDS - elapsed)
                await asyncio.sleep(sleep_for)
                continue

            # ── Normal cycles: process only genuinely new articles ─────────
            new_count = 0
            for item in raw_items:
                url = item["url"]

                # Skip if already seen this session
                if url in _seen_urls:
                    continue

                # Skip if already in DB (handles restart recovery)
                if article_exists(url, item["title"]):
                    _seen_urls.add(url)   # don't query DB again next cycle
                    continue

                # Mark as seen before processing (prevents double-queue on slow runs)
                _seen_urls.add(url)

                scraped_at = now_ts()
                content    = await _fetch_article_content(session, url)

                article = {
                    "title":        item["title"],
                    "url":          url,
                    "image":        item["image"],
                    "content":      content,
                    "source_url":   category_url,
                    "source_name":  cat_name,
                    "source_label": cat_label,
                    "template_key": template_key,
                    "use_ai":       use_ai,
                    "detected_at":  detected_at,
                    "scraped_at":   scraped_at,
                    "fetch_ts": {
                        "year":   scraped_at.year,
                        "month":  scraped_at.month,
                        "day":    scraped_at.day,
                        "hour":   scraped_at.hour,
                        "minute": scraped_at.minute,
                        "second": scraped_at.second,
                        "ms":     scraped_at.microsecond // 1000,
                        "epoch":  scraped_at.timestamp(),
                    },
                }
                await out_queue.put(article)
                new_count += 1
                logger.info(
                    f"📥 [{cat_label}] New: {item['title'][:70]} "
                    f"| scraped_at={scraped_at.strftime('%H:%M:%S.%f')[:-3]}"
                )

            update_scraper_health(
                category_url, cat_name, success=True, articles_found=new_count
            )

        except Exception as exc:
            logger.error(f"❌ Worker error [{cat_label}]: {exc}", exc_info=True)
            update_scraper_health(category_url, cat_name, success=False)

        elapsed   = time.monotonic() - loop_start
        sleep_for = max(0.0, SCRAPE_INTERVAL_SECONDS - elapsed)
        await asyncio.sleep(sleep_for)


# ─────────────────────────────────────────────────────────────────────────────
# Synchronous compatibility shim
# ─────────────────────────────────────────────────────────────────────────────

def get_html(page: int = 1) -> str:
    """Legacy sync wrapper — fetches the UAE category homepage."""
    import requests
    url = BASE_URL if page == 1 else f"{BASE_URL}/page/{page}"
    for attempt in range(1, SCRAPE_MAX_RETRIES + 1):
        try:
            resp = requests.get(
                url, headers=HEADERS,
                timeout=(SCRAPE_TIMEOUT_CONNECT, SCRAPE_TIMEOUT_READ),
            )
            resp.raise_for_status()
            return resp.text
        except Exception:
            if attempt == SCRAPE_MAX_RETRIES:
                raise
            time.sleep(SCRAPE_RETRY_DELAY)
    raise RuntimeError("get_html: all retries exhausted")
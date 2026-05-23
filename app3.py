"""
app3.py — Main entry point for the Mansht real-time news automation system.

Architecture:
  ─────────────────────────────────────────────────────────────────
  asyncio event loop (main thread)
    ├── category_worker(url_1)  ──┐
    ├── category_worker(url_2)  ──┤  push to asyncio.Queue
    ├── ...                     ──┤
    └── category_worker(url_N)  ──┘
                                    ↓
                           article_consumer
                             (process_article in ThreadPoolExecutor)

  background threads (daemon)
    ├── publishing_worker      — drains news_queue → PublishPipeline
    └── recalculation_worker   — refreshes aging scores every 60s
  ─────────────────────────────────────────────────────────────────

PUBLISHING FLOW (SIMPLIFIED)
  All articles → process_article()
    ├─ HIGH priority  → instant_publish() → PublishPipeline → Instagram+Telegram
    └─ NORMAL priority → QueueManager → publishing_worker → PublishPipeline → FB+TW+Telegram

  PublishPipeline is the ONLY component that fires webhooks.
  Idempotency is enforced via publish_log table (fingerprint per article+platform).
"""
from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from threading import Thread

import aiohttp
from dotenv import load_dotenv
from supabase import create_client

from config.settings import CATEGORIES, PARALLEL_WORKERS
from DB.cloud_storage import upload_image
from image.composer import generate_post_image
from scraper.extractor import category_worker, HEADERS as _SCRAPER_HEADERS
from scraper.publisher import send_photo
from scraper.save_news import process_article
from services.scheduler import publishing_worker
from services.recalculation_worker import recalculation_worker
from utils.logger import logger

import requests as _requests

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Environment validation
# ─────────────────────────────────────────────────────────────────────────────

_required = [
    "TOKEN", "CHAT_ID",
    "SUPABASE_URL", "SUPABASE_KEY",
    "DATABASE_URL",
    "PRIORITY_TELEGRAM_CHAT_ID",
]
_missing = [k for k in _required if not os.getenv(k)]
if _missing:
    raise RuntimeError(f"Missing required environment variables: {', '.join(_missing)}")

TOKEN        = os.getenv("TOKEN")
CHAT_ID      = os.getenv("CHAT_ID")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase  = create_client(str(SUPABASE_URL), str(SUPABASE_KEY))
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
TEMPLATES = os.path.join(BASE_DIR, "templates")

# ─────────────────────────────────────────────────────────────────────────────
# Template config
# ─────────────────────────────────────────────────────────────────────────────

TEMPLATE_CONFIG = {
    "سيارات": {
        "template":  os.path.join(TEMPLATES, "سيارات.png"),
        "image_box": (0,   0,    1080, 820),
        "text_box":  (10,  845,  1070, 1210),
        "align":     "center",
    }
    ,
    "ثقافة": {
        "template":  os.path.join(TEMPLATES, "ثقافة.png"),
        "image_box": (0,   0,    1080, 820),
        "text_box":  (10,  845,  1070, 1210),
        "align":     "center",
    }
    ,
    "تكنولوجيا": {
        "template":  os.path.join(TEMPLATES, "تكنولوجيا.png"),
        "image_box": (0,   0,    1080, 820),
        "text_box":  (10,  845,  1070, 1210),
        "align":     "center",
    }
    ,
    "اقتصاد": {
        "template":  os.path.join(TEMPLATES, "اقتصاد.png"),
        "image_box": (0,   0,    1080, 820),
        "text_box":  (10,  845,  1070, 1210),
        "align":     "center",
    },
    "رياضة": {
        "template":  os.path.join(TEMPLATES, "رياضة.png"),
        "image_box": (0,   0,    1080, 835),
        "text_box":  (20,   940,  1060, 1200),
        "align":     "center",
    },
    "سياسة": {
        "template":  os.path.join(TEMPLATES, "سياسة.png"),
        "image_box": (0,   0,    1080, 820),
        "text_box":  (10,  845,  1070, 1210),
        "align":     "center",
    },
    "فن": {
        "template":  os.path.join(TEMPLATES, "عام.png"),
        "image_box": (46,  188,  1040, 744),
        "text_box":  (10,  845,  1070, 1210),
        "align":     "center",
    },
    "عام": {
        "template":  os.path.join(TEMPLATES, "عام.png"),
        "image_box": (46,  188,  1040, 744),
        "text_box":  (10,  845,  1070, 1210),
        "align":     "center",
    },
}

# Shared sync requests session (reused across image downloads)
_sync_session = _requests.Session()
_sync_session.headers.update(_SCRAPER_HEADERS)


def _generate_image(
    title, image_url, news_id, url,
    template_key, confidence, content,
    send_to_telegram=False,
):
    """Sync image generation — called from ThreadPoolExecutor."""
    return generate_post_image(
        title=title,
        image_url=image_url,
        news_id=news_id,
        url=url,
        category=template_key,
        confidence=confidence,
        content=content,
        template_config=TEMPLATE_CONFIG,
        session=_sync_session,
        upload_fn=upload_image,
        supabase_storage=supabase.storage,
        send_telegram_fn=send_photo,
        send_to_telegram=send_to_telegram,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Async consumer
# ─────────────────────────────────────────────────────────────────────────────

async def article_consumer(
    queue: asyncio.Queue,
    executor: ThreadPoolExecutor,
) -> None:
    loop = asyncio.get_running_loop()
    while True:
        item = await queue.get()
        try:
            await loop.run_in_executor(
                executor,
                partial(process_article, item, TEMPLATE_CONFIG, _generate_image),
            )
        except Exception as exc:
            logger.error(f"❌ Consumer error: {exc}", exc_info=True)
        finally:
            queue.task_done()


# ─────────────────────────────────────────────────────────────────────────────
# Main async entrypoint
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    article_queue: asyncio.Queue = asyncio.Queue(maxsize=500)
    executor = ThreadPoolExecutor(max_workers=PARALLEL_WORKERS + 4)

    connector = aiohttp.TCPConnector(
        limit=50,
        limit_per_host=5,
        ttl_dns_cache=300,
        force_close=False,
    )
    async with aiohttp.ClientSession(
        connector=connector,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
            ),
            "Accept-Language": "ar,en;q=0.9",
        },
    ) as session:

        worker_tasks = [
            asyncio.create_task(
                category_worker(url, article_queue, session),
                name=f"worker-{cfg['name']}",
            )
            for url, cfg in CATEGORIES.items()
        ]

        consumer_tasks = [
            asyncio.create_task(
                article_consumer(article_queue, executor),
                name=f"consumer-{i}",
            )
            for i in range(min(PARALLEL_WORKERS, 8))
        ]

        logger.info(
            f"🚀 Real-time scraper started | "
            f"{len(worker_tasks)} category workers | "
            f"{len(consumer_tasks)} article consumers"
        )

        await asyncio.gather(*worker_tasks, *consumer_tasks)


if __name__ == "__main__":
    Thread(target=publishing_worker,    daemon=True, name="publishing-worker").start()
    Thread(target=recalculation_worker, daemon=True, name="recalc-worker").start()
    logger.info("🧵 Background workers started")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped manually")
    except Exception as exc:
        logger.error(f"CRASH: {exc}", exc_info=True)
        raise

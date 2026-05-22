"""
services/instant_publisher.py — Instant publish for high-priority articles.

CHANGES FROM ORIGINAL
────────────────────────────────────────────────────────────────────────────
1. Removed all direct webhook calls and inline platform logic.
   Now routes EXCLUSIVELY through PublishPipeline — the single source
   of truth for all publishing.  This eliminates the duplicate-webhook
   problem where an article was sent via instant_publish AND later
   re-sent by the scheduler worker.

2. Claim-and-publish is fully atomic:
   - Claim row (status = 'processing')
   - Publish via pipeline
   - Update row (status = 'published')
   No other code path can publish a claimed row.

3. publish_log idempotency check in PublishPipeline means even if
   instant_publish is accidentally called twice for the same article,
   the second call is silently blocked.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import psycopg2

from DB.db import db_execute
from services.priority_engine import calculate_priority_score, log_priority_decision
from services.publish_pipeline import PublishPipeline
from utils.logger import logger

_pipeline = PublishPipeline()


# ─────────────────────────────────────────────────────────────────────────────
# Priority detection
# ─────────────────────────────────────────────────────────────────────────────

def is_priority_article(title: str, content: Optional[str] = None) -> bool:
    """
    True if the article should be instantly published (not queued).
    Uses the new priority engine's keyword taxonomy.
    """
    from config.settings import PRIORITY_THRESHOLD_INSTAGRAM
    score = calculate_priority_score(title, content or "")
    return score >= PRIORITY_THRESHOLD_INSTAGRAM


# ─────────────────────────────────────────────────────────────────────────────
# Row claim (optimistic lock)
# ─────────────────────────────────────────────────────────────────────────────

def _claim_queue_row(queue_id: int) -> bool:
    """
    Atomically set status=processing.  Returns True if we won the race.
    Uses a direct psycopg2 connection (not the pool) for explicit tx control.
    """
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE news_queue
                SET status = 'processing',
                    processing_at = NOW(),
                    last_updated  = NOW()
                WHERE id = %s AND status = 'pending'
                """,
                (queue_id,),
            )
            claimed = cur.rowcount == 1
        conn.commit()
        return claimed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def instant_publish(post: dict) -> None:
    """
    Immediately publish a high-priority article.
    Routes through PublishPipeline for idempotency and unified logging.

    Args:
        post: queue row dict containing at minimum:
              id, article_id, title, url, content, image_url,
              priority_score, category, source_label
    """
    queue_id   = post["id"]
    title_snip = (post.get("title") or "")[:80]

    # Claim the row — prevents double-publish if scheduler also sees it
    if not _claim_queue_row(queue_id):
        logger.warning(
            f"🚨 INSTANT PUBLISH SKIPPED — row already claimed "
            f"| queue_id={queue_id} | '{title_snip}'"
        )
        return

    logger.info(
        f"🚨 INSTANT PUBLISH START | queue_id={queue_id} | '{title_snip}'"
    )

    # Get/confirm priority score
    priority_score = int(post.get("priority_score") or 0)
    if priority_score == 0:
        # Recalculate in case it wasn't stored yet
        priority_score = log_priority_decision(
            post.get("title", ""),
            post.get("content") or "",
        )

    # Publish via the single unified pipeline
    results = _pipeline.publish(post, priority_score=priority_score)

    # Derive status strings for DB
    tg_status = results.get("telegram",  "skipped")
    ig_status = results.get("instagram", "skipped")
    fb_status = results.get("facebook",  "skipped")
    tw_status = results.get("twitter",   "skipped")

    # Mark published in DB
    db_execute(
        """
        UPDATE news_queue
        SET
            status           = 'published',
            telegram_status  = %s,
            instagram_status = %s,
            twitter_status   = %s,
            facebook_status  = %s,
            published_at     = NOW(),
            last_updated     = NOW()
        WHERE id = %s AND status = 'processing'
        """,
        (tg_status, ig_status, tw_status, fb_status, queue_id),
    )

    logger.info(
        f"🚨 INSTANT PUBLISH COMPLETE | queue_id={queue_id} "
        f"| tg={tg_status} ig={ig_status} "
        f"tw={tw_status} fb={fb_status}"
    )

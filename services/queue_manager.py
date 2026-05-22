"""
Priority-based publish queue manager.

Queue ordering (highest priority first):
  1. Overdue articles (age > MAX_QUEUE_AGE_HOURS) — oldest first
  2. Pending articles — priority_score DESC → final_score DESC

FIX 1: reorder_queue() now uses AGING_MULTIPLIER (0.12) instead of
        MAX_QUEUE_AGE_HOURS (3.0) — the old value inflated aging scores
        by 25x causing them to overwhelm keyword scores.

FIX 2: fail_stale_processing() now returns the real DB rowcount via
        db_execute(..., return_rowcount=True).
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from config.settings import AGING_MULTIPLIER, MAX_QUEUE_AGE_HOURS
from DB.db import db_execute
from services.priority_engine import calculate_final_score

logger = logging.getLogger(__name__)


class QueueManager:

    def add_or_update_queue_item(self, article: dict) -> None:
        scores = calculate_final_score(
            article["title"],
            article.get("content") or "",
            article["created_at"],
        )

        db_execute(
            """
            INSERT INTO news_queue (
                article_id, title, url, content, image_url,
                created_at, detected_at, scraped_at, queued_at,
                keyword_score, aging_score, ai_score, final_score,
                priority_score, status, last_updated,
                category, template_key, source_label
            )
            VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,'pending',NOW(),
                %s,%s,%s
            )
            ON CONFLICT (url) DO UPDATE SET
                keyword_score  = EXCLUDED.keyword_score,
                aging_score    = EXCLUDED.aging_score,
                ai_score       = EXCLUDED.ai_score,
                final_score    = EXCLUDED.final_score,
                priority_score = EXCLUDED.priority_score,
                image_url      = EXCLUDED.image_url,
                category       = EXCLUDED.category,
                template_key   = EXCLUDED.template_key,
                source_label   = EXCLUDED.source_label,
                last_updated   = NOW()
            """,
            (
                article["article_id"],
                article["title"],
                article["url"],
                article.get("content"),
                article.get("image_url"),
                article["created_at"],
                article.get("detected_at"),
                article.get("scraped_at"),
                article.get("queued_at"),
                scores["keyword_score"],
                scores["aging_score"],
                scores["ai_score"],
                scores["final_score"],
                article.get("priority_score", 0),
                article.get("category"),
                article.get("template_key"),
                article.get("source_label"),
            ),
        )

        logger.debug(
            f"Queued article_id={article['article_id']} "
            f"score={scores['final_score']:.2f} "
            f"priority={article.get('priority_score', 0)}"
        )

    def reorder_queue(self) -> None:
        """
        Recalculate aging_score and final_score for all pending items.

        FIX: uses AGING_MULTIPLIER (0.12) — NOT MAX_QUEUE_AGE_HOURS (3.0).
             Old code: age_minutes * 3.0  → article at 60 min got +180 score
             New code: age_minutes * 0.12 → article at 60 min gets +7.2 score
             This keeps aging as a gentle tiebreaker, not a dominator.
        """
        db_execute(
            """
            UPDATE news_queue
            SET
                aging_score = (EXTRACT(EPOCH FROM NOW()) - created_at) / 60.0 * %s,
                final_score = keyword_score
                            + (EXTRACT(EPOCH FROM NOW()) - created_at) / 60.0 * %s
                            + ai_score
            WHERE status = 'pending'
            """,
            (AGING_MULTIPLIER, AGING_MULTIPLIER),
        )

    def get_next_post(self) -> Optional[dict]:
        """
        Atomically claim the next highest-priority pending article.
        Uses SELECT FOR UPDATE SKIP LOCKED for safe concurrent access.
        """
        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        try:
            conn.autocommit = False
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM news_queue
                    WHERE status = 'pending'
                    ORDER BY
                        CASE
                            WHEN (EXTRACT(EPOCH FROM NOW()) - created_at) / 3600
                                 > %(max_age)s THEN 0
                            ELSE 1
                        END ASC,
                        created_at ASC,
                        priority_score DESC,
                        final_score DESC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """,
                    {"max_age": MAX_QUEUE_AGE_HOURS},
                )
                row = cur.fetchone()
                if not row:
                    conn.rollback()
                    return None

                cur.execute(
                    """
                    UPDATE news_queue
                    SET status        = 'processing',
                        processing_at = NOW(),
                        last_updated  = NOW()
                    WHERE id = %s
                    """,
                    (row["id"],),
                )
            conn.commit()
            return dict(row)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def fail_stale_processing(self, max_minutes: int = 10) -> int:
        """
        Reset articles stuck in 'processing' back to 'pending'.

        FIX: returns the real DB rowcount instead of always returning 0.
             Requires db_execute(..., return_rowcount=True) support in DB/db.py.
        """
        recovered = db_execute(
            """
            UPDATE news_queue
            SET status = 'pending', last_updated = NOW()
            WHERE status = 'processing'
              AND processing_at < NOW() - INTERVAL '%s minutes'
            """,
            (max_minutes,),
            return_rowcount=True,
        ) or 0

        if recovered:
            logger.warning(f"♻️  Recovered {recovered} stale 'processing' rows")

        return recovered
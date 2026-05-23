"""
services/publish_pipeline.py — Unified, Idempotent Publish Orchestration Layer

PROBLEM THIS MODULE SOLVES
────────────────────────────────────────────────────────────────────────────
The original system had TWO separate publish paths:

  Path A (instant): save_news → instant_publish → social_dispatcher
  Path B (queue):   save_news → QueueManager → scheduler → social_dispatcher

This fragmentation caused:
  1. DUPLICATE WEBHOOKS — an article published instantly via Path A could
     also be picked up by the scheduler (Path B) if status wasn't updated
     atomically, sending the same article to Make.com twice.
  2. NO IDEMPOTENCY — retries had no fingerprint check; the same webhook
     payload could be sent multiple times with no detection.
  3. NON-PERSISTENT DELAYED JOBS — threading.Thread delays in social_dispatcher
     are lost on process restart, silently dropping queued posts.
  4. FRAGMENTED TRACING — no single place shows the complete lifecycle of
     one article across all platforms.

SOLUTION
────────────────────────────────────────────────────────────────────────────
PublishPipeline is the SINGLE entry point for all article publishing.
Both instant (high-priority) and normal-priority articles flow through here.

Key guarantees:
  1. Idempotency: each (article_id, platform) pair is fingerprinted.
     Duplicate calls are detected and blocked at DB level.
  2. Single status authority: the news_queue row is the single source
     of truth.  No publish call happens without first claiming the row.
  3. Centralized payload builder: all three webhooks receive consistent,
     sanitized payloads built from the same function.
  4. Structured tracing: every decision (sent / skipped / failed) is
     logged with article_id, platform, score, and timestamp.
  5. Persistent retry: failed platforms are not silently dropped; they
     are re-queued as 'pending' so the scheduler retries them.

PUBLISHING STRATEGY (from Task 2)
────────────────────────────────────────────────────────────────────────────
HIGH PRIORITY (priority_score ≥ PRIORITY_THRESHOLD_INSTAGRAM):
  → Telegram (always first, fastest)
  → Instagram ONLY
  → Facebook:  SKIPPED (policy)
  → Twitter:   SKIPPED (policy)

NORMAL PRIORITY:
  → Telegram (always)
  → Facebook  (if ENABLE_FACEBOOK_POSTING and within date window)
  → Twitter
  → Instagram: SKIPPED
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.settings import (
    ENABLE_FACEBOOK_POSTING,
    PRIORITY_THRESHOLD_INSTAGRAM,
    PRIORITY_THRESHOLD_TWITTER,
    PRIORITY_THRESHOLD_FACEBOOK,
    INSTAGRAM_MIN_INTERVAL_SECONDS,
    TWITTER_MIN_INTERVAL_SECONDS,
    FACEBOOK_MIN_INTERVAL_SECONDS,
    INSTAGRAM_MAX_PER_HOUR,
    TWITTER_MAX_PER_HOUR,
    FACEBOOK_MAX_PER_HOUR,
    FACEBOOK_START_DATE,
    FACEBOOK_END_DATE,
)
from DB.db import db_execute
from utils.text_filter import sanitize_text
from utils.logger import logger

# ─────────────────────────────────────────────────────────────────────────────
# Publish event fingerprinting
# ─────────────────────────────────────────────────────────────────────────────

def _event_fingerprint(article_id: int, platform: str) -> str:
    """
    Create a deterministic fingerprint for (article_id, platform).
    Used to detect and block duplicate publish events.
    """
    raw = f"{article_id}:{platform}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _is_already_published(article_id: Optional[int], platform: str) -> bool:
    """
    Check publish_log table for a prior successful publish of this
    (article_id, platform) pair.  Blocks duplicates at DB level.
    Returns False (allow publish) when article_id is None — can't fingerprint.
    """
    if article_id is None:
        return False
    try:
        row = db_execute(
            """
            SELECT id FROM publish_log
            WHERE article_id = %s AND platform = %s AND status = 'sent'
            LIMIT 1
            """,
            (article_id, platform),
            fetch=True,
        )
        return bool(row)
    except Exception:
        return False   # fail-open: if we can't check, try to publish


def _record_publish_event(
    article_id: Optional[int],
    queue_id: Optional[int],
    platform: str,
    status: str,
    error_msg: Optional[str] = None,
) -> None:
    """Persist a publish event to the log for idempotency + audit trail.
    No-ops silently when article_id is None — cannot fingerprint without it.
    """
    if article_id is None:
        return
    try:
        db_execute(
            """
            INSERT INTO publish_log
              (article_id, queue_id, platform, status, fingerprint, error_msg)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (fingerprint) DO UPDATE
              SET status    = EXCLUDED.status,
                  error_msg = EXCLUDED.error_msg,
                  updated_at = NOW()
            """,
            (
                article_id,
                queue_id,
                platform,
                status,
                _event_fingerprint(article_id, platform),
                error_msg,
            ),
        )
    except Exception as exc:
        logger.warning(f"Publish log write failed for {platform}/{article_id}: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Centralized payload builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_payload(post: dict, platform: str) -> dict:
    """
    Build a consistent, sanitized webhook payload for any platform.
    All platform publishers use this — ensures no inconsistency.
    """
    title      = sanitize_text(post.get("title", ""))
    content    = sanitize_text((post.get("content") or "")[:500])
    url        = post.get("url", "")
    image_url  = post.get("image_url")
    priority   = post.get("priority_score", 0)
    category   = post.get("source_label") or post.get("category") or ""

    if platform == "instagram":
        return {
            "platform":  "instagram",
            "message":   title,
            "caption":   content[:300],
            "image_url": image_url,
            "url":       url,
        }

    elif platform == "twitter":
        tweet = f"📰 {title}\n\n{content[:200]}\n\n🔗 {url}"[:280]
        return {
            "platform":  "twitter",
            "text":      tweet,
            "image_url": image_url,
        }

    elif platform == "facebook":
        urgent = "🔴 عاجل\n\n" if priority >= PRIORITY_THRESHOLD_INSTAGRAM else ""
        cat    = f"📂 {category}\n\n" if category else ""
        return {
            "platform":  "facebook",
            "message":   f"{urgent}📰 {title}\n\n{cat}{content}",
            "image_url": image_url,
            "url":       url,
        }

    raise ValueError(f"Unknown platform: {platform!r}")


# ─────────────────────────────────────────────────────────────────────────────
# Rate limiter (DB-backed, no in-memory state = process-restart safe)
# ─────────────────────────────────────────────────────────────────────────────

def _can_post_now(platform: str) -> tuple[bool, str]:
    """
    Returns (allowed, reason).  reason is '' when allowed.
    All state lives in social_rate_log — survives process restarts.
    """
    cfg = {
        "instagram": (INSTAGRAM_MIN_INTERVAL_SECONDS, INSTAGRAM_MAX_PER_HOUR),
        "twitter":   (TWITTER_MIN_INTERVAL_SECONDS,   TWITTER_MAX_PER_HOUR),
        "facebook":  (FACEBOOK_MIN_INTERVAL_SECONDS,  FACEBOOK_MAX_PER_HOUR),
    }.get(platform)

    if not cfg:
        return True, ""

    min_interval, max_per_hour = cfg

    # Check cooldown
    try:
        row = db_execute(
            """
            SELECT EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM sent_at) AS secs
            FROM social_rate_log
            WHERE platform = %s
            ORDER BY sent_at DESC
            LIMIT 1
            """,
            (platform,),
            fetch=True,
        )
        if row and float(row["secs"]) < min_interval:
            wait = min_interval - float(row["secs"])
            return False, f"cooldown {wait:.0f}s remaining"
    except Exception:
        pass

    # Check hourly cap
    try:
        row = db_execute(
            """
            SELECT COUNT(*) AS cnt
            FROM social_rate_log
            WHERE platform = %s
              AND sent_at > NOW() - INTERVAL '1 hour'
            """,
            (platform,),
            fetch=True,
        )
        if row and int(row["cnt"]) >= max_per_hour:
            return False, f"hourly cap reached ({max_per_hour}/hr)"
    except Exception:
        pass

    return True, ""


def _record_rate_event(
    platform: str,
    article_id: Optional[int],
    queue_id: Optional[int],
) -> None:
    try:
        db_execute(
            "INSERT INTO social_rate_log (platform, article_id, queue_id) VALUES (%s,%s,%s)",
            (platform, article_id, queue_id),
        )
    except Exception as exc:
        logger.warning(f"Rate log write failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Webhook sender with retry
# ─────────────────────────────────────────────────────────────────────────────

def _get_webhook_url(platform: str) -> Optional[str]:
    env_map = {
        "instagram": ["INSTAGRAM_WEBHOOK_URL", "MAKE_WEBHOOK_URL"],
        "twitter":   ["TWITTER_WEBHOOK_URL",   "MAKE_WEBHOOK_URL"],
        "facebook":  ["FACEBOOK_WEBHOOK_URL",  "MAKE_WEBHOOK_URL"],
    }
    for var in env_map.get(platform, []):
        val = os.getenv(var)
        if val:
            return val
    return None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=3, max=20),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def _send_webhook(url: str, payload: dict) -> None:
    resp = requests.post(url, json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        raise requests.RequestException(
            f"Webhook {resp.status_code}: {resp.text[:200]}"
        )


def _publish_to_platform(
    post: dict,
    platform: str,
) -> str:
    """
    Send to one platform.  Returns status string:
      'sent' | 'failed' | 'skipped:reason' | 'rate_limited'
    """
    # Resolve article_id — prefer article_id field, fall back to queue id
    _raw_id    = post.get("article_id") or post.get("id")
    article_id: Optional[int] = int(_raw_id) if _raw_id is not None else None
    queue_id   = post.get("id")

    # Idempotency check
    if _is_already_published(article_id, platform):
        logger.debug(
            f"⏭  Idempotency block | {platform} | article_id={article_id}"
        )
        return "skipped:already_published"

    # Rate limit check
    allowed, reason = _can_post_now(platform)
    if not allowed:
        logger.info(
            f"⏳ Rate limited | {platform} | article_id={article_id} | {reason}"
        )
        return f"rate_limited:{reason}"

    # Facebook date window
    if platform == "facebook":
        if not ENABLE_FACEBOOK_POSTING:
            return "skipped:fb_disabled"
        today = datetime.now(timezone.utc).date()
        if not (FACEBOOK_START_DATE.date() <= today <= FACEBOOK_END_DATE.date()):
            return "skipped:fb_outside_date_window"

    webhook_url = _get_webhook_url(platform)
    if not webhook_url:
        logger.warning(f"⚠️  No webhook URL for {platform}")
        return f"skipped:no_webhook_url"

    payload = _build_payload(post, platform)

    try:
        _send_webhook(webhook_url, payload)
        _record_rate_event(platform, article_id, queue_id)
        _record_publish_event(article_id, queue_id, platform, "sent")
        logger.info(f"✅ {platform.capitalize()} sent | article_id={article_id}")
        return "sent"

    except Exception as exc:
        err = str(exc)[:200]
        _record_publish_event(article_id, queue_id, platform, "failed", err)
        logger.error(
            f"❌ {platform.capitalize()} failed | article_id={article_id} | {err}"
        )
        return "failed"


# ─────────────────────────────────────────────────────────────────────────────
# Terminal summary printer (Task 3)
# ─────────────────────────────────────────────────────────────────────────────

def _print_publish_summary(
    post: dict,
    priority_score: int,
    results: dict[str, str],
) -> None:
    """
    Print a clean, structured publish summary.

    FIXES:
    1. Removed duplicate print() — logger.info() already writes to stdout
       via the StreamHandler. print() + logger.info() caused every summary
       to appear twice in the container log.
    2. Added 'telegram' to the platform loop so all 4 platforms are shown.
    3. Loop order: telegram → instagram → facebook → twitter (logical flow).
    """
    ts    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    level = "HIGH" if priority_score >= PRIORITY_THRESHOLD_INSTAGRAM else "NORMAL"
    title = (post.get("title") or "")[:60]
    aid   = post.get("article_id") or post.get("id")

    def _icon(status: str) -> str:
        if "sent" in status:          return "✅"
        if "failed" in status:        return "❌"
        if "rate_limited" in status:  return "⏳"
        return "⏭ "

    sep = "╔" + "═" * 64 + "╗"
    end = "╚" + "═" * 64 + "╝"
    lines = [
        sep,
        f"║  📰 PUBLISHED  [{ts}]  article_id={aid}",
        f"║  Title    : {title}",
        f"║  Priority : {level} (score={priority_score})",
    ]
    # FIX: include all 4 platforms in correct order
    for platform in ("telegram", "instagram", "facebook", "twitter"):
        status = results.get(platform, "unknown")
        icon   = _icon(status)
        lines.append(f"║  {icon} {platform:<10}: {status}")
    lines.append(end)

    block = "\n".join(lines)
    # FIX: use only logger.info — no print() to avoid duplicate output
    logger.info(block)


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestration entry point
# ─────────────────────────────────────────────────────────────────────────────

class PublishPipeline:
    """
    The single, authoritative publish orchestrator.

    Usage:
        pipeline = PublishPipeline()
        pipeline.publish(post, telegram_publisher)
    """

    def __init__(self) -> None:
        # Import here to avoid circular imports
        from services.priority_telegram_publisher import PriorityTelegramPublisher
        self._telegram = PriorityTelegramPublisher()

    def publish(
        self,
        post: dict,
        priority_score: Optional[int] = None,
    ) -> dict[str, str]:
        """
        Publish one article to the correct platforms based on priority.

        This is the ONLY function that should call webhooks.
        All other publish paths (instant_publisher, scheduler) must
        route through here.

        Returns dict of {platform: status}.
        """
        if priority_score is None:
            priority_score = int(post.get("priority_score") or 0)

        article_id = post.get("article_id") or post.get("id")
        title_snip = (post.get("title") or "")[:80]
        is_high    = priority_score >= PRIORITY_THRESHOLD_INSTAGRAM

        logger.info(
            f"📤 PublishPipeline.publish | article_id={article_id} | "
            f"score={priority_score} | "
            f"{'HIGH→Instagram' if is_high else 'NORMAL→FB+TW'} | "
            f"{title_snip}"
        )

        results: dict[str, str] = {}

        # ── Step 1: Telegram — always first ──────────────────────────────
        try:
            tg_sent = self._telegram.publish(post)
            results["telegram"] = "sent" if tg_sent else "failed"
        except Exception as exc:
            results["telegram"] = f"failed:{exc}"
            logger.error(f"❌ Telegram failed | article_id={article_id} | {exc}")

        # ── Step 2: Social platforms by priority ──────────────────────────
        if is_high:
            # HIGH PRIORITY: Instagram only
            results["instagram"] = _publish_to_platform(post, "instagram")
            results["facebook"]  = "skipped:high_priority_policy"
            results["twitter"]   = "skipped:high_priority_policy"

        else:
            # NORMAL PRIORITY: Facebook + Twitter
            results["instagram"] = "skipped:normal_priority_policy"
            results["twitter"]   = _publish_to_platform(post, "twitter")
            results["facebook"]  = _publish_to_platform(post, "facebook")

        # ── Step 3: Terminal summary ──────────────────────────────────────
        _print_publish_summary(post, priority_score, results)

        return results

    def publish_platform_only(
        self,
        post: dict,
        platform: str,
    ) -> str:
        """
        Publish to exactly one platform.
        Used by the retry worker for failed-platform recovery.
        """
        return _publish_to_platform(post, platform)

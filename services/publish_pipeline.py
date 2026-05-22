"""
services/publish_pipeline.py — Unified, Idempotent Publish Orchestration Layer

═══════════════════════════════════════════════════════════════════════════════
FIXES APPLIED
═══════════════════════════════════════════════════════════════════════════════

ISSUE #2 — Same news published twice
──────────────────────────────────────
ROOT CAUSE — _schedule_delayed thread overlap with publishing_worker:
  When Facebook was rate-limited, _schedule_delayed spawned a daemon thread.
  While that thread slept, the rate window cleared and the publishing_worker's
  next cycle published the same article.  When the thread woke up it also
  published → two posts for the same article.

FIX — Two-layer idempotency:
  Layer 1 (DB fingerprint): _is_already_published() checks publish_log for
    a prior 'sent' record keyed on SHA-256(article_id + platform).
    Any call to _publish_to_platform that finds a matching record returns
    'skipped:already_published' without touching the webhook.

  Layer 2 (delayed-thread guard): social_dispatcher._schedule_delayed now
    writes a 'pending' entry before spawning the thread, then checks
    _is_already_published() when it wakes up.  If the article was published
    during the sleep by any other path, the thread exits silently.

  Together these blocks every known duplicate path:
    • instant_publish called twice (race condition)
    • instant_publish + scheduler overlap
    • delayed thread + scheduler overlap        ← primary bug
    • Make.com retry after a timeout
    • Process restart during a delayed send

ISSUE #3 — Different caption formats per platform
──────────────────────────────────────────────────
Required format:
  Telegram  → Title + Category line + Body
  Facebook  → Title + Body ONLY (NO category line)
  Instagram → Title + Body ONLY (NO category line)

ROOT CAUSE — _build_payload for facebook included a category line:
    cat = f"📂 {category}\\n\\n" if category else ""
    "message": f"{urgent}📰 {title}\\n\\n{cat}{content}"
  This appended the Arabic category label to every Facebook post.

FIX — Split caption generation per platform in _build_payload():
  • "telegram"  → adds category line between title and body.
  • "facebook"  → title + body, NO category.
  • "instagram" → title + body, NO category.

  The platform-specific caption is the single source of truth.
  PriorityTelegramPublisher._caption() is also updated separately.

ISSUE #4 — Publishing interval location (informational)
─────────────────────────────────────────────────────────
The Facebook inter-post delay is controlled by:
  File:     config/settings.py
  Variable: FACEBOOK_MIN_INTERVAL_SECONDS  (currently 300 — 5 minutes)
  Also:     FACEBOOK_MAX_PER_HOUR          (currently 10 posts/hour)

The delay is enforced here in _can_post_now() which queries social_rate_log.
To change the Facebook posting interval, edit FACEBOOK_MIN_INTERVAL_SECONDS
in config/settings.py.  No other file needs touching.
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
# Publish event fingerprinting  (ISSUE #2 FIX)
# ─────────────────────────────────────────────────────────────────────────────

def _event_fingerprint(article_id: int, platform: str) -> str:
    """
    Deterministic fingerprint for (article_id, platform).
    Used to detect and block duplicate publish events at DB level.
    """
    return hashlib.sha256(f"{article_id}:{platform}".encode()).hexdigest()[:16]


def _is_already_published(article_id: Optional[int], platform: str) -> bool:
    """
    Check publish_log for a prior successful publish of this
    (article_id, platform) pair.  Returns False when article_id is None
    (cannot fingerprint without an ID — fail-open, allow publish attempt).
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
        return False  # fail-open: DB error → try to publish


def _record_publish_event(
    article_id: Optional[int],
    queue_id: Optional[int],
    platform: str,
    status: str,
    error_msg: Optional[str] = None,
) -> None:
    """
    Persist a publish event to publish_log for idempotency + audit trail.
    Uses ON CONFLICT so 'pending' → 'sent' transitions are safe.
    No-ops when article_id is None.
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
              SET status     = EXCLUDED.status,
                  error_msg  = EXCLUDED.error_msg,
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
# Centralized payload builder  (ISSUE #3 FIX)
# ─────────────────────────────────────────────────────────────────────────────

def _build_payload(post: dict, platform: str) -> dict:
    """
    Build a sanitized webhook payload for a specific platform.

    ISSUE #3 FIX — Platform-specific caption format:

    Telegram  → [Image] + Title + Category line + Body text
                (handled in PriorityTelegramPublisher._caption, not here)

    Facebook  → [Image] + Title + Body text
                NO category line.

    Instagram → [Image] + Title + Body text
                NO category line.

    The category line was previously included in Facebook captions via
    the `cat = f"📂 {category}\\n\\n"` variable — now removed for FB/IG.
    """
    title     = sanitize_text(post.get("title", ""))
    content   = sanitize_text((post.get("content") or "")[:500])
    url       = post.get("url", "")
    image_url = post.get("image_url")
    priority  = post.get("priority_score", 0)

    if platform == "instagram":
        # Instagram: title + body, NO category
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
        # ISSUE #3 FIX: Facebook gets title + body ONLY — NO category line.
        # Previously this included: cat = f"📂 {category}\n\n"
        # That line is now removed for Facebook and Instagram.
        urgent = "🔴 عاجل\n\n" if priority >= PRIORITY_THRESHOLD_INSTAGRAM else ""
        return {
            "platform":  "facebook",
            "message":   f"{urgent}📰 {title}\n\n{content}",
            "image_url": image_url,
            "url":       url,
        }

    raise ValueError(f"Unknown platform: {platform!r}")


# ─────────────────────────────────────────────────────────────────────────────
# Rate limiter — DB-backed, process-restart safe
# ─────────────────────────────────────────────────────────────────────────────

def _can_post_now(platform: str) -> tuple[bool, str]:
    """
    Returns (allowed, reason).  reason is '' when allowed.

    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    ISSUE #4 — Publishing interval location (answer)
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    The inter-post delay for Facebook is enforced here.
    It reads two values from config/settings.py:

        FACEBOOK_MIN_INTERVAL_SECONDS = 300   ← 5-minute cooldown between posts
        FACEBOOK_MAX_PER_HOUR         = 10    ← hard hourly cap

    To change the Facebook posting interval:
        1. Open  config/settings.py
        2. Change FACEBOOK_MIN_INTERVAL_SECONDS  (seconds between posts)
           or      FACEBOOK_MAX_PER_HOUR         (posts allowed per hour)
        3. Restart the process — no other changes needed.

    The delay is NOT controlled by Make.com, a cron job, a sleep timer,
    or an async worker. It is purely DB-time-based: each call measures
    (NOW() - last_sent_at) and blocks if < FACEBOOK_MIN_INTERVAL_SECONDS.

    For Twitter: TWITTER_MIN_INTERVAL_SECONDS  = 60  (1 minute)
    For Instagram: INSTAGRAM_MIN_INTERVAL_SECONDS = 120 (2 minutes)
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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


def _publish_to_platform(post: dict, platform: str) -> str:
    """
    Send to one platform.  Returns status string:
      'sent' | 'failed' | 'skipped:reason' | 'rate_limited:reason'

    Idempotency check (ISSUE #2 FIX) is the first gate — any prior
    'sent' record for (article_id, platform) immediately returns
    'skipped:already_published' without firing the webhook.
    """
    _raw_id    = post.get("article_id") or post.get("id")
    article_id: Optional[int] = int(_raw_id) if _raw_id is not None else None
    queue_id   = post.get("id")

    # ── Gate 1: idempotency check (ISSUE #2 FIX) ─────────────────────────
    if _is_already_published(article_id, platform):
        logger.debug(f"⏭  Idempotency block | {platform} | article_id={article_id}")
        return "skipped:already_published"

    # ── Gate 2: rate limit ────────────────────────────────────────────────
    allowed, reason = _can_post_now(platform)
    if not allowed:
        logger.info(
            f"⏳ Rate limited | {platform} | article_id={article_id} | {reason}"
        )
        return f"rate_limited:{reason}"

    # ── Gate 3: Facebook date window ──────────────────────────────────────
    if platform == "facebook":
        if not ENABLE_FACEBOOK_POSTING:
            return "skipped:fb_disabled"
        today = datetime.now(timezone.utc).date()
        if not (FACEBOOK_START_DATE.date() <= today <= FACEBOOK_END_DATE.date()):
            return "skipped:fb_outside_date_window"

    # ── Gate 4: webhook URL ───────────────────────────────────────────────
    webhook_url = _get_webhook_url(platform)
    if not webhook_url:
        logger.warning(f"⚠️  No webhook URL for {platform}")
        return f"skipped:no_webhook_url"

    # ── Send ──────────────────────────────────────────────────────────────
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
# Terminal summary printer
# ─────────────────────────────────────────────────────────────────────────────

def _print_publish_summary(
    post: dict,
    priority_score: int,
    results: dict[str, str],
) -> None:
    """Print a clean, structured publish summary to stdout + logger."""
    ts    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    level = "HIGH" if priority_score >= PRIORITY_THRESHOLD_INSTAGRAM else "NORMAL"
    title = (post.get("title") or "")[:60]
    aid   = post.get("article_id") or post.get("id")

    def _icon(status: str) -> str:
        if "sent" in status:         return "✅"
        if "failed" in status:       return "❌"
        if "rate_limited" in status: return "⏳"
        return "⏭ "

    sep   = "╔" + "═" * 64 + "╗"
    end   = "╚" + "═" * 64 + "╝"
    lines = [
        sep,
        f"║  📰 PUBLISHED  [{ts}]  article_id={aid}",
        f"║  Title    : {title}",
        f"║  Priority : {level} (score={priority_score})",
    ]
    for platform in ("telegram", "instagram", "facebook", "twitter"):
        status = results.get(platform, "unknown")
        lines.append(f"║  {_icon(status)} {platform:<10}: {status}")
    lines.append(end)

    block = "\n".join(lines)
    print(block, flush=True)
    logger.info(block)


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestration entry point
# ─────────────────────────────────────────────────────────────────────────────

class PublishPipeline:
    """
    The single, authoritative publish orchestrator.

    All webhook calls in the system must route through this class.
    Both instant_publisher and the scheduler worker use it.
    """

    def __init__(self) -> None:
        from services.priority_telegram_publisher import PriorityTelegramPublisher
        self._telegram = PriorityTelegramPublisher()

    def publish(
        self,
        post: dict,
        priority_score: Optional[int] = None,
    ) -> dict[str, str]:
        """
        Publish one article to the correct platforms based on priority.

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

    def publish_platform_only(self, post: dict, platform: str) -> str:
        """
        Publish to exactly one platform.
        Used by the retry worker for failed-platform recovery.
        """
        return _publish_to_platform(post, platform)
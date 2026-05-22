"""
Smart social-media routing engine.

Strategy
────────
Priority score thresholds (from settings.py):
  ≥ 9  → INSTAGRAM immediately  +  Twitter queue  +  Facebook within 5 min
  ≥ 1  → Twitter queue  +  Facebook if important enough
  = 0  → Twitter queue only (low volume content)

Burst handling:
  If BURST_MAX_INSTANT posts have already been sent to a platform within
  BURST_WINDOW_SECONDS, additional posts are deferred to that platform's
  normal queue (not dropped — just slightly delayed).

Rate enforcement:
  Each platform has a minimum inter-post interval and an hourly cap,
  tracked in the social_rate_log table.

Facebook 5-minute rule:
  High-priority articles (score >= PRIORITY_THRESHOLD_FACEBOOK) must appear
  on Facebook within 5 minutes of website publication.
  The publisher checks the age of the article and fast-tracks if needed.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

from DB.db import db_execute
from config.settings import (
    PRIORITY_THRESHOLD_INSTAGRAM,
    PRIORITY_THRESHOLD_TWITTER,
    PRIORITY_THRESHOLD_FACEBOOK,
    INSTAGRAM_MIN_INTERVAL_SECONDS,
    TWITTER_MIN_INTERVAL_SECONDS,
    FACEBOOK_MIN_INTERVAL_SECONDS,
    FACEBOOK_MAX_PER_HOUR,
    TWITTER_MAX_PER_HOUR,
    INSTAGRAM_MAX_PER_HOUR,
    BURST_WINDOW_SECONDS,
    BURST_MAX_INSTANT,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Rate helpers
# ─────────────────────────────────────────────────────────────────────────────

def _seconds_since_last(platform: str) -> float:
    """Seconds elapsed since the most recent post on this platform."""
    row = db_execute(
        "SELECT sent_at FROM social_rate_log WHERE platform=%s ORDER BY sent_at DESC LIMIT 1",
        (platform,), fetch=True,
    )
    if not row:
        return float("inf")
    sent_at = row["sent_at"]
    if sent_at.tzinfo is None:
        sent_at = sent_at.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - sent_at).total_seconds()


def _posts_in_last_hour(platform: str) -> int:
    row = db_execute(
        """
        SELECT COUNT(*) AS cnt FROM social_rate_log
        WHERE platform = %s AND sent_at > NOW() - INTERVAL '1 hour'
        """,
        (platform,), fetch=True,
    )
    return int(row["cnt"]) if row else 0


def _posts_in_burst_window(platform: str) -> int:
    row = db_execute(
        """
        SELECT COUNT(*) AS cnt FROM social_rate_log
        WHERE platform = %s
          AND sent_at > NOW() - INTERVAL '%s seconds'
        """,
        (platform, BURST_WINDOW_SECONDS), fetch=True,
    )
    return int(row["cnt"]) if row else 0


def _can_post(platform: str) -> tuple[bool, str]:
    """
    Returns (allowed: bool, reason: str).
    reason is empty string when allowed.
    """
    min_interval = {
        "instagram": INSTAGRAM_MIN_INTERVAL_SECONDS,
        "twitter":   TWITTER_MIN_INTERVAL_SECONDS,
        "facebook":  FACEBOOK_MIN_INTERVAL_SECONDS,
    }.get(platform, 60)

    max_per_hour = {
        "instagram": INSTAGRAM_MAX_PER_HOUR,
        "twitter":   TWITTER_MAX_PER_HOUR,
        "facebook":  FACEBOOK_MAX_PER_HOUR,
    }.get(platform, 10)

    since_last = _seconds_since_last(platform)
    if since_last < min_interval:
        return False, f"cooldown ({since_last:.0f}s < {min_interval}s)"

    per_hour = _posts_in_last_hour(platform)
    if per_hour >= max_per_hour:
        return False, f"hourly cap ({per_hour}/{max_per_hour})"

    burst = _posts_in_burst_window(platform)
    if burst >= BURST_MAX_INSTANT:
        return False, f"burst guard ({burst}/{BURST_MAX_INSTANT} in {BURST_WINDOW_SECONDS}s)"

    return True, ""


def _log_post(platform: str, article_id: int, queue_id: int) -> None:
    db_execute(
        "INSERT INTO social_rate_log (platform, article_id, queue_id) VALUES (%s,%s,%s)",
        (platform, article_id, queue_id),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public routing interface
# ─────────────────────────────────────────────────────────────────────────────

def route_to_platforms(
    post: dict,
    *,
    instagram_publisher=None,
    twitter_publisher=None,
    facebook_publisher=None,
) -> dict[str, str]:
    """
    Decide which platforms to post to right now vs defer, then send.

    Returns dict: { platform: 'sent'|'deferred'|'skipped'|'failed' }
    """
    priority_score = post.get("priority_score", 0)
    article_id     = post.get("article_id", 0)
    queue_id       = post.get("id", 0)
    title          = post.get("title", "")[:60]

    # Article age in seconds
    created_at = post.get("created_at")
    age_seconds = (time.time() - float(created_at)) if created_at else 9999

    statuses: dict[str, str] = {}

    # ── Instagram: only high priority ────────────────────────────────────
    if priority_score >= PRIORITY_THRESHOLD_INSTAGRAM and instagram_publisher:
        allowed, reason = _can_post("instagram")
        if allowed:
            try:
                instagram_publisher.publish(post)
                _log_post("instagram", article_id, queue_id)
                statuses["instagram"] = "sent"
                logger.info(f"📸 Instagram sent | priority={priority_score} | {title}")
            except Exception as exc:
                statuses["instagram"] = "failed"
                logger.error(f"❌ Instagram failed: {exc}")
        else:
            statuses["instagram"] = "deferred"
            logger.info(f"📸 Instagram deferred ({reason}) | {title}")
    else:
        statuses["instagram"] = "skipped"

    # ── Twitter: medium + high priority ───────────────────────────────────
    if priority_score >= PRIORITY_THRESHOLD_TWITTER and twitter_publisher:
        allowed, reason = _can_post("twitter")
        if allowed:
            try:
                twitter_publisher.publish(post)
                _log_post("twitter", article_id, queue_id)
                statuses["twitter"] = "sent"
                logger.info(f"🐦 Twitter sent | priority={priority_score} | {title}")
            except Exception as exc:
                statuses["twitter"] = "failed"
                logger.error(f"❌ Twitter failed: {exc}")
        else:
            statuses["twitter"] = "deferred"
            logger.info(f"🐦 Twitter deferred ({reason}) | {title}")
    else:
        statuses["twitter"] = "skipped"

    # ── Facebook: high priority + within 5 min window ────────────────────
    send_fb = (
        priority_score >= PRIORITY_THRESHOLD_FACEBOOK
        or age_seconds <= 300  # force-send if article ≤ 5 min old + high priority
    ) and priority_score >= PRIORITY_THRESHOLD_FACEBOOK

    if send_fb and facebook_publisher:
        allowed, reason = _can_post("facebook")
        if allowed:
            try:
                facebook_publisher.publish(post)
                _log_post("facebook", article_id, queue_id)
                statuses["facebook"] = "sent"
                logger.info(f"📘 Facebook sent | age={age_seconds:.0f}s | {title}")
            except Exception as exc:
                statuses["facebook"] = "failed"
                logger.error(f"❌ Facebook failed: {exc}")
        else:
            statuses["facebook"] = "deferred"
            logger.info(f"📘 Facebook deferred ({reason}) | {title}")
    else:
        statuses["facebook"] = "skipped"

    return statuses

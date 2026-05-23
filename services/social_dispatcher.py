"""
services/social_dispatcher.py — Intelligent multi-platform dispatcher.

NEW PUBLISHING STRATEGY (Task 2)
──────────────────────────────────────────────────────────────────────────────
HIGH PRIORITY (priority_score >= PRIORITY_THRESHOLD_INSTAGRAM):
  → Instagram ONLY
  → Facebook: SKIPPED  (policy: high-priority news goes to Instagram only)
  → Twitter:  SKIPPED  (policy: high-priority news goes to Instagram only)

NORMAL PRIORITY (priority_score < PRIORITY_THRESHOLD_INSTAGRAM):
  → Facebook + Twitter  (existing queue logic)
  → Instagram: SKIPPED

RATIONALE:
  High-priority breaking news gets maximum visual impact on Instagram.
  Facebook and Twitter handle the regular news flow.
  This prevents the same article appearing on all three platforms at once,
  which reduces audience fatigue and keeps each platform's feed distinct.

BURST PROTECTION:
  If N high-priority articles arrive within BURST_WINDOW_SECONDS,
  the 3rd+ articles are staggered automatically to respect rate limits.

RATE LIMITING:
  All decisions query social_rate_log to respect per-platform cooldowns.

TERMINAL NOTIFICATIONS (Task 3):
  Every publish decision prints a structured, human-readable summary to
  the terminal (via logger) showing:
    - News title
    - Priority level
    - Which platforms were published to / skipped / failed
    - Timestamp
    - Reason for any skipped platforms
"""
from __future__ import annotations

import logging
import os
import time
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from config.settings import (
    ENABLE_FACEBOOK_POSTING,
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
    FACEBOOK_START_DATE,
    FACEBOOK_END_DATE,
)
from DB.db import db_execute
from utils.text_filter import sanitize_text

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Terminal notification helper  (Task 3)
# ─────────────────────────────────────────────────────────────────────────────

def _print_publish_summary(
    title: str,
    priority_score: int,
    results: dict[str, str],
    queue_id,
) -> None:
    """
    Print a clear, structured publish summary to the terminal.

    Example output:
    ╔══════════════════════════════════════════════════════════════╗
    ║  📰 NEWS PUBLISHED  [2026-05-21 14:33:07 UTC]  id=42
    ║  Title    : حاكم الشارقة يصدر مرسوما أميريا…
    ║  Priority : HIGH (score=18)
    ║  ✅ Instagram : sent
    ║  ⏭  Facebook  : skipped — high-priority policy (Instagram only)
    ║  ⏭  Twitter   : skipped — high-priority policy (Instagram only)
    ╚══════════════════════════════════════════════════════════════╝
    """
    ts    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    level = "HIGH" if priority_score >= PRIORITY_THRESHOLD_INSTAGRAM else "NORMAL"

    def _line(platform: str, status: str) -> str:
        icons = {
            "sent":     "✅",
            "failed":   "❌",
            "skipped":  "⏭ ",
            "deferred": "⏳",
        }
        icon = next(
            (v for k, v in icons.items() if k in status.lower()),
            "ℹ️ ",
        )
        return f"║  {icon} {platform:<10}: {status}"

    sep = "╔" + "═" * 62 + "╗"
    end = "╚" + "═" * 62 + "╝"

    lines = [
        sep,
        f"║  📰 NEWS PUBLISHED  [{ts}]  id={queue_id}",
        f"║  Title    : {title[:58]}",
        f"║  Priority : {level} (score={priority_score})",
    ]
    for platform in ("telegram", "instagram", "facebook", "twitter"):
        status = results.get(platform, "unknown")
        lines.append(_line(platform.capitalize(), status))
    lines.append(end)

    # FIX: use only logger.info — print() + logger.info() caused duplicate output
    block = "\n".join(lines)
    logger.info(block)


# ─────────────────────────────────────────────────────────────────────────────
# Rate state
# ─────────────────────────────────────────────────────────────────────────────

class _RateState:
    """Thread-safe in-process rate limiter with DB-backed precision."""

    def __init__(self, platform: str, min_interval: int, max_per_hour: int):
        self.platform     = platform
        self.min_interval = min_interval
        self.max_per_hour = max_per_hour
        self._lock        = threading.Lock()
        self._burst_times: deque = deque(maxlen=BURST_MAX_INSTANT + 5)

    def _db_last_sent(self) -> Optional[float]:
        try:
            row = db_execute(
                """
                SELECT EXTRACT(EPOCH FROM sent_at) AS epoch
                FROM social_rate_log
                WHERE platform = %s
                ORDER BY sent_at DESC
                LIMIT 1
                """,
                (self.platform,),
                fetch=True,
            )
            return float(row["epoch"]) if row else None
        except Exception:
            return None

    def _db_count_last_hour(self) -> int:
        try:
            row = db_execute(
                """
                SELECT COUNT(*) AS cnt
                FROM social_rate_log
                WHERE platform = %s
                  AND sent_at > NOW() - INTERVAL '1 hour'
                """,
                (self.platform,),
                fetch=True,
            )
            return int(row["cnt"]) if row else 0
        except Exception:
            return 0

    def can_send(self) -> tuple[bool, float]:
        with self._lock:
            now       = time.time()
            last_sent = self._db_last_sent()

            if last_sent is not None:
                elapsed = now - last_sent
                if elapsed < self.min_interval:
                    return False, self.min_interval - elapsed

            hourly = self._db_count_last_hour()
            if hourly >= self.max_per_hour:
                return False, 60.0

            return True, 0.0

    def burst_count_in_window(self) -> int:
        cutoff = time.time() - BURST_WINDOW_SECONDS
        return sum(1 for t in self._burst_times if t > cutoff)

    def record_send(
        self,
        article_id: Optional[int] = None,
        queue_id: Optional[int] = None,
    ) -> None:
        with self._lock:
            self._burst_times.append(time.time())
            try:
                db_execute(
                    """
                    INSERT INTO social_rate_log (platform, article_id, queue_id)
                    VALUES (%s, %s, %s)
                    """,
                    (self.platform, article_id, queue_id),
                )
            except Exception as exc:
                logger.warning(f"Rate log write failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Platform publishers
# ─────────────────────────────────────────────────────────────────────────────

class _InstagramPublisher:

    def __init__(self):
        self.webhook_url = (
            os.getenv("INSTAGRAM_WEBHOOK_URL") or os.getenv("MAKE_WEBHOOK_URL")
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=3, max=20),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _post(self, payload: dict) -> None:
        if not self.webhook_url:
            raise RuntimeError("INSTAGRAM_WEBHOOK_URL not set")
        resp = requests.post(self.webhook_url, json=payload, timeout=30)
        if resp.status_code not in (200, 201):
            raise requests.RequestException(
                f"Instagram webhook {resp.status_code}: {resp.text[:200]}"
            )

    def publish(self, post: dict) -> bool:
        if not self.webhook_url:
            logger.warning("Instagram publish skipped — no webhook URL")
            return False
        try:
            self._post({
                "platform":  "instagram",
                "message":   sanitize_text(post["title"]),
                "caption":   sanitize_text((post.get("content") or "")[:300]),
                "image_url": post.get("image_url"),
                "url":       post.get("url"),
            })
            logger.info(f"✅ Instagram sent | id={post.get('id')}")
            return True
        except Exception as exc:
            logger.error(f"❌ Instagram failed | id={post.get('id')} | {exc}")
            return False


class _TwitterPublisher:

    def __init__(self):
        self.webhook_url = (
            os.getenv("TWITTER_WEBHOOK_URL") or os.getenv("MAKE_WEBHOOK_URL")
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=3, max=20),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _post(self, payload: dict) -> None:
        if not self.webhook_url:
            raise RuntimeError("TWITTER_WEBHOOK_URL not set")
        resp = requests.post(self.webhook_url, json=payload, timeout=30)
        if resp.status_code not in (200, 201):
            raise requests.RequestException(
                f"Twitter webhook {resp.status_code}: {resp.text[:200]}"
            )

    def publish(self, post: dict) -> bool:
        if not self.webhook_url:
            logger.warning("Twitter publish skipped — no webhook URL")
            return False
        try:
            title   = sanitize_text(post["title"])
            content = sanitize_text((post.get("content") or "")[:200])
            tweet   = f"📰 {title}\n\n{content}\n\n🔗 {post.get('url', '')}"[:280]
            self._post({
                "platform":  "twitter",
                "text":      tweet,
                "image_url": post.get("image_url"),
            })
            logger.info(f"✅ Twitter sent | id={post.get('id')}")
            return True
        except Exception as exc:
            logger.error(f"❌ Twitter failed | id={post.get('id')} | {exc}")
            return False


class _FacebookPublisher:

    def __init__(self):
        self.webhook_url = (
            os.getenv("FACEBOOK_WEBHOOK_URL") or os.getenv("MAKE_WEBHOOK_URL")
        )

    def _facebook_enabled(self) -> bool:
        if not ENABLE_FACEBOOK_POSTING:
            return False
        today = datetime.now(timezone.utc).date()
        return FACEBOOK_START_DATE.date() <= today <= FACEBOOK_END_DATE.date()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=30),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _post(self, payload: dict) -> None:
        if not self.webhook_url:
            raise RuntimeError("FACEBOOK_WEBHOOK_URL not set")
        resp = requests.post(self.webhook_url, json=payload, timeout=30)
        if resp.status_code not in (200, 201):
            raise requests.RequestException(
                f"Facebook webhook {resp.status_code}: {resp.text[:200]}"
            )

    def publish(self, post: dict) -> bool:
        if not self._facebook_enabled():
            return False
        if not self.webhook_url:
            logger.warning("Facebook publish skipped — no webhook URL")
            return False
        try:
            self._post({
                "platform":  "facebook",
                "message":   (
                    f"📰 {sanitize_text(post['title'])}\n\n"
                    f"{sanitize_text((post.get('content') or '')[:500])}"
                ),
                "image_url": post.get("image_url"),
                "url":       post.get("url"),
            })
            logger.info(f"✅ Facebook sent | id={post.get('id')}")
            return True
        except Exception as exc:
            logger.error(f"❌ Facebook failed | id={post.get('id')} | {exc}")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# Main Dispatcher
# ─────────────────────────────────────────────────────────────────────────────

class SocialDispatcher:

    def __init__(self):
        self._instagram = _InstagramPublisher()
        self._twitter   = _TwitterPublisher()
        self._facebook  = _FacebookPublisher()

        self._ig_state = _RateState(
            "instagram", INSTAGRAM_MIN_INTERVAL_SECONDS, INSTAGRAM_MAX_PER_HOUR
        )
        self._tw_state = _RateState(
            "twitter", TWITTER_MIN_INTERVAL_SECONDS, TWITTER_MAX_PER_HOUR
        )
        self._fb_state = _RateState(
            "facebook", FACEBOOK_MIN_INTERVAL_SECONDS, FACEBOOK_MAX_PER_HOUR
        )

        self._pending: deque = deque()
        self._lock           = threading.Lock()

    def enqueue(self, post: dict) -> None:
        """Add article to delayed dispatch queue (non-blocking)."""
        with self._lock:
            self._pending.append(post)

    def instant_dispatch(
        self,
        post: dict,
        priority_score: Optional[int] = None,
    ) -> dict[str, str]:
        """
        Immediately dispatch to appropriate platforms based on NEW priority rules:

        HIGH PRIORITY (score >= PRIORITY_THRESHOLD_INSTAGRAM):
          → Instagram ONLY
          → Facebook: skipped (policy)
          → Twitter:  skipped (policy)

        NORMAL PRIORITY (score < PRIORITY_THRESHOLD_INSTAGRAM):
          → Facebook + Twitter
          → Instagram: skipped

        priority_score is optional — if not passed it reads from post automatically.
        Returns dict of {platform: status}.
        """
        if priority_score is None:
            priority_score = int(post.get("priority_score") or 0)

        results: dict[str, str] = {}
        aid   = post.get("article_id") or post.get("id")
        qid   = post.get("id")
        title = post.get("title", "")[:80]

        is_high_priority = priority_score >= PRIORITY_THRESHOLD_INSTAGRAM

        if is_high_priority:
            # ── HIGH PRIORITY: Instagram ONLY ─────────────────────────────
            burst         = self._ig_state.burst_count_in_window()
            allowed, wait = self._ig_state.can_send()

            if allowed and burst < BURST_MAX_INSTANT:
                if self._instagram.publish(post):
                    self._ig_state.record_send(aid, qid)
                    results["instagram"] = "sent"
                else:
                    results["instagram"] = "failed"
            else:
                delay = max(
                    wait,
                    INSTAGRAM_MIN_INTERVAL_SECONDS * (burst - BURST_MAX_INSTANT + 1),
                )
                self._schedule_delayed(post, "instagram", delay)
                results["instagram"] = f"queued:{delay:.0f}s"
                logger.info(
                    f"⏳ Instagram burst — queued +{delay:.0f}s | id={qid}"
                )

            # Facebook and Twitter explicitly skipped for high-priority
            results["facebook"] = "skipped — high-priority policy (Instagram only)"
            results["twitter"]  = "skipped — high-priority policy (Instagram only)"

        else:
            # ── NORMAL PRIORITY: Facebook + Twitter ───────────────────────
            results["instagram"] = "skipped — normal-priority (Facebook+Twitter only)"

            # Twitter
            if priority_score >= PRIORITY_THRESHOLD_TWITTER:
                allowed, wait = self._tw_state.can_send()
                burst         = self._tw_state.burst_count_in_window()

                if allowed and burst < BURST_MAX_INSTANT * 2:
                    if self._twitter.publish(post):
                        self._tw_state.record_send(aid, qid)
                        results["twitter"] = "sent"
                    else:
                        results["twitter"] = "failed"
                else:
                    self._schedule_delayed(
                        post, "twitter", wait or TWITTER_MIN_INTERVAL_SECONDS
                    )
                    results["twitter"] = f"queued:{wait:.0f}s"
            else:
                results["twitter"] = "skipped — below twitter threshold"

            # Facebook
            if priority_score >= PRIORITY_THRESHOLD_FACEBOOK:
                allowed, wait = self._fb_state.can_send()
                if allowed:
                    if self._facebook.publish(post):
                        self._fb_state.record_send(aid, qid)
                        results["facebook"] = "sent"
                    else:
                        results["facebook"] = "failed"
                else:
                    capped_wait = min(wait, 300)
                    self._schedule_delayed(post, "facebook", capped_wait)
                    results["facebook"] = f"queued:{capped_wait:.0f}s"
                    logger.info(
                        f"⏳ Facebook delayed {capped_wait:.0f}s | id={qid}"
                    )
            else:
                results["facebook"] = "skipped — below facebook threshold"

        # ── Terminal notification ──────────────────────────────────────────
        _print_publish_summary(title, priority_score, results, qid)

        return results

    def _schedule_delayed(self, post: dict, platform: str, delay: float) -> None:
        """Spawn a background thread to publish after `delay` seconds."""
        def _send() -> None:
            time.sleep(max(0.0, delay))
            state = {
                "instagram": self._ig_state,
                "twitter":   self._tw_state,
                "facebook":  self._fb_state,
            }[platform]
            pub = {
                "instagram": self._instagram,
                "twitter":   self._twitter,
                "facebook":  self._facebook,
            }[platform]

            allowed, wait2 = state.can_send()
            if not allowed:
                time.sleep(wait2)

            if pub.publish(post):
                state.record_send(post.get("article_id"), post.get("id"))
                logger.info(f"✅ Delayed {platform} sent | id={post.get('id')}")
            else:
                logger.error(f"❌ Delayed {platform} failed | id={post.get('id')}")

        threading.Thread(
            target=_send, daemon=True, name=f"delayed-{platform}"
        ).start()

    def process_pending_queue(self) -> int:
        """
        Drain the pending queue. Called periodically by the scheduler.
        Returns number of articles dispatched.
        """
        dispatched = 0
        with self._lock:
            items = list(self._pending)
            self._pending.clear()

        for post in items:
            results = self.instant_dispatch(post)   # reads priority_score from post
            logger.info(f"📤 Queued dispatch | id={post.get('id')} | {results}")
            dispatched += 1

        return dispatched

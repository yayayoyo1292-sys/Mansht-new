"""
services/priority_telegram_publisher.py — Sends news to the Telegram channel.

═══════════════════════════════════════════════════════════════════════════════
FIXES APPLIED
═══════════════════════════════════════════════════════════════════════════════

ISSUE #3 — Different caption formats per platform
──────────────────────────────────────────────────
Required Telegram format:
  [Image]

  📰 Title

  🏷 Category

  Body text

  ━━━━━━━━━━ (separator line)

Required Facebook / Instagram format (handled in publish_pipeline.py):
  [Image]

  📰 Title

  Body text

  (NO category line at all)

ROOT CAUSE — The caption format was identical across all platforms.
  The Telegram _caption() method built the same string used for Facebook,
  without any platform-aware distinction.

FIX — _caption() now produces the Telegram-specific format:
  1. Title line           → "📰 {title}"
  2. Category line        → "🏷 {category}" — ONLY for Telegram, always present
  3. Separator            → "━━━━━━━━━━"
  4. Body text            → first 500 chars of content
  5. Link                 → "🔗 {url}"

  Facebook and Instagram captions are built separately in
  publish_pipeline._build_payload() and contain NO category line.

Other fixes (unchanged from previous refactor):
  • _TOKEN and _CHAT_ID fall back to TOKEN / CHAT_ID env vars for
    single-bot setups.
  • 4-attempt exponential retry with tenacity.
  • Caption truncated to Telegram's 1024-char limit.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from dotenv import load_dotenv
from utils.text_filter import sanitize_text

load_dotenv()
logger = logging.getLogger(__name__)

# Fall back to generic TOKEN / CHAT_ID for single-bot setups
_TOKEN   = os.getenv("PRIORITY_TELEGRAM_BOT_TOKEN") or os.getenv("TOKEN")
_CHAT_ID = os.getenv("PRIORITY_TELEGRAM_CHAT_ID")  or os.getenv("CHAT_ID")

_CAPTION_LIMIT = 1024
_SEPARATOR     = "━━━━━━━━━━"


class PriorityTelegramPublisher:

    def __init__(self) -> None:
        if not _TOKEN or not _CHAT_ID:
            raise RuntimeError(
                "Telegram bot token and chat ID are not set.\n"
                "Set PRIORITY_TELEGRAM_BOT_TOKEN + PRIORITY_TELEGRAM_CHAT_ID\n"
                "or fall back to TOKEN + CHAT_ID in your .env file."
            )

    def _caption(self, post: dict) -> str:
        """
        Build the Telegram-specific caption.

        ISSUE #3 FIX — Telegram format (different from Facebook/Instagram):

          📰 {title}

          🏷 {category}

          ━━━━━━━━━━

          {body text}

          🔗 {url}

        The category line is ONLY included in this Telegram caption.
        Facebook and Instagram captions are built in publish_pipeline
        and explicitly omit the category line.
        """
        title    = sanitize_text(post.get("title", ""))
        content  = sanitize_text((post.get("content") or "")[:500])
        category = post.get("source_label") or post.get("category") or ""
        url      = post.get("url", "")

        parts = [f"📰 {title}"]

        # Category line — Telegram ONLY
        if category:
            parts.append(f"🏷 {category}")

        # Separator before body
        parts.append(_SEPARATOR)

        # Body text
        if content:
            parts.append(content)

        # Source link
        if url:
            parts.append(f"🔗 {url}")

        return "\n\n".join(parts)[:_CAPTION_LIMIT]

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _send(self, endpoint: str, **kwargs) -> requests.Response:
        resp = requests.post(
            f"https://api.telegram.org/bot{_TOKEN}/{endpoint}",
            timeout=30,
            **kwargs,
        )
        if resp.status_code != 200:
            raise requests.RequestException(
                f"Telegram {endpoint} returned {resp.status_code}: {resp.text[:200]}"
            )
        return resp

    def publish(self, post: dict) -> bool:
        """
        Send the article to the Telegram channel.

        Uses sendPhoto when image_url is available, sendMessage otherwise.
        Caption always includes the category line (Telegram-specific format).
        """
        caption   = self._caption(post)
        image_url = post.get("image_url")

        try:
            if image_url:
                self._send(
                    "sendPhoto",
                    data={
                        "chat_id": _CHAT_ID,
                        "photo":   image_url,
                        "caption": caption,
                    },
                )
            else:
                self._send(
                    "sendMessage",
                    data={
                        "chat_id": _CHAT_ID,
                        "text":    caption,
                    },
                )
            logger.info(f"✅ Priority TG sent | {post.get('title', '')[:60]}")
            return True

        except Exception as exc:
            logger.error(f"❌ Priority TG error: {exc}")
            return False
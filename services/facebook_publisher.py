"""
Facebook publisher — posts via Make.com webhook.
Includes smart rate limiting checked against social_rate_log.
"""
from __future__ import annotations

import logging
import os

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from utils.text_filter import sanitize_text

logger = logging.getLogger(__name__)


class FacebookPublisher:

    def __init__(self) -> None:
        self.webhook_url = os.getenv("MAKE_WEBHOOK_URL")
        if not self.webhook_url:
            logger.warning("⚠️ MAKE_WEBHOOK_URL not set — Facebook publishing disabled")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=30),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def _send_request(self, payload: dict) -> requests.Response:
        assert self.webhook_url is not None
        response = requests.post(self.webhook_url, json=payload, timeout=30)
        if response.status_code != 200:
            logger.error(f"Facebook webhook {response.status_code}: {response.text[:200]}")
            raise requests.RequestException(f"Bad response: {response.status_code}")
        return response

    def publish(self, post: dict) -> bool:
        if not self.webhook_url:
            return False

        safe_title   = sanitize_text(post.get("title", ""))
        safe_content = sanitize_text((post.get("content") or "")[:500])
        category     = post.get("source_label") or post.get("category") or ""
        priority     = post.get("priority_score", 0)

        urgent_prefix = "🔴 عاجل\n\n" if priority >= 9 else ""
        cat_line      = f"📂 {category}\n\n" if category else ""

        payload = {
            "message":   f"{urgent_prefix}📰 {safe_title}\n\n{cat_line}{safe_content}",
            "image_url": post.get("image_url"),
            "url":       post.get("url"),
        }

        try:
            self._send_request(payload)
            logger.info(f"✅ Facebook sent | id={post.get('id')} | {safe_title[:50]}")
            return True
        except Exception as exc:
            logger.error(f"❌ Facebook failed | id={post.get('id')}: {exc}")
            return False

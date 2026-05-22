"""
Recalculation worker — periodically refreshes aging scores and reorders
the queue so that older articles get a priority boost over time.

Runs in a daemon thread every 60 seconds.
"""
from __future__ import annotations

import time
import logging

from services.queue_manager import QueueManager
from utils.logger import logger

_queue = QueueManager()


def recalculation_worker() -> None:
    logger.info("🔄 Recalculation worker started")
    while True:
        try:
            _queue.reorder_queue()
            logger.debug("🔄 Queue scores refreshed")
        except Exception as exc:
            logger.error(f"⚠️ Recalculation worker error: {exc}")
        time.sleep(60)

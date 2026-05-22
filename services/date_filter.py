"""Date-range guard for publishing windows."""
from __future__ import annotations
from datetime import datetime
from config.settings import FACEBOOK_START_DATE, FACEBOOK_END_DATE


def is_within_range(dt: datetime) -> bool:
    naive = dt.replace(tzinfo=None) if dt.tzinfo else dt
    return FACEBOOK_START_DATE <= naive <= FACEBOOK_END_DATE

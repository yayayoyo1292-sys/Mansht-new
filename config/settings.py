"""
config/settings.py — Central configuration for the Mansht news automation system.

═══════════════════════════════════════════════════════════════════════════════
FIXES APPLIED
═══════════════════════════════════════════════════════════════════════════════

ISSUE #1 — Category template_key alignment fix
───────────────────────────────────────────────
ROOT CAUSE: Non-AI categories (Economy, Cars, Tech, Culture) all had
  template_key = "عام" in their CATEGORIES config.  When save_news.py
  called _resolve_category_and_template() it used source_label as the
  category for template lookup (e.g. "الاقتصاد" with the ال prefix).
  But TEMPLATE_CONFIG in app3.py has keys without ال ("اقتصاد").
  This mismatch caused template_config.get("الاقتصاد") to return None
  and silently fall back to the wrong 'عام' config with a different
  image_box, breaking image composition.

FIX PART 1 (here): Non-AI categories now use their own template_key
  ("اقتصاد", "سيارات", "تكنولوجيا", "ثقافة") so the template_config
  lookup matches directly without relying on the source_label.

FIX PART 2 (in image/composer.py): _template_has_transparent_image_box()
  detects composition order automatically — no name-list dependency.

FIX PART 3 (in image/composer.py): _generate_post_image now strips the
  ال prefix as a fallback: template_config.get(category.lstrip("ال"))
  so any future label mismatches are handled gracefully.

ISSUE #4 — Publishing interval location (answer)
─────────────────────────────────────────────────
The Facebook inter-post delay is controlled entirely by this file:

    FACEBOOK_MIN_INTERVAL_SECONDS = 300   ← change this to adjust FB interval
    FACEBOOK_MAX_PER_HOUR         = 10    ← change this to adjust hourly cap

These values are read by publish_pipeline._can_post_now() which queries
social_rate_log to enforce the cooldown.  The delay is NOT from:
  ✗ Make.com        (Make.com just receives webhooks, doesn't schedule them)
  ✗ Cron job        (no cron — the scheduler worker loops in Python)
  ✗ Sleep timer     (no time.sleep between posts — it's DB-time-based)
  ✗ Async worker    (the publishing_worker is synchronous)
  ✗ External queue  (all queueing is in the news_queue DB table)

To change posting intervals safely:
  1. Edit the desired *_MIN_INTERVAL_SECONDS value below.
  2. Restart the process (python app3.py).
  3. No DB changes needed — takes effect immediately on next cycle.
"""
import os
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY DEFINITIONS
# source_url → internal name, display name, template key, use_ai_classifier
#
# ISSUE #1 FIX:
#   Non-AI categories now use their own specific template_key so the
#   template_config lookup in app3.py matches directly.
#   Previously all non-AI categories used template_key="عام" which caused
#   _resolve_category_and_template() to return the source_label (e.g.
#   "الاقتصاد" with ال prefix) as the category, which then failed to
#   match any TEMPLATE_CONFIG key.
# ─────────────────────────────────────────────────────────────────────────────

CATEGORIES: dict[str, dict] = {
    # AI-classified (political/general detection)
    "https://mnsht.net/category/1":  {"name": "uae",      "label": "الإمارات",   "template_key": None,         "use_ai": True},
    "https://mnsht.net/category/20": {"name": "saudi",    "label": "السعودية",   "template_key": None,         "use_ai": True},
    "https://mnsht.net/category/21": {"name": "egypt",    "label": "مصر",        "template_key": None,         "use_ai": True},
    "https://mnsht.net/category/22": {"name": "gulf",     "label": "الخليج",     "template_key": None,         "use_ai": True},
    "https://mnsht.net/category/23": {"name": "arab",     "label": "عربي",       "template_key": None,         "use_ai": True},
    "https://mnsht.net/category/7":  {"name": "world",    "label": "العالم",     "template_key": None,         "use_ai": True},

    # Fixed-template categories — each uses its own dedicated template key
    # ISSUE #1 FIX: was template_key="عام" for all of these; now specific keys
    "https://mnsht.net/category/10": {"name": "economy",  "label": "الاقتصاد",   "template_key": "اقتصاد",    "use_ai": False},
    "https://mnsht.net/category/8":  {"name": "sports",   "label": "رياضة",      "template_key": "رياضة",     "use_ai": False},
    "https://mnsht.net/category/11": {"name": "arts",     "label": "فن",         "template_key": "فن",        "use_ai": False},
    "https://mnsht.net/category/13": {"name": "culture",  "label": "ثقافة",      "template_key": "ثقافة",     "use_ai": False},
    "https://mnsht.net/category/14": {"name": "cars",     "label": "سيارات",     "template_key": "سيارات",    "use_ai": False},
    "https://mnsht.net/category/15": {"name": "tech",     "label": "تكنولوجيا",  "template_key": "تكنولوجيا", "use_ai": False},
    "https://mnsht.net/category/16": {"name": "misc",     "label": "متنوع",      "template_key": "عام",       "use_ai": False},
}

# AI-classified categories (classifier runs on these)
AI_CATEGORY_URLS: set[str] = {url for url, cfg in CATEGORIES.items() if cfg["use_ai"]}

# Template key resolution for AI categories (determined at runtime by classifier)
AI_TEMPLATE_MAP: dict[int, str] = {0: "عام", 1: "سياسة"}

# ─────────────────────────────────────────────────────────────────────────────
# SCRAPER SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

SCRAPE_INTERVAL_SECONDS: int = 30
SCRAPE_TIMEOUT_CONNECT: int  = 8
SCRAPE_TIMEOUT_READ: int     = 25
SCRAPE_MAX_RETRIES: int      = 4
SCRAPE_RETRY_DELAY: int      = 3
MAX_SCRAPE_PAGES: int        = 3
PARALLEL_WORKERS: int        = len(CATEGORIES)

# True  — warm-up first cycle: snapshot existing URLs, skip them (recommended)
# False — process every new-to-DB URL immediately on first run
SCRAPE_ONLY_NEW: bool = True

# ─────────────────────────────────────────────────────────────────────────────
# PUBLISHING SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

ENABLE_FACEBOOK_POSTING: bool  = True
FACEBOOK_START_DATE            = datetime(2026, 5, 20)
FACEBOOK_END_DATE              = datetime(2026, 12, 31)
MAX_QUEUE_AGE_HOURS: float     = 3.0
AGING_MULTIPLIER: float        = 0.12

# How long to keep publish_log rows (maintenance worker prunes older entries)
PUBLISH_LOG_RETENTION_DAYS: int = 30

# ─────────────────────────────────────────────────────────────────────────────
# PRIORITY SCORE THRESHOLDS
# ─────────────────────────────────────────────────────────────────────────────

PRIORITY_THRESHOLD_INSTAGRAM: int = 9
PRIORITY_THRESHOLD_TWITTER:   int = 1    # almost all articles → Twitter
PRIORITY_THRESHOLD_FACEBOOK:  int = 9    # kept for reference (pipeline logic overrides)

# ─────────────────────────────────────────────────────────────────────────────
# SOCIAL MEDIA RATE LIMITS & COOLDOWNS
#
# ISSUE #4 ANSWER — These variables control the publishing interval:
#
#   FACEBOOK_MIN_INTERVAL_SECONDS = 300
#     → Minimum seconds between two Facebook posts.
#     → Currently 5 minutes.  Change to 180 for 3-min interval, etc.
#
#   FACEBOOK_MAX_PER_HOUR = 10
#     → Hard cap on Facebook posts per rolling hour.
#     → Even if cooldown has cleared, this blocks posting if cap is reached.
#
#   These are enforced in: services/publish_pipeline.py → _can_post_now()
#   The cooldown is DB-time-based (compares NOW() to last sent_at in
#   social_rate_log), NOT a sleep timer or cron job.
#
#   To safely modify:
#     1. Change the value here.
#     2. Restart app3.py.
#     3. No DB migration needed.
# ─────────────────────────────────────────────────────────────────────────────

INSTAGRAM_MIN_INTERVAL_SECONDS: int = 120   # 2 minutes between Instagram posts
TWITTER_MIN_INTERVAL_SECONDS:   int = 60    # 1 minute between Twitter posts
FACEBOOK_MIN_INTERVAL_SECONDS:  int = 300   # 5 minutes between Facebook posts  ← ISSUE #4

FACEBOOK_MAX_PER_HOUR:  int = 10   # max Facebook posts per rolling hour   ← ISSUE #4
TWITTER_MAX_PER_HOUR:   int = 25   # max Twitter posts per rolling hour
INSTAGRAM_MAX_PER_HOUR: int = 10   # max Instagram posts per rolling hour

BURST_WINDOW_SECONDS: int = 60
BURST_MAX_INSTANT:    int = 2
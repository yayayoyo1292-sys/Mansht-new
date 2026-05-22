"""
DB/create_tables.py — Creates all required tables and indexes.
Run once on a fresh database, or run idempotently.

ADDED: publish_log table for PublishPipeline idempotency.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.db import get_conn

conn = get_conn()
conn.autocommit = True
cur  = conn.cursor()

# ── news ──────────────────────────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS news (
    id                  SERIAL PRIMARY KEY,
    title               TEXT UNIQUE,
    url                 TEXT UNIQUE,
    image               TEXT,
    category            TEXT,
    template_key        TEXT,
    confidence          REAL,
    content             TEXT,
    source_url          TEXT,
    source_category     TEXT,
    source_label        TEXT,
    detected_at         TIMESTAMPTZ,
    scraped_at          TIMESTAMPTZ,
    inserted_at         TIMESTAMPTZ DEFAULT NOW(),
    processed_at        TIMESTAMPTZ,
    queued_at           TIMESTAMPTZ,
    reviewed            BOOLEAN DEFAULT FALSE,
    locked_by           TEXT,
    locked_at           TIMESTAMP,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);
""")

# ── confirmed_training ────────────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS confirmed_training (
    id          SERIAL PRIMARY KEY,
    title       TEXT UNIQUE,
    label       INT  NOT NULL,
    confidence  REAL,
    source      TEXT DEFAULT 'auto',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
""")

# ── news_queue ────────────────────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS news_queue (
    id           SERIAL PRIMARY KEY,
    article_id   INT REFERENCES news(id) ON DELETE CASCADE,
    title        TEXT,
    url          TEXT UNIQUE,
    content      TEXT,
    created_at       DOUBLE PRECISION,
    detected_at      TIMESTAMPTZ,
    scraped_at       TIMESTAMPTZ,
    inserted_at      TIMESTAMPTZ DEFAULT NOW(),
    queued_at        TIMESTAMPTZ DEFAULT NOW(),
    processing_at    TIMESTAMPTZ,
    published_at     TIMESTAMPTZ,
    last_updated     TIMESTAMPTZ DEFAULT NOW(),
    keyword_score    FLOAT DEFAULT 0,
    aging_score      FLOAT DEFAULT 0,
    ai_score         FLOAT DEFAULT 0,
    final_score      FLOAT DEFAULT 0,
    priority_score   INT   DEFAULT 0,
    status           TEXT DEFAULT 'pending',
    telegram_status  TEXT DEFAULT 'pending',
    instagram_status TEXT DEFAULT 'pending',
    twitter_status   TEXT DEFAULT 'pending',
    facebook_status  TEXT DEFAULT 'pending',
    telegram_attempts  INT DEFAULT 0,
    instagram_attempts INT DEFAULT 0,
    twitter_attempts   INT DEFAULT 0,
    facebook_attempts  INT DEFAULT 0,
    generated_image  TEXT,
    image_url        TEXT,
    scheduled_publish_time TIMESTAMPTZ,
    category         TEXT,
    template_key     TEXT,
    source_label     TEXT
);
""")

# ── publish_log (NEW) ─────────────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS publish_log (
    id          SERIAL PRIMARY KEY,
    article_id  INT     NOT NULL,
    queue_id    INT,
    platform    TEXT    NOT NULL,
    status      TEXT    NOT NULL,
    fingerprint TEXT    UNIQUE NOT NULL,
    error_msg   TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
""")

# ── social_rate_log ───────────────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS social_rate_log (
    id          SERIAL PRIMARY KEY,
    platform    TEXT NOT NULL,
    sent_at     TIMESTAMPTZ DEFAULT NOW(),
    article_id  INT,
    queue_id    INT
);
""")

# ── scraper_health ────────────────────────────────────────────────────────────
cur.execute("""
CREATE TABLE IF NOT EXISTS scraper_health (
    id              SERIAL PRIMARY KEY,
    category_url    TEXT UNIQUE,
    category_name   TEXT,
    last_checked    TIMESTAMPTZ DEFAULT NOW(),
    last_success    TIMESTAMPTZ,
    last_failure    TIMESTAMPTZ,
    consecutive_failures INT DEFAULT 0,
    articles_found  INT DEFAULT 0,
    status          TEXT DEFAULT 'ok'
);
""")

# ── indexes ───────────────────────────────────────────────────────────────────
indexes = [
    "CREATE INDEX IF NOT EXISTS idx_news_created_at      ON news(created_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_news_category        ON news(category);",
    "CREATE INDEX IF NOT EXISTS idx_news_source_url      ON news(source_url);",
    "CREATE INDEX IF NOT EXISTS idx_news_inserted_at     ON news(inserted_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_queue_status         ON news_queue(status);",
    "CREATE INDEX IF NOT EXISTS idx_queue_final_score    ON news_queue(final_score DESC) WHERE status='pending';",
    "CREATE INDEX IF NOT EXISTS idx_queue_priority_score ON news_queue(priority_score DESC, queued_at ASC) WHERE status='pending';",
    "CREATE INDEX IF NOT EXISTS idx_queue_created_at     ON news_queue(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_rate_log_platform    ON social_rate_log(platform, sent_at DESC);",
    "CREATE INDEX IF NOT EXISTS idx_scraper_health_url   ON scraper_health(category_url);",
    # New publish_log indexes
    "CREATE INDEX IF NOT EXISTS idx_publish_log_article  ON publish_log(article_id, platform, status);",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_publish_log_fp ON publish_log(fingerprint);",
]
for idx in indexes:
    cur.execute(idx)

cur.close()
conn.close()
print("✅ All tables and indexes created/verified successfully")

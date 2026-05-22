-- DB/migrations_publish_log.sql
-- Run this migration to add the publish_log table required by PublishPipeline.
-- Safe to run multiple times (all statements use IF NOT EXISTS).

-- ─────────────────────────────────────────────────────────────────────────────
-- publish_log — idempotency store for all webhook publish events
-- ─────────────────────────────────────────────────────────────────────────────
-- fingerprint = sha256(article_id:platform)[:16]
-- ON CONFLICT (fingerprint) handles duplicate publish attempts gracefully.

CREATE TABLE IF NOT EXISTS publish_log (
    id          SERIAL PRIMARY KEY,
    article_id  INT     NOT NULL,
    queue_id    INT,
    platform    TEXT    NOT NULL,   -- instagram | facebook | twitter | telegram
    status      TEXT    NOT NULL,   -- sent | failed | skipped:*
    fingerprint TEXT    UNIQUE NOT NULL,
    error_msg   TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Fast lookup for idempotency check
CREATE INDEX IF NOT EXISTS idx_publish_log_article_platform
    ON publish_log (article_id, platform, status);

-- Cleanup: prune old rows (run nightly via maintenance worker)
-- DELETE FROM publish_log WHERE created_at < NOW() - INTERVAL '30 days';

-- ─────────────────────────────────────────────────────────────────────────────
-- news_queue: add missing columns if upgrading from old schema
-- ─────────────────────────────────────────────────────────────────────────────
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS telegram_attempts   INT DEFAULT 0;
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS instagram_attempts  INT DEFAULT 0;
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS twitter_attempts    INT DEFAULT 0;
ALTER TABLE news_queue ADD COLUMN IF NOT EXISTS facebook_attempts   INT DEFAULT 0;
